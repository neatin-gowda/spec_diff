"""
Upgraded extractor (v2) — drop-in replacement for extractor.py.

Adds, beyond v1:
  * Cross-page table stitching
  * Multi-strategy table detection (grid + whitespace + camelot fallback)
  * Image-text OCR capture as figure blocks
  * Anchor tagging on every block (clause numbers, dollar amounts, dates,
    defined terms, alphanumeric codes)
  * Defined-term discovery pass (for legal/lease docs)

Output shape unchanged — still produces a flat list of `Block` objects
that the differ already understands.
"""
from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Optional

import fitz

from .anchors import (
    Anchor,
    anchor_signature,
    discover_defined_terms,
    find_anchored_terms,
    find_anchors,
)
from .extractor import (    # reuse what already works from v1
    _Line,
    _body_font_size,
    _is_heading,
    _row_bbox_overlaps,
    _TYPICAL_KEY_RE,
    coverage_pct,
    render_pages,
)
from .image_text import extract_image_text, is_scanned_page, ocr_full_page
from .models import Block, BlockType, TemplateProfile
from .table_extractor import extract_tables_robust
from .table_stitcher import stitch_tables


def _hash_content(payload: dict) -> str:
    s = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _slug(s: str) -> str:
    s = re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_").lower()
    return s[:60] or "section"


def _page_sizes(pdf_path: str) -> dict[int, tuple[float, float]]:
    doc = fitz.open(pdf_path)
    sizes = {i + 1: (page.rect.width, page.rect.height) for i, page in enumerate(doc)}
    doc.close()
    return sizes


def _detect_stable_key(row: list[str], profile: Optional[TemplateProfile]) -> Optional[str]:
    """Pick a stable identifier from a row. Profile-driven, with a generic fallback."""
    if profile and profile.stable_key_patterns:
        for spec in profile.stable_key_patterns:
            try:
                rx = re.compile(spec["regex"])
            except re.error:
                continue
            for cell in row:
                m = rx.search(str(cell or ""))
                if m:
                    return m.group(0)
    code_rx = re.compile(r"^[A-Z0-9]{2,4}[A-Z]?$")
    for cell in row:
        cell = str(cell or "").strip()
        if cell in {"S", "O", "M", "I", "X"}:
            continue
        if code_rx.fullmatch(cell):
            return cell
    return None


def _collect_lines_with_filter(pdf_path: str) -> list[_Line]:
    """Identical to v1's _collect_lines, kept here for explicit reuse."""
    from .extractor import _collect_lines
    return _collect_lines(pdf_path)


def extract_blocks_v2(
    pdf_path: str,
    profile: Optional[TemplateProfile] = None,
    enable_ocr: bool = True,
) -> list[Block]:
    """
    Returns a flat list of Block objects covering:
      sections, tables (stitched across pages), table_rows, list_items,
      kv_pairs, paragraphs, figures (with OCR'd image text and captions).
    """
    page_sizes = _page_sizes(pdf_path)
    lines = _collect_lines_with_filter(pdf_path)
    if not lines:
        # Possibly a fully scanned PDF — try full-page OCR
        if enable_ocr:
            doc = fitz.open(pdf_path)
            n_pages = len(doc)
            doc.close()
            blocks: list[Block] = []
            for p in range(1, n_pages + 1):
                txt = ocr_full_page(pdf_path, p)
                if txt.strip():
                    page_width, page_height = page_sizes.get(p, (612, 792))
                    payload = {
                        "text": txt,
                        "ocr": True,
                        "page_width": page_width,
                        "page_height": page_height,
                    }
                    b = Block(
                        block_type=BlockType.PARAGRAPH,
                        path=f"/scanned_page_{p}",
                        page_number=p,
                        text=txt,
                        payload=payload,
                        sequence=p,
                    )
                    b.content_hash = _hash_content(payload)
                    blocks.append(b)
            return blocks
        return []

    body = _body_font_size(lines)

    # --- Tables: robust extract + cross-page stitch ---
    tables_by_page = extract_tables_robust(pdf_path)
    stitched = stitch_tables(tables_by_page)

    # Build per-page table bboxes for line filtering
    table_bboxes_by_page: dict[int, list[tuple[float, float, float, float]]] = {}
    for st in stitched:
        for pno, bb in st.bboxes_by_page.items():
            table_bboxes_by_page.setdefault(pno, []).append(bb)

    # --- Defined-term discovery pass (helps lease/legal docs) ---
    line_corpus = [ln.text for ln in lines]
    defined_terms = discover_defined_terms(line_corpus, min_occurrences=4)

    blocks: list[Block] = []
    seq = 0
    path_stack: list[str] = []
    current_section_block: Optional[Block] = None

    # First emit stitched tables (anchored to their FIRST page)
    emitted_table_anchor_pages: set[int] = set()

    def _emit_table(st):
        nonlocal seq
        first_page = st.pages[0]
        tbl_path = "/".join(path_stack + [f"table_{first_page}_{len(blocks)}"])
        bbox = list(st.bboxes_by_page[first_page])
        page_width, page_height = page_sizes.get(first_page, (612, 792))

        payload = {
            "header": st.header,
            "rows": st.rows,
            "spans_pages": st.pages,
            "stitched_from": st.source_count,
            "page_width": page_width,
            "page_height": page_height,
        }

        anchors_in_table = []
        for h in st.header:
            anchors_in_table.extend(find_anchors(h or ""))
        anc_sig = list({a.key() for a in anchors_in_table})

        tblock = Block(
            parent_id=current_section_block.id if current_section_block else None,
            block_type=BlockType.TABLE,
            path="/" + tbl_path,
            page_number=first_page,
            bbox=bbox,
            text="",
            payload={**payload, "anchors": anc_sig},
            sequence=seq,
        )
        tblock.content_hash = _hash_content(payload)
        blocks.append(tblock)
        seq += 1

        # One Block per row, with anchors and stable keys.
        # Important: each row receives its own approximate bbox; using the full
        # table bbox here causes huge full-page highlights in the viewer.
        table_x0, table_y0, table_x1, table_y1 = bbox
        row_count = max(1, len(st.rows))
        row_slot_count = row_count + 1  # reserve one slot for the header row
        row_height = (table_y1 - table_y0) / row_slot_count

        for ri, row in enumerate(st.rows):
            stable_key = _detect_stable_key(row, profile)
            row_text = " | ".join(str(c or "") for c in row)
            anchors = find_anchors(row_text) + find_anchored_terms(row_text, defined_terms)

            row_y0 = table_y0 + row_height * (ri + 1)
            row_y1 = table_y0 + row_height * (ri + 2)
            row_bbox = [table_x0, row_y0, table_x1, row_y1]

            row_payload = {
                h or f"col_{i}": v
                for i, (h, v) in enumerate(zip(st.header, row))
            }
            row_payload["__anchors__"] = [a.key() for a in anchors]
            row_payload["__pages__"] = st.pages
            row_payload["page_width"] = page_width
            row_payload["page_height"] = page_height

            rblock = Block(
                parent_id=tblock.id,
                block_type=BlockType.TABLE_ROW,
                path=f"{tblock.path}/row_{ri}",
                stable_key=stable_key,
                page_number=first_page,
                bbox=row_bbox,
                text=row_text,
                payload=row_payload,
                sequence=seq,
            )
            rblock.content_hash = _hash_content(row_payload)
            blocks.append(rblock)
            seq += 1

    # Tables go in the section context they were *seen* in; we track that via line iteration below
    pending_tables = list(stitched)
    table_iter = iter(pending_tables)
    next_table = next(table_iter, None)

    for ln in lines:
        # Emit any tables whose first page is at-or-before this line's page
        while next_table is not None and next_table.pages[0] <= ln.page and next_table.pages[0] not in emitted_table_anchor_pages:
            # Anchor this table to its first appearance — only emit once
            if next_table.pages[0] == ln.page or next_table.pages[0] < ln.page:
                _emit_table(next_table)
                emitted_table_anchor_pages.add(next_table.pages[0])
                next_table = next(table_iter, None)
            else:
                break

        # Skip lines inside any table region
        if _row_bbox_overlaps(ln, table_bboxes_by_page.get(ln.page, [])):
            continue

        # Section heading?
        if _is_heading(ln, body):
            slug = _slug(ln.text)
            depth = max(1, int(round((ln.avg_size - body) / max(0.5, body * 0.1))))
            depth = min(depth, len(path_stack) + 1)
            path_stack = path_stack[:depth - 1] + [slug]
            page_width, page_height = page_sizes.get(ln.page, (612, 792))
            payload = {
                "heading": ln.text,
                "size": ln.avg_size,
                "anchors": [a.key() for a in find_anchors(ln.text)],
                "page_width": page_width,
                "page_height": page_height,
            }

            blk = Block(
                parent_id=current_section_block.id if (current_section_block and depth > 1) else None,
                block_type=BlockType.SECTION,
                path="/" + "/".join(path_stack),
                page_number=ln.page,
                bbox=[ln.x0, ln.y, ln.x1, ln.y + ln.avg_size],
                text=ln.text,
                payload=payload,
                sequence=seq,
            )
            blk.content_hash = _hash_content(payload)
            blocks.append(blk)
            seq += 1
            current_section_block = blk
            continue

        # Key:value?
        m = _TYPICAL_KEY_RE.match(ln.text)
        if m:
            page_width, page_height = page_sizes.get(ln.page, (612, 792))
            payload = {
                "key": m.group("key").strip(),
                "value": m.group("val").strip(),
                "anchors": [a.key() for a in find_anchors(ln.text)],
                "page_width": page_width,
                "page_height": page_height,
            }

            base = (current_section_block.path if current_section_block else "/root")
            blk = Block(
                parent_id=current_section_block.id if current_section_block else None,
                block_type=BlockType.KV_PAIR,
                path=f"{base}/kv_{seq}",
                page_number=ln.page,
                bbox=[ln.x0, ln.y, ln.x1, ln.y + ln.avg_size],
                text=ln.text,
                payload=payload,
                sequence=seq,
            )
            blk.content_hash = _hash_content(payload)
            blocks.append(blk)
            seq += 1
            continue

        # List item?
        list_marker = re.match(r"^\s*([●○■•—–\-▪◦])\s+(.*)$", ln.text)
        if list_marker:
            txt = list_marker.group(2).strip()
            anchors = find_anchors(txt) + find_anchored_terms(txt, defined_terms)
            page_width, page_height = page_sizes.get(ln.page, (612, 792))
            payload = {
                "marker": list_marker.group(1),
                "text": txt,
                "anchors": [a.key() for a in anchors],
                "page_width": page_width,
                "page_height": page_height,
            }

            base = (current_section_block.path if current_section_block else "/root")
            blk = Block(
                parent_id=current_section_block.id if current_section_block else None,
                block_type=BlockType.LIST_ITEM,
                path=f"{base}/list_{seq}",
                page_number=ln.page,
                bbox=[ln.x0, ln.y, ln.x1, ln.y + ln.avg_size],
                text=txt,
                payload=payload,
                sequence=seq,
            )
            blk.content_hash = _hash_content(payload)
            blocks.append(blk)
            seq += 1
            continue

        # Plain paragraph
        anchors = find_anchors(ln.text) + find_anchored_terms(ln.text, defined_terms)
        page_width, page_height = page_sizes.get(ln.page, (612, 792))
        payload = {
            "text": ln.text,
            "anchors": [a.key() for a in anchors],
            "page_width": page_width,
            "page_height": page_height,
        }

        base = (current_section_block.path if current_section_block else "/root")
        blk = Block(
            parent_id=current_section_block.id if current_section_block else None,
            block_type=BlockType.PARAGRAPH,
            path=f"{base}/p_{seq}",
            page_number=ln.page,
            bbox=[ln.x0, ln.y, ln.x1, ln.y + ln.avg_size],
            text=ln.text,
            payload=payload,
            sequence=seq,
        )
        blk.content_hash = _hash_content(payload)
        blocks.append(blk)
        seq += 1

    # Emit any remaining tables we didn't reach via line iteration
    while next_table is not None:
        if next_table.pages[0] not in emitted_table_anchor_pages:
            _emit_table(next_table)
            emitted_table_anchor_pages.add(next_table.pages[0])
        next_table = next(table_iter, None)

    # --- Image text capture (figure blocks) ---
    if enable_ocr:
        for fig in extract_image_text(pdf_path):
            txt_parts = []
            if fig["near_text"]:
                txt_parts.append(fig["near_text"])
            if fig["ocr_text"]:
                txt_parts.append(fig["ocr_text"])
            if not txt_parts:
                continue
            text = " | ".join(txt_parts)
            page_width, page_height = page_sizes.get(fig["page"], (612, 792))
            payload = {
                "caption": fig["near_text"],
                "ocr_text": fig["ocr_text"],
                "anchors": [a.key() for a in find_anchors(text)],
                "kind": fig["kind"],
                "page_width": page_width,
                "page_height": page_height,
            }

            blk = Block(
                block_type=BlockType.FIGURE,
                path=f"/figures/page_{fig['page']}_xref_{fig['image_xref']}",
                page_number=fig["page"],
                bbox=fig["bbox"],
                text=text,
                payload=payload,
                sequence=seq,
            )
            blk.content_hash = _hash_content(payload)
            blocks.append(blk)
            seq += 1

    return blocks


# Re-export for compatibility
extract_blocks = extract_blocks_v2
__all__ = ["extract_blocks_v2", "extract_blocks", "render_pages", "coverage_pct"]
