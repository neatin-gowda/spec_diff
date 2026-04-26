"""
Upgraded extractor (v2) - drop-in replacement for extractor.py.

Adds, beyond v1:
  * Cross-page table stitching
  * Multi-strategy table detection (grid + whitespace + camelot fallback)
  * Image-text OCR capture as figure blocks
  * Anchor tagging on every block (clause numbers, dollar amounts, dates,
    defined terms, alphanumeric codes)
  * Defined-term discovery pass (for legal/lease docs)
  * Better table row keys and column-aware row text for query/report use
  * Preserves table metadata for user-facing table names and previews

Output shape unchanged - still produces a flat list of Block objects
that the differ already understands.
"""
from __future__ import annotations

import hashlib
import json
import re
from typing import Optional

import fitz

from .anchors import (
    discover_defined_terms,
    find_anchored_terms,
    find_anchors,
)
from .extractor import (  # reuse what already works from v1
    _Line,
    _TYPICAL_KEY_RE,
    _body_font_size,
    _is_heading,
    _row_bbox_overlaps,
    coverage_pct,
    render_pages,
)
from .image_text import extract_image_text, ocr_full_page
from .models import Block, BlockType, TemplateProfile
from .table_extractor import extract_tables_robust
from .table_stitcher import stitch_tables


_IDENTIFIER_HEADER_TERMS = (
    "id",
    "code",
    "key",
    "number",
    "no",
    "part",
    "item",
    "model",
    "option",
    "order",
    "package",
    "pcv",
    "pcb",
    "sku",
    "ref",
    "reference",
)

_VALUE_HEADER_TERMS = (
    "price",
    "cost",
    "amount",
    "msrp",
    "invoice",
    "total",
    "subtotal",
    "power",
    "hp",
    "horsepower",
    "date",
    "year",
    "qty",
    "quantity",
    "percent",
    "%",
)

_NOISE_STABLE_KEYS = {
    "s",
    "o",
    "m",
    "i",
    "x",
    "-",
    "--",
    "n/a",
    "na",
    "none",
    "yes",
    "no",
    "tbd",
}


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


def _clean_cell(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _header_name(value: object, index: int) -> str:
    text = _clean_cell(value)
    if not text:
        return f"Column {index + 1}"
    text = re.sub(r"\s+", " ", text)
    return text[:90]


def _header_key(value: object) -> str:
    text = _clean_cell(value).lower()
    text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    return text


def _looks_like_money_or_measure(value: str) -> bool:
    text = value.strip()
    low = text.lower()

    if not text:
        return True
    if low in _NOISE_STABLE_KEYS:
        return True
    if "$" in text:
        return True
    if re.search(r"\b\d+(?:\.\d+)?\s*(hp|kw|kg|lb|lbs|mph|mpg|mm|cm|in|inch|ft|%)\b", low):
        return True
    if re.fullmatch(r"\d{1,2}[/-]\d{1,2}[/-]\d{2,4}", text):
        return True
    if re.fullmatch(r"(?:19|20)\d{2}", text):
        return True

    return False


def _looks_like_identifier(value: str) -> bool:
    text = value.strip()

    if not text:
        return False
    if text.lower() in _NOISE_STABLE_KEYS:
        return False
    if _looks_like_money_or_measure(text):
        return False

    # Codes like E1, 44Q, 99H, 205, PCV-205, PCB-205, ABC123.
    if re.fullmatch(r"[A-Z]{1,8}[- ]?\d{1,8}[A-Z]?", text, re.I):
        return True
    if re.fullmatch(r"\d{2,8}[A-Z]?", text, re.I):
        return True
    if re.fullmatch(r"[A-Z0-9]{2,8}[A-Z]?", text, re.I):
        return True

    return False


def _row_payload(header: list[str], row: list[str]) -> dict:
    payload: dict[str, str] = {}
    used: set[str] = set()

    max_len = max(len(header), len(row))

    for i in range(max_len):
        raw_key = header[i] if i < len(header) else ""
        key = _header_name(raw_key, i)

        if key in used:
            key = f"{key} {i + 1}"
        used.add(key)

        value = _clean_cell(row[i]) if i < len(row) else ""
        payload[key] = value

    return payload


def _row_text_from_payload(payload: dict[str, str]) -> str:
    parts = []

    for key, value in payload.items():
        value = _clean_cell(value)
        if not value:
            continue

        key_text = str(key)
        if re.match(r"^(col|column)\s*[_-]?\s*\d+$", key_text, re.I):
            parts.append(value)
        else:
            parts.append(f"{key_text}: {value}")

    return " | ".join(parts)


def _detect_stable_key(
    row: list[str],
    profile: Optional[TemplateProfile],
    header: Optional[list[str]] = None,
) -> Optional[str]:
    """Pick a stable row identifier. Profile-driven first, then generic/header-aware."""
    if profile and profile.stable_key_patterns:
        for spec in profile.stable_key_patterns:
            try:
                rx = re.compile(spec["regex"])
            except re.error:
                continue

            for cell in row:
                m = rx.search(_clean_cell(cell))
                if m:
                    return m.group(0)

    header = header or []
    cells = [_clean_cell(c) for c in row]
    header_keys = [_header_key(h) for h in header]

    # Prefer cells under identifier-like columns.
    for i, cell in enumerate(cells):
        if not _looks_like_identifier(cell):
            continue

        h = header_keys[i] if i < len(header_keys) else ""
        if any(term in h for term in _IDENTIFIER_HEADER_TERMS) and not any(term in h for term in _VALUE_HEADER_TERMS):
            return cell

    # Then prefer the first identifier-looking non-measure cell.
    for cell in cells:
        if _looks_like_identifier(cell):
            return cell

    # Last fallback: first descriptive cell, useful for table rows without formal codes.
    for i, cell in enumerate(cells):
        if not cell or cell.lower() in _NOISE_STABLE_KEYS:
            continue

        h = header_keys[i] if i < len(header_keys) else ""
        if any(term in h for term in _VALUE_HEADER_TERMS):
            continue

        if len(cell) >= 3 and not _looks_like_money_or_measure(cell):
            return cell[:100]

    return None


def _collect_lines_with_filter(pdf_path: str) -> list[_Line]:
    """Identical to v1's _collect_lines, kept here for explicit reuse."""
    from .extractor import _collect_lines

    return _collect_lines(pdf_path)


def _table_sort_key(st) -> tuple[int, float]:
    first_page = st.pages[0]
    bbox = st.bboxes_by_page.get(first_page)
    y0 = bbox[1] if bbox else 0.0
    return first_page, y0


def _title_from_context(path_stack: list[str], near_texts: list[str], header: list[str], first_page: int) -> str:
    for text in near_texts:
        text = _clean_cell(text)
        if len(text) >= 3:
            return text[:140]

    for part in reversed(path_stack):
        label = part.replace("_", " ").title()
        if label and label.lower() != "root":
            return label[:140]

    useful_headers = [h for h in header if h and not re.match(r"^(col|column)\s*[_-]?\s*\d+$", h, re.I)]
    if useful_headers:
        return " / ".join(useful_headers[:3])[:140]

    return f"Table on page {first_page}"


def _table_context(path_stack: list[str], near_texts: list[str]) -> str:
    parts = []

    if path_stack:
        parts.append(" / ".join(p.replace("_", " ").title() for p in path_stack if p))

    parts.extend(_clean_cell(t) for t in near_texts if _clean_cell(t))

    seen = set()
    out = []

    for part in parts:
        key = part.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(part)

    return " | ".join(out[:4])[:300]


def _meta_list(st, attr: str) -> list:
    value = getattr(st, attr, [])
    if isinstance(value, list):
        return value
    return []


def _source_table_metadata(st) -> list[dict]:
    out = []

    for item in _meta_list(st, "source_tables"):
        if not isinstance(item, dict):
            continue

        out.append(
            {
                "page": item.get("page"),
                "bbox": item.get("bbox"),
                "header": item.get("header"),
                "header_source": item.get("header_source"),
                "strategy": item.get("strategy"),
                "near_text": item.get("near_text"),
                "n_rows": item.get("n_rows"),
            }
        )

    return out


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
        # Possibly a fully scanned PDF - try full-page OCR.
        if enable_ocr:
            doc = fitz.open(pdf_path)
            n_pages = len(doc)
            doc.close()

            blocks: list[Block] = []

            for p in range(1, n_pages + 1):
                txt = ocr_full_page(pdf_path, p)
                if txt.strip():
                    page_width, page_height = page_sizes.get(p, (612, 792))
                    anchors = find_anchors(txt)
                    payload = {
                        "text": txt,
                        "ocr": True,
                        "anchors": [a.key() for a in anchors],
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
    stitched = sorted(stitch_tables(tables_by_page), key=_table_sort_key)

    # Build per-page table bboxes for line filtering.
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

    emitted_table_indexes: set[int] = set()

    def _emit_table(st, table_index: int):
        nonlocal seq

        first_page = st.pages[0]
        tbl_path = "/".join(path_stack + [f"table_{first_page}_{len(blocks)}"])
        bbox = list(st.bboxes_by_page[first_page])
        page_width, page_height = page_sizes.get(first_page, (612, 792))

        header = [_header_name(h, i) for i, h in enumerate(st.header or [])]
        header_text = " | ".join(header)

        near_texts = [_clean_cell(t) for t in _meta_list(st, "near_texts") if _clean_cell(t)]
        header_sources = [str(x) for x in _meta_list(st, "header_sources") if x]
        strategies = [str(x) for x in _meta_list(st, "strategies") if x]
        source_tables = _source_table_metadata(st)

        table_title = _title_from_context(path_stack, near_texts, header, first_page)
        context = _table_context(path_stack, near_texts)

        payload = {
            "header": header,
            "rows": st.rows,
            "spans_pages": st.pages,
            "stitched_from": st.source_count,
            "table_title": table_title,
            "table_context": context,
            "near_texts": near_texts,
            "header_sources": header_sources,
            "strategies": strategies,
            "source_tables": source_tables,
            "page_width": page_width,
            "page_height": page_height,
        }

        anchors_in_table = []
        for h in header:
            anchors_in_table.extend(find_anchors(h or ""))
        for t in near_texts:
            anchors_in_table.extend(find_anchors(t or ""))

        anc_sig = list({a.key() for a in anchors_in_table})

        tblock = Block(
            parent_id=current_section_block.id if current_section_block else None,
            block_type=BlockType.TABLE,
            path="/" + tbl_path,
            page_number=first_page,
            bbox=bbox,
            text=table_title or header_text,
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
        row_height = (table_y1 - table_y0) / row_slot_count if table_y1 > table_y0 else 10

        for ri, row in enumerate(st.rows):
            row_values = [_clean_cell(c) for c in row]
            stable_key = _detect_stable_key(row_values, profile, header)
            row_payload = _row_payload(header, row_values)
            row_text = _row_text_from_payload(row_payload)

            anchors = find_anchors(row_text) + find_anchored_terms(row_text, defined_terms)

            row_y0 = table_y0 + row_height * (ri + 1)
            row_y1 = table_y0 + row_height * (ri + 2)
            row_bbox = [table_x0, row_y0, table_x1, row_y1]

            row_payload["__anchors__"] = [a.key() for a in anchors]
            row_payload["__pages__"] = st.pages
            row_payload["__row_index__"] = ri
            row_payload["__table_title__"] = table_title
            row_payload["__table_context__"] = context
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

    def _emit_tables_before_line(ln: _Line):
        for idx, st in enumerate(stitched):
            if idx in emitted_table_indexes:
                continue

            first_page = st.pages[0]
            bbox = st.bboxes_by_page.get(first_page)
            table_y0 = bbox[1] if bbox else 0.0

            if first_page < ln.page or (first_page == ln.page and table_y0 <= ln.y):
                _emit_table(st, idx)
                emitted_table_indexes.add(idx)

    for ln in lines:
        # Emit tables only when the line cursor reaches their vertical position.
        # This keeps tables closer to the section heading that precedes them.
        _emit_tables_before_line(ln)

        # Skip lines inside any table region.
        if _row_bbox_overlaps(ln, table_bboxes_by_page.get(ln.page, [])):
            continue

        # Section heading?
        if _is_heading(ln, body):
            slug = _slug(ln.text)
            depth = max(1, int(round((ln.avg_size - body) / max(0.5, body * 0.1))))
            depth = min(depth, len(path_stack) + 1)
            path_stack = path_stack[: depth - 1] + [slug]

            page_width, page_height = page_sizes.get(ln.page, (612, 792))
            anchors = find_anchors(ln.text) + find_anchored_terms(ln.text, defined_terms)
            payload = {
                "heading": ln.text,
                "size": ln.avg_size,
                "anchors": [a.key() for a in anchors],
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
            anchors = find_anchors(ln.text) + find_anchored_terms(ln.text, defined_terms)
            payload = {
                "key": m.group("key").strip(),
                "value": m.group("val").strip(),
                "anchors": [a.key() for a in anchors],
                "page_width": page_width,
                "page_height": page_height,
            }

            base = current_section_block.path if current_section_block else "/root"
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

            base = current_section_block.path if current_section_block else "/root"
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

        # Plain paragraph.
        anchors = find_anchors(ln.text) + find_anchored_terms(ln.text, defined_terms)
        page_width, page_height = page_sizes.get(ln.page, (612, 792))
        payload = {
            "text": ln.text,
            "anchors": [a.key() for a in anchors],
            "page_width": page_width,
            "page_height": page_height,
        }

        base = current_section_block.path if current_section_block else "/root"
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

    # Emit any remaining tables we did not reach via line iteration.
    for idx, st in enumerate(stitched):
        if idx not in emitted_table_indexes:
            _emit_table(st, idx)
            emitted_table_indexes.add(idx)

    # --- Image text capture (figure blocks) ---
    if enable_ocr:
        for fig in extract_image_text(pdf_path):
            txt_parts = []

            if fig.get("near_text"):
                txt_parts.append(fig["near_text"])
            if fig.get("ocr_text"):
                txt_parts.append(fig["ocr_text"])
            if not txt_parts:
                continue

            text = " | ".join(txt_parts)
            page_width, page_height = page_sizes.get(fig["page"], (612, 792))
            anchors = find_anchors(text) + find_anchored_terms(text, defined_terms)
            payload = {
                "caption": fig.get("near_text", ""),
                "ocr_text": fig.get("ocr_text", ""),
                "anchors": [a.key() for a in anchors],
                "kind": fig.get("kind", "image"),
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
