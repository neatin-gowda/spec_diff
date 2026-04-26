"""
Cross-page table stitching.

Problem:
    PDF extractors usually return one table object per page, even when a
    logical table continues across several pages. They can also split one
    visual table into multiple adjacent table objects on the same page.

Strategy:
  1. Walk tables in document order.
  2. Stitch adjacent tables when they look like the same logical table:
       * same page, or consecutive pages
       * compatible column shape
       * compatible headers, or continuation has weak/inferred headers
       * similar page region / table title context
  3. Drop repeated header rows.
  4. Preserve source metadata so API/UI can show better table names.

Input:
    {page_num: [{"bbox": ..., "header": [...], "rows": [[...]], ...}, ...]}

Output:
    list[StitchedTable]
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Optional

from rapidfuzz import fuzz


@dataclass
class StitchedTable:
    pages: list[int] = field(default_factory=list)
    bboxes_by_page: dict[int, tuple[float, float, float, float]] = field(default_factory=dict)
    header: list[str] = field(default_factory=list)
    rows: list[list[str]] = field(default_factory=list)
    source_count: int = 1

    # Metadata for better API/UI presentation.
    near_texts: list[str] = field(default_factory=list)
    header_sources: list[str] = field(default_factory=list)
    strategies: list[str] = field(default_factory=list)
    source_tables: list[dict[str, Any]] = field(default_factory=list)


_GENERIC_HEADER_RE = re.compile(r"^(col|column|value)\s*[_-]?\s*\d+$", re.I)

_WEAK_HEADER_VALUES = {
    "",
    "-",
    "--",
    "—",
    "–",
    ".",
    "•",
    "●",
    "○",
    "o",
    "x",
    "s",
    "m",
    "i",
    "na",
    "n/a",
    "none",
    "tbd",
}


def _norm(value: Any) -> str:
    return " ".join(str(value or "").lower().split())


def _clean(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _has_letters(value: str) -> bool:
    return bool(re.search(r"[a-zA-Z]", value or ""))


def _is_numericish(value: str) -> bool:
    text = _clean(value)
    compact = re.sub(r"[\s,$%/().:-]", "", text)

    if not compact:
        return False

    digits = sum(ch.isdigit() for ch in compact)
    letters = sum(ch.isalpha() for ch in compact)

    return digits > 0 and digits >= max(1, letters * 2)


def _is_weak_header_cell(value: Any) -> bool:
    text = _clean(value)

    if not text:
        return True

    if text.lower() in _WEAK_HEADER_VALUES:
        return True

    if _GENERIC_HEADER_RE.match(text):
        return True

    if _is_numericish(text) and not _has_letters(text):
        return True

    return False


def _header_quality(header: list[str]) -> float:
    if not header:
        return 0.0

    useful = 0

    for cell in header:
        if not _is_weak_header_cell(cell):
            useful += 1

    return useful / max(1, len(header))


def _is_weak_header(header: list[str], source: Optional[str] = None) -> bool:
    if source in {"inferred", "vertical"}:
        # Vertical is not weak by itself. Inferred is useful, but still not
        # strong enough to prevent stitching.
        return source == "inferred" or _header_quality(header) < 0.45

    return _header_quality(header) < 0.45


def _row_width(row: list[str]) -> int:
    return len(row or [])


def _dominant_width(rows: list[list[str]], fallback: int = 0) -> int:
    counts: dict[int, int] = {}

    for row in rows:
        width = _row_width(row)
        if width <= 0:
            continue
        counts[width] = counts.get(width, 0) + 1

    if not counts:
        return fallback

    return max(counts.items(), key=lambda item: item[1])[0]


def _pad_or_trim_row(row: list[str], width: int) -> list[str]:
    row = list(row or [])

    if len(row) < width:
        return row + [""] * (width - len(row))

    if len(row) > width:
        return row[:width]

    return row


def _normalize_rows(rows: list[list[str]], width: int) -> list[list[str]]:
    return [_pad_or_trim_row(row, width) for row in rows]


def _headers_compatible(
    h1: list[str],
    h2: list[str],
    h1_source: Optional[str] = None,
    h2_source: Optional[str] = None,
) -> bool:
    if not h1 or not h2:
        return True

    width1 = len(h1)
    width2 = len(h2)

    if abs(width1 - width2) > 1:
        return False

    if _is_weak_header(h2, h2_source):
        return True

    if _is_weak_header(h1, h1_source):
        return True

    width = min(width1, width2)
    scores = []

    for a, b in zip(h1[:width], h2[:width]):
        if not a and not b:
            continue

        scores.append(fuzz.token_set_ratio(_norm(a), _norm(b)) / 100.0)

    if not scores:
        return True

    return (sum(scores) / len(scores)) >= 0.62


def _looks_like_repeated_header(row: list[str], header: list[str]) -> bool:
    if not row or not header:
        return False

    width = min(len(row), len(header))
    if width == 0:
        return False

    scores = []

    for a, b in zip(row[:width], header[:width]):
        if not a and not b:
            continue
        scores.append(fuzz.token_set_ratio(_norm(a), _norm(b)) / 100.0)

    if not scores:
        return False

    return (sum(scores) / len(scores)) >= 0.76


def _bbox_center_x(bbox: tuple[float, float, float, float]) -> float:
    return (bbox[0] + bbox[2]) / 2.0


def _bbox_width(bbox: tuple[float, float, float, float]) -> float:
    return max(1.0, bbox[2] - bbox[0])


def _same_horizontal_region(
    previous_bbox: tuple[float, float, float, float],
    current_bbox: tuple[float, float, float, float],
) -> bool:
    prev_center = _bbox_center_x(previous_bbox)
    curr_center = _bbox_center_x(current_bbox)
    prev_width = _bbox_width(previous_bbox)
    curr_width = _bbox_width(current_bbox)

    center_close = abs(prev_center - curr_center) <= max(prev_width, curr_width) * 0.18
    width_close = min(prev_width, curr_width) / max(prev_width, curr_width) >= 0.65

    return center_close and width_close


def _same_page_continuation(
    previous_bbox: tuple[float, float, float, float],
    current_bbox: tuple[float, float, float, float],
) -> bool:
    # Same-page split tables should be vertically close and horizontally aligned.
    vertical_gap = current_bbox[1] - previous_bbox[3]
    return vertical_gap >= -8 and vertical_gap <= 45 and _same_horizontal_region(previous_bbox, current_bbox)


def _context_compatible(previous: "StitchedTable", current_table: dict) -> bool:
    current_near = _norm(current_table.get("near_text"))
    previous_near = _norm(" ".join(previous.near_texts[-2:]))

    if not current_near or not previous_near:
        return True

    score = fuzz.partial_ratio(previous_near, current_near) / 100.0

    # Continuation pages often have no nearby title or only repeated page
    # headers. Avoid over-blocking stitching on context.
    return score >= 0.35 or current_near in previous_near or previous_near in current_near


def _column_shape_compatible(previous: "StitchedTable", current_table: dict) -> bool:
    prev_width = len(previous.header) or _dominant_width(previous.rows)
    curr_header = list(current_table.get("header") or [])
    curr_rows = list(current_table.get("rows") or [])
    curr_width = len(curr_header) or _dominant_width(curr_rows)

    if prev_width <= 0 or curr_width <= 0:
        return False

    if prev_width == curr_width:
        return True

    if abs(prev_width - curr_width) == 1:
        # Extraction sometimes creates/drops a blank column.
        return True

    return False


def _should_stitch(previous: "StitchedTable", pno: int, current_table: dict) -> bool:
    if not previous.pages:
        return False

    previous_page = previous.pages[-1]
    same_page = pno == previous_page
    next_page = pno == previous_page + 1

    if not same_page and not next_page:
        return False

    previous_bbox = previous.bboxes_by_page.get(previous_page)
    current_bbox = tuple(current_table.get("bbox") or (0, 0, 0, 0))

    if same_page and previous_bbox and current_bbox:
        if not _same_page_continuation(previous_bbox, current_bbox):
            return False

    if next_page and previous_bbox and current_bbox:
        if not _same_horizontal_region(previous_bbox, current_bbox):
            # Multi-page tables can shift slightly, but very different page
            # regions are likely a new table.
            return False

    if not _column_shape_compatible(previous, current_table):
        return False

    current_header = list(current_table.get("header") or [])
    current_source = current_table.get("header_source")

    previous_source = previous.header_sources[-1] if previous.header_sources else None

    if not _headers_compatible(previous.header, current_header, previous_source, current_source):
        return False

    if not _context_compatible(previous, current_table):
        return False

    return True


def _choose_better_header(previous_header: list[str], current_header: list[str], current_source: Optional[str]) -> list[str]:
    previous_quality = _header_quality(previous_header)
    current_quality = _header_quality(current_header)

    if current_source == "vertical" and current_quality >= previous_quality:
        return list(current_header)

    if previous_quality < 0.45 and current_quality > previous_quality:
        return list(current_header)

    return list(previous_header)


def _merge_table(previous: StitchedTable, pno: int, current_table: dict) -> None:
    current_header = list(current_table.get("header") or [])
    current_rows = list(current_table.get("rows") or [])
    current_bbox = tuple(current_table.get("bbox") or (0, 0, 0, 0))
    current_source = current_table.get("header_source") or "unknown"
    current_strategy = current_table.get("strategy") or "unknown"
    current_near = _clean(current_table.get("near_text"))

    width = len(previous.header) or len(current_header) or _dominant_width(previous.rows + current_rows)

    if abs(len(current_header) - width) <= 1:
        current_header = _pad_or_trim_row(current_header, width)

    previous.header = _choose_better_header(previous.header, current_header, current_source)
    previous.header = _pad_or_trim_row(previous.header, width)
    previous.rows = _normalize_rows(previous.rows, width)

    rows = _normalize_rows(current_rows, width)

    if rows and _looks_like_repeated_header(rows[0], previous.header):
        rows = rows[1:]

    previous.pages.append(pno)
    previous.bboxes_by_page[pno] = current_bbox
    previous.rows.extend(rows)
    previous.source_count += 1

    if current_near:
        previous.near_texts.append(current_near)

    previous.header_sources.append(current_source)
    previous.strategies.append(current_strategy)
    previous.source_tables.append(
        {
            "page": pno,
            "bbox": current_bbox,
            "header": current_header,
            "header_source": current_source,
            "strategy": current_strategy,
            "near_text": current_near,
            "n_rows": len(rows),
        }
    )


def _new_stitched_table(pno: int, table: dict) -> StitchedTable:
    header = list(table.get("header") or [])
    rows = list(table.get("rows") or [])
    width = len(header) or _dominant_width(rows)

    header = _pad_or_trim_row(header, width)
    rows = _normalize_rows(rows, width)

    bbox = tuple(table.get("bbox") or (0, 0, 0, 0))
    near_text = _clean(table.get("near_text"))
    header_source = table.get("header_source") or "unknown"
    strategy = table.get("strategy") or "unknown"

    return StitchedTable(
        pages=[pno],
        bboxes_by_page={pno: bbox},
        header=header,
        rows=rows,
        source_count=1,
        near_texts=[near_text] if near_text else [],
        header_sources=[header_source],
        strategies=[strategy],
        source_tables=[
            {
                "page": pno,
                "bbox": bbox,
                "header": header,
                "header_source": header_source,
                "strategy": strategy,
                "near_text": near_text,
                "n_rows": len(rows),
            }
        ],
    )


def stitch_tables(tables_by_page: dict[int, list[dict]]) -> list[StitchedTable]:
    """
    Input:
        {page_num: [{"bbox": ..., "header": [...], "rows": [[...]]}, ...]}

    Output:
        list of StitchedTable in document order.
    """
    flat: list[tuple[int, dict]] = []

    for pno in sorted(tables_by_page.keys()):
        page_tables = tables_by_page[pno]
        page_tables = sorted(page_tables, key=lambda t: (tuple(t.get("bbox") or (0, 0, 0, 0))[1], tuple(t.get("bbox") or (0, 0, 0, 0))[0]))

        for table in page_tables:
            flat.append((pno, table))

    stitched: list[StitchedTable] = []

    for pno, table in flat:
        if stitched and _should_stitch(stitched[-1], pno, table):
            _merge_table(stitched[-1], pno, table)
        else:
            stitched.append(_new_stitched_table(pno, table))

    return stitched
