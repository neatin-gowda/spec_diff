"""
Robust table extraction.

The original extractor used pdfplumber.find_tables() with default settings.
That is fast and reliable for clean grid tables, but misses:

  * Tables defined by whitespace/alignment alone (no ruling lines)
  * Nested tables or merged cells
  * Tables with rotated / vertical column headers
  * Tables where the first extracted row is data, not a real header

This module wraps multiple strategies and reconciles the results:

  Strategy A: pdfplumber default (lines+text) - fastest, best for grids
  Strategy B: pdfplumber text-only mode - catches whitespace tables
  Strategy C: camelot stream mode - best for sparse tables

The output contract is intentionally unchanged:
    {page_num: [{"bbox": ..., "header": [...], "rows": [[...]], "strategy": "..."}]}

Downstream extractor/stitcher/differ code can keep working.
"""
from __future__ import annotations

import re
import warnings
from collections import defaultdict
from typing import Any, Optional

import pdfplumber


_GENERIC_COL_RE = re.compile(r"^col[_\s-]?\d+$", re.I)

_SYMBOL_ONLY_VALUES = {
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
    "O",
    "x",
    "X",
    "s",
    "S",
    "m",
    "M",
    "i",
    "I",
    "na",
    "n/a",
    "none",
    "tbd",
}


def _clean_cell(value: Any) -> str:
    text = str(value or "")
    text = text.replace("\n", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _clean_header(value: Any) -> str:
    text = _clean_cell(value)
    text = re.sub(r"\s*/\s*", " / ", text)
    text = re.sub(r"\s+", " ", text)
    return text[:90].strip()


def _normalize_rows(rows: list[list[Optional[str]]]) -> list[list[str]]:
    if not rows:
        return []

    n_cols = max(len(r) for r in rows)
    out = []

    for row in rows:
        normalized = [_clean_cell(c) for c in row]
        normalized += [""] * (n_cols - len(normalized))
        out.append(normalized)

    return out


def _forward_fill_rowspans(rows: list[list[Optional[str]]]) -> list[list[str]]:
    """
    pdfplumber returns None for cells covered by a rowspan-merged cell.
    We forward-fill from the cell above so each row stands alone.
    """
    filled = _normalize_rows(rows)

    if not filled:
        return []

    for ri in range(1, len(filled)):
        for ci in range(len(filled[ri])):
            if not filled[ri][ci] and filled[ri - 1][ci]:
                filled[ri][ci] = filled[ri - 1][ci]

    return filled


def _is_sparse(rows: list[list[Optional[str]]]) -> float:
    """Fraction of cells that are None/empty - signal of merged cells."""
    if not rows:
        return 0.0

    total = sum(len(r) for r in rows)
    empty = sum(1 for r in rows for c in r if c is None or c == "")

    return empty / max(1, total)


def _strategy_a(page) -> list[dict]:
    """Default pdfplumber table extraction."""
    out = []

    for t in page.find_tables():
        rows = t.extract()

        if not rows or len(rows) < 2:
            continue

        rows = _normalize_rows(rows)
        out.append({"bbox": t.bbox, "rows": rows, "strategy": "A"})

    return out


def _strategy_b(page) -> list[dict]:
    """Text-only mode for whitespace-only tables."""
    out = []
    settings = {
        "vertical_strategy": "text",
        "horizontal_strategy": "text",
        "intersection_tolerance": 5,
        "snap_tolerance": 4,
        "join_tolerance": 4,
        "text_tolerance": 3,
    }

    try:
        tables = page.find_tables(table_settings=settings)
    except Exception:
        return out

    for t in tables:
        rows = t.extract()

        if not rows or len(rows) < 2:
            continue

        rows = _normalize_rows(rows)
        out.append({"bbox": t.bbox, "rows": rows, "strategy": "B"})

    return out


def _strategy_c(pdf_path: str, page_num: int) -> list[dict]:
    """Camelot stream mode - best for sparse / messy layouts."""
    try:
        import camelot

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            ts = camelot.read_pdf(
                pdf_path,
                pages=str(page_num),
                flavor="stream",
                suppress_stdout=True,
            )

        out = []

        for t in ts:
            rows = t.df.fillna("").values.tolist()
            rows = _normalize_rows(rows)

            if len(rows) < 2:
                continue

            # camelot reports bbox in PDF coords. This is still useful for
            # approximate filtering/highlighting even if not perfect.
            bbox = tuple(t._bbox) if hasattr(t, "_bbox") else (0, 0, 0, 0)
            out.append({"bbox": bbox, "rows": rows, "strategy": "C"})

        return out
    except Exception:
        return []


def _cell_has_letters(text: str) -> bool:
    return bool(re.search(r"[A-Za-z]", text or ""))


def _cell_is_mostly_numeric(text: str) -> bool:
    text = _clean_cell(text)

    if not text:
        return False

    compact = re.sub(r"[\s,$%/().:-]", "", text)
    if not compact:
        return False

    digits = sum(ch.isdigit() for ch in compact)
    letters = sum(ch.isalpha() for ch in compact)

    return digits > 0 and digits >= max(1, letters * 2)


def _is_noise_cell(text: str) -> bool:
    return _clean_cell(text).lower() in _SYMBOL_ONLY_VALUES


def _looks_like_header_row(row: list[str], next_rows: list[list[str]]) -> bool:
    """
    Decide whether a row is truly a header.

    Important: in many spec PDFs the first extracted row is a value row
    like "5,610 | 21,023". That must not become the header.
    """
    row = [_clean_cell(c) for c in row]
    non_empty = [c for c in row if c]

    if not non_empty:
        return False

    alpha_cells = sum(1 for c in non_empty if _cell_has_letters(c))
    numeric_cells = sum(1 for c in non_empty if _cell_is_mostly_numeric(c))
    noise_cells = sum(1 for c in non_empty if _is_noise_cell(c))
    avg_len = sum(len(c) for c in non_empty) / max(1, len(non_empty))

    if numeric_cells >= max(1, len(non_empty) * 0.6) and alpha_cells == 0:
        return False

    if noise_cells >= max(1, len(non_empty) * 0.6):
        return False

    if alpha_cells == 0:
        return False

    # Very long cells are usually body text, not headers.
    if avg_len > 70:
        return False

    # Headers are normally shorter than body rows.
    body_lengths = []
    for body_row in next_rows[:5]:
        body_cells = [_clean_cell(c) for c in body_row if _clean_cell(c)]
        if body_cells:
            body_lengths.append(sum(len(c) for c in body_cells) / len(body_cells))

    if body_lengths and avg_len > (sum(body_lengths) / len(body_lengths)) * 1.8:
        return False

    return True


def _merge_header_rows(rows: list[list[str]], max_header_rows: int = 2) -> tuple[list[str], list[list[str]]]:
    """
    Supports simple two-line headers, e.g.
        ["F.E. LABEL", "", "ESTIMATED"]
        ["CITY", "HIGHWAY", "ANNUAL FUEL COSTS"]
    """
    if not rows:
        return [], []

    first = rows[0]
    second = rows[1] if len(rows) > 1 else []
    first_is_header = _looks_like_header_row(first, rows[1:])
    second_is_header = bool(second) and _looks_like_header_row(second, rows[2:])

    if not first_is_header:
        return _infer_fallback_headers(rows), rows

    if max_header_rows >= 2 and second_is_header:
        n_cols = max(len(first), len(second))
        first = first + [""] * (n_cols - len(first))
        second = second + [""] * (n_cols - len(second))
        merged = []

        for a, b in zip(first, second):
            a = _clean_header(a)
            b = _clean_header(b)

            if a and b and a.lower() != b.lower():
                merged.append(f"{a} / {b}")
            else:
                merged.append(a or b)

        return _dedupe_headers(merged), rows[2:]

    return _dedupe_headers(first), rows[1:]


def _infer_fallback_headers(rows: list[list[str]]) -> list[str]:
    """
    If no true header is found, provide business-friendly fallback labels.

    This is better than col_1/col_2 for UI users and still honest:
    the column is inferred, not claimed as an actual PDF header.
    """
    if not rows:
        return []

    n_cols = max(len(r) for r in rows)
    sample = rows[:80]

    if n_cols == 1:
        return ["Content"]

    first_col_text = 0
    other_symbol_or_short = 0
    other_total = 0

    for row in sample:
        first = _clean_cell(row[0] if row else "")

        if len(first) >= 8 or _cell_has_letters(first):
            first_col_text += 1

        for cell in row[1:]:
            value = _clean_cell(cell)
            if not value:
                continue

            other_total += 1

            if _is_noise_cell(value) or len(value) <= 12:
                other_symbol_or_short += 1

    first_col_is_feature = first_col_text >= max(2, len(sample) * 0.35)
    rest_are_values = other_total == 0 or (other_symbol_or_short / max(1, other_total)) >= 0.55

    if first_col_is_feature and rest_are_values:
        headers = ["Feature / item"]
        headers.extend(f"Value {i}" for i in range(1, n_cols))
        return headers

    if n_cols == 2:
        return ["Feature / item", "Value"]

    headers = ["Feature / item"]
    headers.extend(f"Column {i}" for i in range(2, n_cols + 1))
    return headers


def _dedupe_headers(headers: list[str]) -> list[str]:
    out = []
    seen = defaultdict(int)

    for idx, raw in enumerate(headers, start=1):
        header = _clean_header(raw)

        if not header:
            header = f"Column {idx}"

        if _is_noise_cell(header):
            header = f"Column {idx}"

        key = header.lower()
        seen[key] += 1

        if seen[key] > 1:
            header = f"{header} {seen[key]}"

        out.append(header)

    return out


def _header_quality(headers: list[str]) -> float:
    if not headers:
        return 0.0

    useful = 0

    for h in headers:
        h = _clean_cell(h)

        if not h:
            continue
        if _GENERIC_COL_RE.match(h):
            continue
        if _is_noise_cell(h):
            continue
        if _cell_is_mostly_numeric(h) and not _cell_has_letters(h):
            continue

        useful += 1

    return useful / max(1, len(headers))


def _chars_in_bbox(page, bbox: tuple[float, float, float, float]) -> list[dict]:
    x0, top, x1, bottom = bbox
    chars = []

    for ch in getattr(page, "chars", []) or []:
        cx0 = float(ch.get("x0", 0))
        cx1 = float(ch.get("x1", 0))
        ctop = float(ch.get("top", ch.get("y0", 0)))
        cbottom = float(ch.get("bottom", ch.get("y1", 0)))

        if cx1 < x0 or cx0 > x1 or cbottom < top or ctop > bottom:
            continue

        chars.append(ch)

    return chars


def _vertical_header_candidates(page, bbox: tuple[float, float, float, float], n_cols: int) -> list[str]:
    """
    Extract rotated / vertical text that sits inside the top part of the table.

    pdfplumber exposes char["upright"] for rotated text in many PDFs.
    We group vertical chars by approximate table column.
    """
    if not page or not bbox or n_cols <= 0:
        return []

    x0, top, x1, bottom = bbox
    table_height = max(1.0, bottom - top)
    table_width = max(1.0, x1 - x0)
    header_bottom = top + min(max(70.0, table_height * 0.35), table_height)

    chars = _chars_in_bbox(page, (x0, top, x1, header_bottom))
    vertical_chars = []

    for ch in chars:
        text = ch.get("text", "")

        if not text or not text.strip():
            continue

        upright = ch.get("upright", True)

        if upright is False:
            vertical_chars.append(ch)

    if not vertical_chars:
        return []

    grouped: dict[int, list[dict]] = defaultdict(list)
    col_width = table_width / n_cols

    for ch in vertical_chars:
        cx = (float(ch.get("x0", 0)) + float(ch.get("x1", 0))) / 2
        col_idx = int((cx - x0) / max(1.0, col_width))
        col_idx = max(0, min(n_cols - 1, col_idx))
        grouped[col_idx].append(ch)

    headers = [""] * n_cols

    for col_idx, group in grouped.items():
        # Vertical text is usually ordered top-to-bottom. Sort by top,
        # then x to keep stacked letters stable.
        group = sorted(group, key=lambda c: (float(c.get("top", c.get("y0", 0))), float(c.get("x0", 0))))
        pieces = [str(c.get("text", "")) for c in group]
        text = "".join(pieces)
        text = re.sub(r"\s+", " ", text).strip()

        # If letters are extracted one by one, joined text can be compact.
        # Keep it if it has letters and is not just a symbol.
        if len(text) >= 2 and _cell_has_letters(text):
            headers[col_idx] = _clean_header(text)

    return headers


def _extract_nearby_title(page, bbox: tuple[float, float, float, float]) -> str:
    """
    Capture nearby text above a table. API can use this later to name tables.
    This does not affect downstream behavior if ignored.
    """
    if not page or not bbox:
        return ""

    x0, top, x1, _bottom = bbox
    search_top = max(0, top - 90)
    search_bottom = max(0, top - 4)

    try:
        cropped = page.crop((x0, search_top, x1, search_bottom))
        text = cropped.extract_text() or ""
    except Exception:
        return ""

    lines = [_clean_cell(line) for line in text.splitlines()]
    lines = [line for line in lines if line]

    if not lines:
        return ""

    # Prefer the closest meaningful line.
    for line in reversed(lines):
        if len(line) >= 3 and not _cell_is_mostly_numeric(line):
            return line[:160]

    return lines[-1][:160]


def _apply_vertical_headers(
    page,
    bbox: tuple[float, float, float, float],
    header: list[str],
    body: list[list[str]],
) -> tuple[list[str], str]:
    """
    Replace weak headers with vertical/rotated headers when available.
    """
    n_cols = max(len(header), max((len(r) for r in body), default=0))

    if n_cols <= 0:
        return header, "none"

    header = header + [""] * (n_cols - len(header))
    vertical = _vertical_header_candidates(page, bbox, n_cols)

    if not vertical:
        return _dedupe_headers(header), "normal"

    merged = list(header)
    replaced = False

    for idx, candidate in enumerate(vertical):
        if not candidate:
            continue

        current = _clean_cell(merged[idx] if idx < len(merged) else "")

        if (
            not current
            or _GENERIC_COL_RE.match(current)
            or _is_noise_cell(current)
            or (_cell_is_mostly_numeric(current) and not _cell_has_letters(current))
            or len(current) <= 3
        ):
            merged[idx] = candidate
            replaced = True

    # If the whole detected header is weak, use vertical headers directly.
    if _header_quality(header) < 0.45 and _header_quality(vertical) > _header_quality(header):
        merged = [vertical[i] or header[i] for i in range(n_cols)]
        replaced = True

    return _dedupe_headers(merged), "vertical" if replaced else "normal"


def _split_header_body(
    raw_rows: list[list[str]],
    page=None,
    bbox: Optional[tuple[float, float, float, float]] = None,
) -> tuple[list[str], list[list[str]], str]:
    """
    Pick a trustworthy header and body.

    If the PDF has vertical headers, supplement/replace weak extracted headers.
    If no real header exists, return inferred business-friendly headers and
    keep all rows as body rows.
    """
    rows = _normalize_rows(raw_rows)

    if not rows:
        return [], [], "empty"

    header, body = _merge_header_rows(rows)

    source = "inferred" if body is rows or header == _infer_fallback_headers(rows) else "normal"

    if bbox is not None:
        header, vertical_source = _apply_vertical_headers(page, bbox, header, body)

        if vertical_source == "vertical":
            source = "vertical"

    if _header_quality(header) < 0.35:
        header = _infer_fallback_headers(body or rows)
        source = "inferred"

    # Ensure every body row has the same width as the header.
    n_cols = max(len(header), max((len(r) for r in body), default=0))
    header = _dedupe_headers(header + [""] * (n_cols - len(header)))

    normalized_body = []
    for row in body:
        row = row + [""] * (n_cols - len(row))
        normalized_body.append(row[:n_cols])

    return header, normalized_body, source


def _bboxes_overlap(b1, b2, tol: float = 5.0) -> bool:
    return not (
        b1[2] < b2[0] - tol
        or b2[2] < b1[0] - tol
        or b1[3] < b2[1] - tol
        or b2[3] < b1[1] - tol
    )


def _looks_like_text_columns(rows: list[list[str]]) -> bool:
    """
    Detect when pdfplumber has mistaken multi-column page layout
    for a table.

    Signals:
      * cells contain very long text
      * many cells start with bullets
      * the table has too few structured rows
    """
    if not rows:
        return False

    long_cells = 0
    bullet_cells = 0
    total_cells = 0

    for row in rows:
        for cell in row:
            cell = _clean_cell(cell)

            if not cell:
                continue

            total_cells += 1

            if len(cell) > 220:
                long_cells += 1

            if cell.lstrip().startswith(("●", "—", "○", "•", "▪", "◦")):
                bullet_cells += 1

    if total_cells == 0:
        return False

    if long_cells / total_cells > 0.20:
        return True

    if bullet_cells / total_cells > 0.35:
        return True

    return False


def _table_has_enough_structure(rows: list[list[str]]) -> bool:
    if len(rows) < 2:
        return False

    n_cols = max(len(r) for r in rows)

    if n_cols < 2:
        return False

    filled = 0
    total = n_cols * len(rows)

    for row in rows:
        for cell in row:
            if _clean_cell(cell):
                filled += 1

    fill_ratio = filled / max(1, total)

    # Low fill can still be a valid options table with sparse marks,
    # but it needs several rows to be useful.
    if fill_ratio < 0.18 and len(rows) < 5:
        return False

    return True


def _dedupe_overlapping_tables(tables: list[dict]) -> list[dict]:
    """
    When strategies A/B/C return overlapping tables, keep the richer one.
    """
    kept: list[dict] = []

    def richness(table: dict) -> tuple[int, int, int]:
        rows = table.get("rows", [])
        n_cols = max((len(r) for r in rows), default=0)
        filled = sum(1 for row in rows for cell in row if _clean_cell(cell))
        return (n_cols, len(rows), filled)

    for table in sorted(tables, key=richness, reverse=True):
        bbox = table.get("bbox")

        if not bbox:
            kept.append(table)
            continue

        overlaps_existing = False

        for existing in kept:
            existing_bbox = existing.get("bbox")

            if existing_bbox and _bboxes_overlap(bbox, existing_bbox, tol=8):
                overlaps_existing = True
                break

        if not overlaps_existing:
            kept.append(table)

    return sorted(kept, key=lambda t: (t.get("bbox") or (0, 0, 0, 0))[1])


def extract_tables_robust(pdf_path: str) -> dict[int, list[dict]]:
    """
    Returns:
        {
          page_num: [
            {
              "bbox": ...,
              "header": [...],
              "rows": [[...]],
              "strategy": "A|B|C",
              "header_source": "normal|vertical|inferred",
              "near_text": "optional text above table"
            }
          ]
        }

    Existing downstream code only requires bbox/header/rows/strategy.
    Extra metadata is safe for future API/UI improvements.
    """
    by_page: dict[int, list[dict]] = {}

    with pdfplumber.open(pdf_path) as pdf:
        for pno, page in enumerate(pdf.pages, start=1):
            results: list[dict] = []

            a_results = _strategy_a(page)
            results.extend(a_results)

            # Strategy B is aggressive. Use it when A found nothing, or when
            # A found sparse/weak tables that may have missed text-aligned columns.
            run_b = not a_results or any(_is_sparse(r["rows"]) > 0.30 for r in a_results)

            if run_b:
                b_results = _strategy_b(page)

                for br in b_results:
                    rows = br["rows"]

                    if not _table_has_enough_structure(rows):
                        continue

                    results.append(br)

            # If still nothing on a page that has table-shaped text,
            # fall back to camelot.
            if not results:
                txt = page.extract_text() or ""
                digit_count = sum(c.isdigit() for c in txt)

                if txt.count("\n") > 5 and digit_count > 20:
                    results.extend(_strategy_c(pdf_path, pno))

            results = _dedupe_overlapping_tables(results)

            cleaned: list[dict] = []

            for result in results:
                rows = _forward_fill_rowspans(result["rows"])

                if not rows:
                    continue

                if _looks_like_text_columns(rows):
                    continue

                if not _table_has_enough_structure(rows):
                    continue

                bbox = tuple(result["bbox"])
                header, body, header_source = _split_header_body(rows, page=page, bbox=bbox)

                if not body:
                    continue

                cleaned.append(
                    {
                        "bbox": bbox,
                        "header": header,
                        "rows": body,
                        "strategy": result["strategy"],
                        "header_source": header_source,
                        "near_text": _extract_nearby_title(page, bbox),
                    }
                )

            if cleaned:
                by_page[pno] = cleaned

    return by_page
