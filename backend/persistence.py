"""
Persistence layer for normalized document/table comparison data.

This module is intentionally defensive:
- If database env vars are missing, persistence is skipped.
- If persistence fails, the API comparison can still complete in memory.
- IDs from in-memory dataclasses are mapped to SQL UUIDs.
"""
from __future__ import annotations

import hashlib
import json
import re
import uuid
from pathlib import Path
from typing import Any, Optional

from .db import db_enabled, get_conn
from .embeddings import embed_texts, vector_literal
from .models import Block, BlockDiff


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def _clean(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _norm(value: Any) -> str:
    return _clean(value).lower()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _payload(block: Block) -> dict:
    return block.payload if isinstance(block.payload, dict) else {}


def _table_rows(table: Block, blocks: list[Block]) -> list[Block]:
    return [
        b for b in blocks
        if b.parent_id == table.id and b.block_type.value == "table_row"
    ]


def _row_values(row: Block) -> dict[str, Any]:
    if not isinstance(row.payload, dict):
        return {}

    out = {}
    for key, value in row.payload.items():
        key = str(key)
        if key.startswith("__"):
            continue
        if key in {
            "anchors",
            "page_width",
            "page_height",
        }:
            continue
        out[key] = value

    return out


def _is_generic_column(name: str) -> bool:
    return bool(re.match(r"^(col|column|value)\s*[_-]?\s*\d+$", str(name or ""), re.I))


def _value_type(value: Any) -> str:
    text = _clean(value)

    if not text:
        return "blank"

    low = text.lower()

    if low in {"-", "--", "—", "–", ".", "•", "●", "○", "x", "s", "o", "m", "i"}:
        return "symbol"

    if "$" in text or re.search(r"\b(?:usd|eur|inr|cad)\b", low):
        return "currency"

    if re.fullmatch(r"\d{1,2}[/-]\d{1,2}[/-]\d{2,4}", text):
        return "date"

    compact = re.sub(r"[\s,%(),.-]", "", text)
    if compact and compact.isdigit():
        return "number"

    return "text"


def _semantic_role(column_name: str, column_index: int) -> str:
    low = _norm(column_name)

    if any(term in low for term in ("feature", "description", "item", "content", "name")):
        return "row_label"

    if any(term in low for term in ("order", "code", "part", "model", "sku", "ref", "reference")):
        return "code"

    if "pcv" in low or "pcb" in low:
        return "pcv"

    if any(term in low for term in ("price", "cost", "amount", "msrp", "$")):
        return "amount"

    if any(term in low for term in ("date", "year", "month")):
        return "date"

    if any(term in low for term in ("status", "availability", "available", "standard", "optional")):
        return "status"

    if column_index == 0:
        return "row_label"

    return "value"


def _table_title(table: Block) -> str:
    payload = _payload(table)

    for key in ("table_title", "title", "caption"):
        value = _clean(payload.get(key))
        if value:
            return value[:240]

    near_texts = payload.get("near_texts")
    if isinstance(near_texts, list):
        for item in near_texts:
            value = _clean(item)
            if value:
                return value[:240]

    if table.text:
        return _clean(table.text)[:240]

    return f"Table on page {table.page_number}"


def _table_context(table: Block) -> str:
    payload = _payload(table)
    context = _clean(payload.get("table_context"))

    if context:
        return context[:500]

    if table.path:
        return table.path[:500]

    return ""


def _table_pages(table: Block) -> list[int]:
    payload = _payload(table)
    pages = payload.get("spans_pages")

    if isinstance(pages, list) and pages:
        return [int(p) for p in pages if p]

    return [table.page_number]


def _table_columns(table: Block, rows: list[Block]) -> list[str]:
    payload = _payload(table)
    header = [str(h or "").strip() for h in payload.get("header", [])]

    columns = []
    seen = set()

    for idx, h in enumerate(header):
        name = h or f"Column {idx + 1}"
        if name not in seen:
            columns.append(name)
            seen.add(name)

    for row in rows:
        for key in _row_values(row).keys():
            if key not in seen:
                columns.append(key)
                seen.add(key)

    return columns


def _row_label(row: Block) -> str:
    if row.stable_key:
        return str(row.stable_key)

    values = _row_values(row)
    for value in values.values():
        text = _clean(value)
        if text:
            return text[:300]

    return _clean(row.text)[:300]


def _embedding_text(block: Block) -> str:
    payload = _payload(block)
    parts = [
        block.block_type.value,
        block.path or "",
        block.stable_key or "",
        block.text or "",
    ]

    if block.block_type.value in {"table", "table_row"}:
        title = payload.get("table_title") or payload.get("__table_title__")
        context = payload.get("table_context") or payload.get("__table_context__")
        if title:
            parts.append(str(title))
        if context:
            parts.append(str(context))

        if block.block_type.value == "table":
            header = payload.get("header")
            if isinstance(header, list):
                parts.append(" | ".join(str(h or "") for h in header))

        if block.block_type.value == "table_row":
            values = _row_values(block)
            parts.extend(f"{key}: {value}" for key, value in values.items())

    text = _clean(" | ".join(str(p or "") for p in parts))
    return text[:7500]


def _block_embeddings(blocks: list[Block]) -> dict[Any, Optional[str]]:
    texts = [_embedding_text(block) for block in blocks]

    try:
        vectors = embed_texts(texts)
    except Exception:
        vectors = [None] * len(blocks)

    return {
        block.id: vector_literal(vector)
        for block, vector in zip(blocks, vectors)
    }


def persist_run(
    *,
    run_id: str,
    family_supplier: str,
    family_name: str,
    base_label: str,
    target_label: str,
    base_pdf: Path,
    target_pdf: Path,
    base_blocks: list[Block],
    target_blocks: list[Block],
    diffs: list[BlockDiff],
    summary: list[Any],
    stats: dict,
    coverage: dict,
    base_page_count: int,
    target_page_count: int,
    enable_embeddings: bool = True,
) -> Optional[str]:
    """
    Persist comparison data to PostgreSQL.

    Returns comparison_run.id as string when successful.
    Returns None when DB is not configured.
    Raises on actual DB failures; caller should catch and log.
    """
    if not db_enabled():
        return None

    with get_conn() as conn:
        family_id = _upsert_family(conn, family_supplier, family_name)

        base_doc_id = _upsert_document(
            conn,
            family_id=family_id,
            label=base_label,
            pdf_path=base_pdf,
            page_count=base_page_count,
            coverage=coverage.get("base"),
        )
        target_doc_id = _upsert_document(
            conn,
            family_id=family_id,
            label=target_label,
            pdf_path=target_pdf,
            page_count=target_page_count,
            coverage=coverage.get("target"),
        )

        base_block_map = _insert_blocks(conn, base_doc_id, base_blocks, enable_embeddings=enable_embeddings)
        target_block_map = _insert_blocks(conn, target_doc_id, target_blocks, enable_embeddings=enable_embeddings)

        _insert_tables(conn, base_doc_id, base_blocks, base_block_map)
        _insert_tables(conn, target_doc_id, target_blocks, target_block_map)

        comparison_id = _upsert_comparison_run(
            conn,
            family_id=family_id,
            base_doc_id=base_doc_id,
            target_doc_id=target_doc_id,
            summary=summary,
            stats=stats,
        )

        _insert_block_diffs(conn, comparison_id, diffs, base_block_map, target_block_map)

        return str(comparison_id)


def _upsert_family(conn, supplier: str, family_name: str) -> uuid.UUID:
    row = conn.execute(
        """
        INSERT INTO document_family (supplier, family_name)
        VALUES (%s, %s)
        ON CONFLICT (supplier, family_name)
        DO UPDATE SET updated_at = now()
        RETURNING id
        """,
        (supplier, family_name),
    ).fetchone()

    return row["id"]


def _upsert_document(
    conn,
    *,
    family_id,
    label: str,
    pdf_path: Path,
    page_count: int,
    coverage: Optional[float],
) -> uuid.UUID:
    sha = _sha256_file(pdf_path)

    row = conn.execute(
        """
        INSERT INTO spec_document (
            family_id,
            label,
            raw_pdf_blob_uri,
            page_images_prefix,
            page_count,
            sha256,
            extracted_at,
            coverage_pct
        )
        VALUES (%s, %s, %s, %s, %s, %s, now(), %s)
        ON CONFLICT (family_id, sha256)
        DO UPDATE SET
            label = EXCLUDED.label,
            page_count = EXCLUDED.page_count,
            extracted_at = now(),
            coverage_pct = EXCLUDED.coverage_pct
        RETURNING id
        """,
        (
            family_id,
            label,
            str(pdf_path),
            "",
            page_count,
            sha,
            coverage,
        ),
    ).fetchone()

    return row["id"]


def _insert_blocks(conn, document_id, blocks: list[Block], *, enable_embeddings: bool) -> dict[Any, uuid.UUID]:
    conn.execute("DELETE FROM doc_block WHERE document_id = %s", (document_id,))

    block_id_map: dict[Any, uuid.UUID] = {}
    embeddings = _block_embeddings(blocks) if enable_embeddings else {}

    for block in blocks:
        sql_id = uuid.uuid4()
        block_id_map[block.id] = sql_id

    for block in blocks:
        sql_id = block_id_map[block.id]
        parent_sql_id = block_id_map.get(block.parent_id)

        conn.execute(
            """
            INSERT INTO doc_block (
                id,
                document_id,
                parent_id,
                block_type,
                path,
                stable_key,
                page_number,
                bbox,
                text,
                payload,
                embedding,
                content_hash,
                sequence
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::vector, %s, %s)
            """,
            (
                sql_id,
                document_id,
                parent_sql_id,
                block.block_type.value,
                block.path,
                block.stable_key,
                block.page_number,
                block.bbox,
                block.text,
                _json(block.payload or {}),
                embeddings.get(block.id),
                block.content_hash,
                block.sequence,
            ),
        )

    return block_id_map


def _insert_tables(conn, document_id, blocks: list[Block], block_id_map: dict[Any, uuid.UUID]) -> None:
    conn.execute(
        """
        DELETE FROM doc_table
        WHERE document_id = %s
        """,
        (document_id,),
    )

    table_index = 0

    for table in blocks:
        if table.block_type.value != "table":
            continue

        rows = _table_rows(table, blocks)
        columns = _table_columns(table, rows)
        pages = _table_pages(table)
        payload = _payload(table)

        table_id = uuid.uuid4()
        page_start = min(pages) if pages else table.page_number
        page_end = max(pages) if pages else table.page_number

        header_sources = payload.get("header_sources", [])
        strategies = payload.get("strategies", [])

        header_source = "mixed"
        if isinstance(header_sources, list) and header_sources:
            unique_sources = sorted(set(str(x) for x in header_sources if x))
            header_source = unique_sources[0] if len(unique_sources) == 1 else "mixed"

        extraction_strategy = "mixed"
        if isinstance(strategies, list) and strategies:
            unique_strategies = sorted(set(str(x) for x in strategies if x))
            extraction_strategy = unique_strategies[0] if len(unique_strategies) == 1 else "mixed"

        conn.execute(
            """
            INSERT INTO doc_table (
                id,
                document_id,
                block_id,
                table_index,
                title,
                context,
                page_start,
                page_end,
                pages,
                bbox_by_page,
                header_source,
                extraction_strategy,
                extraction_confidence,
                stitched_from,
                column_count,
                row_count,
                metadata
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                table_id,
                document_id,
                block_id_map.get(table.id),
                table_index,
                _table_title(table),
                _table_context(table),
                page_start,
                page_end,
                pages,
                _json({str(page): table.bbox for page in pages}),
                header_source,
                extraction_strategy,
                None,
                int(payload.get("stitched_from", 1) or 1),
                len(columns),
                len(rows),
                _json(
                    {
                        "path": table.path,
                        "payload": payload,
                        "bbox": table.bbox,
                    }
                ),
            ),
        )

        column_id_map = _insert_table_columns(conn, table_id, columns, rows, payload)
        row_id_map = _insert_table_rows(conn, table_id, rows, block_id_map)
        _insert_table_cells(conn, table_id, rows, columns, row_id_map, column_id_map)

        table_index += 1


def _insert_table_columns(conn, table_id, columns: list[str], rows: list[Block], payload: dict) -> dict[int, uuid.UUID]:
    column_id_map = {}

    for idx, column in enumerate(columns):
        column_id = uuid.uuid4()
        column_id_map[idx] = column_id

        samples = []
        value_types = []

        for row in rows[:80]:
            values = _row_values(row)
            value = values.get(column, "")
            if _clean(value) and len(samples) < 8:
                samples.append(_clean(value))
            value_types.append(_value_type(value))

        value_type_hint = "mixed"
        if value_types:
            non_blank = [v for v in value_types if v != "blank"]
            if non_blank:
                common = max(set(non_blank), key=non_blank.count)
                value_type_hint = common

        header_sources = payload.get("header_sources", [])
        header_source = header_sources[0] if isinstance(header_sources, list) and header_sources else None

        conn.execute(
            """
            INSERT INTO doc_table_column (
                id,
                table_id,
                column_index,
                header_text,
                normalized_header,
                header_source,
                semantic_role,
                value_type_hint,
                sample_values,
                confidence,
                metadata
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s::jsonb)
            """,
            (
                column_id,
                table_id,
                idx,
                column,
                _norm(column),
                header_source,
                _semantic_role(column, idx),
                value_type_hint,
                _json(samples),
                None,
                _json({"is_generic": _is_generic_column(column)}),
            ),
        )

    return column_id_map


def _insert_table_rows(conn, table_id, rows: list[Block], block_id_map: dict[Any, uuid.UUID]) -> dict[int, uuid.UUID]:
    row_id_map = {}

    for idx, row in enumerate(rows):
        row_id = uuid.uuid4()
        row_id_map[idx] = row_id

        conn.execute(
            """
            INSERT INTO doc_table_row (
                id,
                table_id,
                block_id,
                row_index,
                page_number,
                bbox,
                stable_key,
                row_label,
                row_text,
                metadata
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                row_id,
                table_id,
                block_id_map.get(row.id),
                idx,
                row.page_number,
                row.bbox,
                row.stable_key,
                _row_label(row),
                row.text,
                _json({"payload": row.payload or {}}),
            ),
        )

    return row_id_map


def _insert_table_cells(
    conn,
    table_id,
    rows: list[Block],
    columns: list[str],
    row_id_map: dict[int, uuid.UUID],
    column_id_map: dict[int, uuid.UUID],
) -> None:
    for row_idx, row in enumerate(rows):
        values = _row_values(row)
        row_id = row_id_map[row_idx]

        for col_idx, column in enumerate(columns):
            column_id = column_id_map[col_idx]
            raw_value = values.get(column, "")
            normalized_value = _norm(raw_value)

            conn.execute(
                """
                INSERT INTO doc_table_cell (
                    table_id,
                    row_id,
                    column_id,
                    row_index,
                    column_index,
                    raw_value,
                    normalized_value,
                    value_type,
                    bbox,
                    metadata
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                """,
                (
                    table_id,
                    row_id,
                    column_id,
                    row_idx,
                    col_idx,
                    _clean(raw_value),
                    normalized_value,
                    _value_type(raw_value),
                    None,
                    _json({}),
                ),
            )


def _upsert_comparison_run(
    conn,
    *,
    family_id,
    base_doc_id,
    target_doc_id,
    summary: list[Any],
    stats: dict,
) -> uuid.UUID:
    row = conn.execute(
        """
        INSERT INTO comparison_run (
            family_id,
            base_doc_id,
            target_doc_id,
            status,
            summary_json,
            stats,
            finished_at
        )
        VALUES (%s, %s, %s, 'complete', %s::jsonb, %s::jsonb, now())
        ON CONFLICT (base_doc_id, target_doc_id)
        DO UPDATE SET
            status = 'complete',
            summary_json = EXCLUDED.summary_json,
            stats = EXCLUDED.stats,
            finished_at = now(),
            error = NULL
        RETURNING id
        """,
        (
            family_id,
            base_doc_id,
            target_doc_id,
            _json([_to_plain(s) for s in summary]),
            _json(stats),
        ),
    ).fetchone()

    comparison_id = row["id"]

    conn.execute("DELETE FROM block_diff WHERE run_id = %s", (comparison_id,))

    return comparison_id


def _insert_block_diffs(
    conn,
    comparison_id,
    diffs: list[BlockDiff],
    base_block_map: dict[Any, uuid.UUID],
    target_block_map: dict[Any, uuid.UUID],
) -> None:
    for diff in diffs:
        conn.execute(
            """
            INSERT INTO block_diff (
                run_id,
                base_block_id,
                target_block_id,
                change_type,
                similarity,
                field_diffs,
                token_diff,
                impact_score
            )
            VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s)
            """,
            (
                comparison_id,
                base_block_map.get(diff.base_block_id),
                target_block_map.get(diff.target_block_id),
                diff.change_type.value,
                diff.similarity,
                _json([_to_plain(fd) for fd in diff.field_diffs]),
                _json([_to_plain(td) for td in diff.token_diff]),
                diff.impact_score,
            ),
        )


def _to_plain(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump()
    if hasattr(value, "dict"):
        return value.dict()
    if hasattr(value, "__dict__"):
        return value.__dict__
    return value
