"""
FastAPI app - orchestrates upload, extraction, diff, reports, queries.

Flow:
  POST /compare              stores files, starts background processing, returns run_id quickly
  GET  /runs/{run_id}        returns progress/status/result metadata
  GET  /                    health check

Table intelligence:
  GET  /runs/{run_id}/tables?include_rows=true
  POST /runs/{run_id}/table-view
  POST /runs/{run_id}/compare-table-columns
"""
from __future__ import annotations

import re
import json
import os
import shutil
import tempfile
import threading
import traceback
import uuid
from collections import defaultdict
from io import BytesIO
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel, Field
from rapidfuzz import fuzz

from .differ_v2 import diff_blocks, diff_stats
from .document_ingest import (
    coverage_for_source,
    extract_blocks_from_source,
    normalize_to_pdf,
    save_upload_to_source,
    source_kind,
    supported_input_extensions,
)
from .extractor_v2 import coverage_pct, extract_blocks_v2 as extract_blocks, render_pages
from .models import Block, ChangeType
from .query import ai_health, query as nl_query
from .summarizer import summarize


app = FastAPI(title="Spec-Diff", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


_RUNS: dict[str, dict] = {}


class CompareResponse(BaseModel):
    run_id: str
    status: str
    status_message: str
    progress: int


class ExtractResponse(BaseModel):
    run_id: str
    status: str
    status_message: str
    progress: int


class QueryReq(BaseModel):
    question: str
    mode: str = "fast"
    response_language: str = "source"


class AiSummaryPdfReq(BaseModel):
    title: str = "AI Summary"
    answer: str = ""
    columns: list[str] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)
    confidence: Optional[float] = None


class CompareTablesReq(BaseModel):
    base_table_id: Optional[str] = None
    target_table_id: Optional[str] = None
    base_header_query: Optional[str] = None
    target_header_query: Optional[str] = None
    base_row_key: Optional[str] = None
    target_row_key: Optional[str] = None


class TableViewReq(BaseModel):
    side: str = Field("base", description="base or target")
    table_id: str
    columns: list[str] = Field(default_factory=list)
    row_filter: Optional[str] = None
    limit: int = 300


class CompareTableColumnsReq(BaseModel):
    base_table_id: str
    target_table_id: str

    # Columns used to identify/align rows. If empty, backend chooses likely label columns.
    base_row_columns: list[str] = Field(default_factory=list)
    target_row_columns: list[str] = Field(default_factory=list)

    # Columns whose values should be compared. If empty, all non-row-label columns are used.
    base_value_columns: list[str] = Field(default_factory=list)
    target_value_columns: list[str] = Field(default_factory=list)

    # Optional row filter. Supports exact/fuzzy matching against row label/cells.
    row_filter: Optional[str] = None

    # Maximum output rows.
    limit: int = 200

    # Optional focused AI review over only this selected table slice.
    use_ai: bool = False
    question: Optional[str] = None


def _dump_model(obj):
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    if hasattr(obj, "dict"):
        return obj.dict()
    return obj


def _max_block_page(blocks: list[Block]) -> int:
    pages = [int(getattr(block, "page_number", 1) or 1) for block in blocks or []]
    return max(pages, default=1)


def _set_run_status(run_id: str, message: str, progress: int, status: str = "running") -> None:
    if run_id not in _RUNS:
        _RUNS[run_id] = {}

    _RUNS[run_id].update(
        {
            "status": status,
            "status_message": message,
            "progress": progress,
        }
    )


def _ensure_run(run_id: str) -> dict:
    r = _RUNS.get(run_id)
    if not r:
        raise HTTPException(404, "no such run")
    return r


def _ensure_complete(run_id: str) -> dict:
    r = _ensure_run(run_id)

    if r.get("status") == "failed":
        raise HTTPException(500, r.get("error", "Comparison failed"))

    if r.get("status") != "complete":
        raise HTTPException(409, r.get("status_message", "Comparison is still running"))

    return r


def _ensure_extraction_complete(run_id: str) -> dict:
    r = _ensure_run(run_id)

    if r.get("kind") != "extraction":
        raise HTTPException(404, "no such extraction run")

    if r.get("status") == "failed":
        raise HTTPException(500, r.get("error", "Extraction failed"))

    if r.get("status") != "complete":
        raise HTTPException(409, r.get("status_message", "Extraction is still running"))

    return r


def _db_health_payload() -> dict:
    try:
        from .db import ping_db
    except Exception as exc:
        return {
            "enabled": False,
            "ok": False,
            "error": f"Database module could not be loaded: {exc}",
        }

    return ping_db()


def _persist_run_safely(
    *,
    run_id: str,
    base_label: str,
    target_label: str,
    base_pdf: Path,
    target_pdf: Path,
    base_blocks: list[Block],
    target_blocks: list[Block],
    diffs: list,
    summary: list,
    stats: dict,
    coverage: dict,
    base_page_count: int,
    target_page_count: int,
    enable_embeddings: bool = True,
) -> tuple[Optional[int], Optional[str]]:
    try:
        from .persistence import persist_run
    except Exception as exc:
        return None, f"Persistence module could not be loaded: {exc}"

    try:
        db_run_id = persist_run(
            run_id=run_id,
            family_supplier="uploaded",
            family_name="document_comparison",
            base_label=base_label,
            target_label=target_label,
            base_pdf=base_pdf,
            target_pdf=target_pdf,
            base_blocks=base_blocks,
            target_blocks=target_blocks,
            diffs=diffs,
            summary=summary,
            stats=stats,
            coverage=coverage,
            base_page_count=base_page_count,
            target_page_count=target_page_count,
            enable_embeddings=enable_embeddings,
        )
        return db_run_id, None
    except Exception:
        return None, traceback.format_exc()


def _block_record(block: Block, *, include_payload: bool = True) -> dict[str, Any]:
    payload = block.payload if isinstance(block.payload, dict) else {}
    record = {
        "id": str(block.id),
        "parent_id": str(block.parent_id) if block.parent_id else None,
        "type": block.block_type.value,
        "path": block.path,
        "stable_key": block.stable_key,
        "page_number": block.page_number,
        "bbox": block.bbox,
        "text": block.text,
        "sequence": block.sequence,
    }
    if include_payload:
        record["payload"] = payload
    return record


def _extraction_summary(blocks: list[Block], coverage: float, page_count: int, source_format: str) -> dict[str, Any]:
    counts: dict[str, int] = defaultdict(int)
    table_count = 0
    row_count = 0
    figure_count = 0
    text_chars = 0

    for block in blocks:
        counts[block.block_type.value] += 1
        text_chars += len(block.text or "")
        if block.block_type.value == "table":
            table_count += 1
        elif block.block_type.value == "table_row":
            row_count += 1
        elif block.block_type.value == "figure":
            figure_count += 1

    quality = "high"
    if coverage < 65:
        quality = "low"
    elif coverage < 85:
        quality = "medium"

    return {
        "source_format": source_format,
        "page_count": page_count,
        "coverage": coverage,
        "quality": quality,
        "block_counts": dict(counts),
        "table_count": table_count,
        "table_row_count": row_count,
        "figure_count": figure_count,
        "text_characters": text_chars,
        "message": (
            f"Extracted {len(blocks)} semantic block(s), {table_count} table(s), "
            f"{row_count} table row(s), and {figure_count} image/figure block(s)."
        ),
    }


def _adjust_extraction_blocks(
    blocks: list[Block],
    *,
    doc_index: int,
    label: str,
    page_offset: int,
) -> list[Block]:
    """
    Multiple uploaded files are exposed as one extraction workspace. Prefix
    paths and offset pages so page lookup, citations, and JSON remain stable.
    """
    label_slug = re.sub(r"[^A-Za-z0-9]+", "_", str(label or f"document_{doc_index}")).strip("_").lower() or f"document_{doc_index}"

    for block in blocks:
        block.page_number = int(block.page_number or 1) + page_offset
        original_path = block.path or f"/block_{block.sequence}"
        if not original_path.startswith(f"/{label_slug}/"):
            block.path = f"/{label_slug}{original_path if original_path.startswith('/') else '/' + original_path}"

        payload = block.payload if isinstance(block.payload, dict) else {}
        payload["document_index"] = doc_index
        payload["document_label"] = label
        payload["original_page_number"] = int(block.page_number or 1) - page_offset

        for pages_key in ("spans_pages", "__pages__"):
            if isinstance(payload.get(pages_key), list):
                payload[pages_key] = [
                    int(page or 1) + page_offset
                    for page in payload.get(pages_key, [])
                    if str(page or "").strip()
                ]

        if isinstance(payload.get("bbox_by_page"), dict):
            payload["bbox_by_page"] = {
                str(int(page) + page_offset): bbox
                for page, bbox in payload.get("bbox_by_page", {}).items()
                if str(page).isdigit()
            }

        block.payload = payload

    return blocks


def _semantic_field_candidates(blocks: list[Block], limit: int = 220) -> list[dict[str, Any]]:
    fields = []
    seen = set()
    key_value_rx = re.compile(r"^\s*([^:：]{2,80})\s*[:：]\s*(.{1,300})$")
    attribute_patterns = [
        ("color", re.compile(r"\b(?:colou?r|shade)\s*(?:is|=|:)?\s*([A-Za-z][A-Za-z\s/-]{2,40})", re.I)),
        ("size", re.compile(r"\b(?:size|dimension)\s*(?:is|=|:)?\s*([A-Z0-9][A-Z0-9\s./x-]{0,40})", re.I)),
        ("quantity", re.compile(r"\b(?:qty|quantity|count|units?)\s*(?:is|=|:)?\s*(\d[\d,]*(?:\.\d+)?)", re.I)),
        ("price", re.compile(r"([$€£]\s?\d[\d,]*(?:\.\d+)?)")),
        ("percentage", re.compile(r"\b(\d+(?:\.\d+)?%)\b")),
        ("date", re.compile(r"\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}-\d{1,2}-\d{1,2})\b")),
        ("code", re.compile(r"\b([A-Z]{1,8}[- ]?\d{2,12}[A-Z]?)\b", re.I)),
    ]

    for block in blocks:
        payload = block.payload if isinstance(block.payload, dict) else {}
        text = block.text or payload.get("text") or ""
        match = key_value_rx.match(str(text))
        if match:
            key = re.sub(r"\s+", " ", match.group(1)).strip()
            value = re.sub(r"\s+", " ", match.group(2)).strip()
            dedupe = (block.page_number, key.lower(), value.lower())
            if dedupe not in seen:
                seen.add(dedupe)
                fields.append(
                    {
                        "field": key,
                        "value": value,
                        "page": block.page_number,
                        "source": block.block_type.value,
                        "citation": f"p.{block.page_number} - {_path_label(block.path)}",
                    }
                )

        for field_name, pattern in attribute_patterns:
            for attr_match in pattern.finditer(str(text)):
                value = re.sub(r"\s+", " ", attr_match.group(1)).strip()
                if not value:
                    continue
                dedupe = (block.page_number, field_name, value.lower())
                if dedupe in seen:
                    continue
                seen.add(dedupe)
                fields.append(
                    {
                        "field": field_name,
                        "value": value,
                        "page": block.page_number,
                        "source": block.block_type.value,
                        "citation": f"p.{block.page_number} - {_path_label(block.path)}",
                    }
                )

        if block.block_type.value == "table_row":
            for key, value in payload.items():
                if str(key).startswith("__") or str(key) in {"source_format", "page_width", "page_height", "anchors"}:
                    continue
                clean_value = str(value or "").strip()
                if not clean_value:
                    continue
                dedupe = (block.page_number, str(key).lower(), clean_value.lower())
                if dedupe in seen:
                    continue
                seen.add(dedupe)
                fields.append(
                    {
                        "field": str(key),
                        "value": clean_value,
                        "page": block.page_number,
                        "source": "table_row",
                        "table": payload.get("__table_title__"),
                        "citation": f"p.{block.page_number} - {_path_label(block.path)}",
                    }
                )

        if len(fields) >= limit:
            break

    return fields


def _extract_text_fields(text: Any, page: int, path: str, source: str) -> list[dict[str, Any]]:
    raw = re.sub(r"\s+", " ", str(text or "")).strip()
    if not raw:
        return []

    fields = []
    seen = set()
    key_value_pairs = re.findall(
        r"([^\s:：|,;][^:：|,;]{1,70})\s*[:：]\s*([^|,;]{1,260})",
        raw,
        flags=re.UNICODE,
    )
    for key, value in key_value_pairs:
        clean_key = re.sub(r"\s+", " ", key).strip()
        clean_value = re.sub(r"\s+", " ", value).strip()
        if not clean_key or not clean_value:
            continue
        dedupe = (clean_key.lower(), clean_value.lower())
        if dedupe in seen:
            continue
        seen.add(dedupe)
        fields.append(
            {
                "field": clean_key,
                "value": clean_value,
                "page": page,
                "source": source,
                "citation": f"p.{page} - {_path_label(path)}",
            }
        )

    attribute_patterns = [
        ("color", re.compile(r"\b(?:colou?r|shade)\s*(?:is|=|:)?\s*([A-Za-z][A-Za-z\s/-]{2,40})", re.I)),
        ("size", re.compile(r"\b(?:size|dimension)\s*(?:is|=|:)?\s*([A-Z0-9][A-Z0-9\s./x-]{0,40})", re.I)),
        ("quantity", re.compile(r"\b(?:qty|quantity|count|units?)\s*(?:is|=|:)?\s*(\d[\d,]*(?:\.\d+)?)", re.I)),
        ("price", re.compile(r"([$€£]\s?\d[\d,]*(?:\.\d+)?)")),
        ("percentage", re.compile(r"\b(\d+(?:\.\d+)?%)\b")),
        ("date", re.compile(r"\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}-\d{1,2}-\d{1,2})\b")),
        ("code", re.compile(r"\b([A-Z]{1,8}[- ]?\d{2,12}[A-Z]?)\b", re.I)),
    ]

    for field_name, pattern in attribute_patterns:
        for attr_match in pattern.finditer(raw):
            value = re.sub(r"\s+", " ", attr_match.group(1)).strip()
            if not value:
                continue
            dedupe = (field_name, value.lower())
            if dedupe in seen:
                continue
            seen.add(dedupe)
            fields.append(
                {
                    "field": field_name,
                    "value": value,
                    "page": page,
                    "source": source,
                    "citation": f"p.{page} - {_path_label(path)}",
                }
            )

    return fields


def _inline_record_from_text(text: Any) -> Optional[dict[str, Any]]:
    raw = re.sub(r"\s+", " ", str(text or "")).strip()
    if not raw:
        return None

    if "|" in raw:
        cells = [part.strip() for part in raw.split("|") if part.strip()]
    elif "\t" in raw:
        cells = [part.strip() for part in raw.split("\t") if part.strip()]
    else:
        cells = [part.strip() for part in re.split(r"\s{3,}", str(text or "")) if part.strip()]

    if len(cells) >= 2:
        return {
            "record_type": "inline_row",
            "columns": [f"Column {idx + 1}" for idx in range(len(cells))],
            "values": {f"Column {idx + 1}": value for idx, value in enumerate(cells)},
            "text": raw,
        }

    fields = _extract_text_fields(raw, 0, "", "inline_text")
    if len(fields) >= 2:
        return {
            "record_type": "inline_key_values",
            "values": {field["field"]: field["value"] for field in fields},
            "text": raw,
        }

    return None


def _business_structure(blocks: list[Block], tables: list[dict[str, Any]]) -> dict[str, Any]:
    table_by_path = {table.get("path"): table for table in tables}
    table_paths = set(table_by_path.keys())
    documents: dict[int, dict[str, Any]] = {}
    section_by_doc: dict[int, dict[str, Any]] = {}

    def _doc_for(block: Block) -> dict[str, Any]:
        payload = block.payload if isinstance(block.payload, dict) else {}
        doc_index = int(payload.get("document_index") or 1)
        doc_label = payload.get("document_label") or "document"
        if doc_index not in documents:
            documents[doc_index] = {
                "document_index": doc_index,
                "label": doc_label,
                "sections": [],
            }
        return documents[doc_index]

    def _ensure_page_section(block: Block) -> dict[str, Any]:
        doc = _doc_for(block)
        doc_index = int(doc["document_index"])
        current = section_by_doc.get(doc_index)
        if current and current.get("page") == block.page_number:
            return current

        section = {
            "title": f"Page {block.page_number}",
            "page": block.page_number,
            "path": f"/page_{block.page_number}",
            "content": [],
            "fields": [],
            "inline_records": [],
            "tables": [],
        }
        doc["sections"].append(section)
        section_by_doc[doc_index] = section
        return section

    for block in sorted(blocks, key=lambda b: (b.page_number, b.sequence)):
        payload = block.payload if isinstance(block.payload, dict) else {}
        doc = _doc_for(block)
        doc_index = int(doc["document_index"])

        if block.block_type.value in {"section", "heading"}:
            section = {
                "title": block.text or _path_label(block.path),
                "page": block.page_number,
                "path": block.path,
                "content": [],
                "fields": [],
                "inline_records": [],
                "tables": [],
            }
            doc["sections"].append(section)
            section_by_doc[doc_index] = section
            continue

        section = section_by_doc.get(doc_index) or _ensure_page_section(block)

        if block.block_type.value == "table":
            table = table_by_path.get(block.path)
            if table:
                section["tables"].append(
                    {
                        "title": table.get("display_name") or table.get("title") or "Detected table",
                        "page_label": table.get("page_label"),
                        "columns": table.get("columns", []),
                        "row_count": table.get("n_rows", 0),
                        "sample_rows": (table.get("rows") or table.get("row_preview") or [])[:8],
                    }
                )
            continue

        if block.block_type.value == "table_row" or block.path in table_paths:
            continue

        text = block.text or payload.get("text") or payload.get("layout_text") or ""
        if not str(text).strip():
            continue

        fields = _extract_text_fields(text, block.page_number, block.path, block.block_type.value)
        inline_record = _inline_record_from_text(text)

        content_item = {
            "type": block.block_type.value,
            "page": block.page_number,
            "path": block.path,
            "text": text,
        }
        if fields:
            content_item["fields"] = fields
            section["fields"].extend(fields)
        if inline_record:
            inline_record["page"] = block.page_number
            inline_record["citation"] = f"p.{block.page_number} - {_path_label(block.path)}"
            section["inline_records"].append(inline_record)

        section["content"].append(content_item)

    for doc in documents.values():
        if not doc["sections"]:
            doc["sections"].append(
                {
                    "title": "Document",
                    "page": 1,
                    "path": "/document",
                    "content": [],
                    "fields": [],
                    "inline_records": [],
                    "tables": [],
                }
            )

        for section in doc["sections"]:
            section["content"] = section["content"][:80]
            section["fields"] = section["fields"][:80]
            section["inline_records"] = section["inline_records"][:40]
            section["tables"] = section["tables"][:20]

    return {
        "documents": [documents[key] for key in sorted(documents)],
        "section_count": sum(len(doc["sections"]) for doc in documents.values()),
    }


def _structured_extraction_json(r: dict, run_id: str) -> dict[str, Any]:
    blocks = r.get("blocks", [])
    tables = [
        _table_matrix(block, blocks, include_rows=True)
        for block in blocks
        if block.block_type.value == "table"
    ]
    sections = [
        {
            "title": block.text or _path_label(block.path),
            "page": block.page_number,
            "path": block.path,
            "text": block.text,
        }
        for block in blocks
        if block.block_type.value in {"section", "heading"}
    ][:200]
    text_blocks = [
        {
            "page": block.page_number,
            "type": block.block_type.value,
            "path": block.path,
            "text": block.text,
        }
        for block in blocks
        if block.block_type.value in {"paragraph", "list_item", "kv_pair", "figure"}
    ][:500]

    return {
        "run_id": run_id,
        "documents": r.get("documents") or [
            {
                "label": r.get("label"),
                "source_format": r.get("source_format"),
                "page_start": 1,
                "page_count": len(r.get("page_imgs", [])),
            }
        ],
        "summary": r.get("summary", {}),
        "coverage": r.get("coverage"),
        "semantic_fields": _semantic_field_candidates(blocks),
        "business_structure": _business_structure(blocks, tables),
        "sections": sections,
        "tables": tables,
        "text_blocks": text_blocks,
        "ai_analysis": r.get("ai_analysis"),
    }


def _curated_extraction_context(blocks: list[Block], limit_chars: int = 42000) -> str:
    parts = []
    used = 0

    priority = {"section": 0, "heading": 1, "table": 2, "paragraph": 3, "kv_pair": 4, "list_item": 5, "figure": 6, "table_row": 7}
    for block in sorted(blocks, key=lambda b: (priority.get(b.block_type.value, 9), b.page_number, b.sequence)):
        payload = block.payload if isinstance(block.payload, dict) else {}
        if block.block_type.value == "table":
            header = payload.get("header") or []
            rows = payload.get("rows") or []
            text = f"Page {block.page_number} table {block.text or block.path}. Columns: {' | '.join(str(h) for h in header)}. Sample rows: {json.dumps(rows[:5], ensure_ascii=False, default=str)}"
        elif block.block_type.value == "table_row":
            continue
        else:
            text = f"Page {block.page_number} {block.block_type.value}: {block.text or payload.get('text') or ''}"

        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            continue
        if used + len(text) > limit_chars:
            break
        parts.append(text)
        used += len(text)

    return "\n".join(parts)


def _ai_extraction_summary(blocks: list[Block], summary: dict[str, Any]) -> Optional[dict[str, Any]]:
    try:
        from openai import AzureOpenAI
    except Exception as exc:
        return {"available": False, "error": f"Azure OpenAI library is unavailable: {exc}"}

    endpoint = None
    api_key = None
    deployment = None
    for name in ("AZURE_OPENAI_ENDPOINT",):
        endpoint = endpoint or os.getenv(name)
    for name in ("AZURE_OPENAI_API_KEY",):
        api_key = api_key or os.getenv(name)
    for name in ("AZURE_OPENAI_DEPLOYMENT", "AZURE_OPENAI_CHAT_DEPLOYMENT", "AZURE_OPENAI_DEPLOYMENT_NAME", "AZURE_OPENAI_MODEL"):
        deployment = deployment or os.getenv(name)

    if not (endpoint and api_key and deployment):
        return {
            "available": False,
            "error": "Azure OpenAI is not configured. Set endpoint, API key, and chat deployment.",
        }

    context = _curated_extraction_context(blocks)
    if not context:
        return {"available": False, "error": "No extracted content was available for AI analysis."}

    prompt = {
        "document_stats": summary,
        "extracted_context": context,
    }

    try:
        client = AzureOpenAI(
            api_key=api_key,
            azure_endpoint=endpoint,
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-08-01-preview"),
        )
        response = client.chat.completions.create(
            model=deployment,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You analyze extracted document content only. "
                        "Return concise JSON with keys: executive_summary, key_items, tables_found, "
                        "quality_notes, recommended_review. Do not invent facts not present in context."
                    ),
                },
                {"role": "user", "content": json.dumps(prompt, ensure_ascii=False, default=str)},
            ],
            temperature=0.1,
            max_tokens=1400,
            response_format={"type": "json_object"},
        )
        content = response.choices[0].message.content or "{}"
        return {"available": True, "result": json.loads(content)}
    except Exception as exc:
        return {"available": False, "error": f"Azure OpenAI extraction analysis failed: {type(exc).__name__}: {exc}"}


def _process_extract(
    run_id: str,
    work: Path,
    sources: list[Path],
    label: str,
    use_ai: bool,
) -> None:
    try:
        _set_run_status(run_id, "Preparing uploaded document", 12)

        converted_dir = work / "converted"
        all_blocks: list[Block] = []
        all_page_imgs: list[Path] = []
        pdf_paths: list[Path] = []
        coverages: list[float] = []
        documents_meta: list[dict[str, Any]] = []
        page_offset = 0

        total_sources = max(1, len(sources))
        for idx, source in enumerate(sources, start=1):
            source_label = Path(source).stem.replace("extract_", "", 1)
            fmt = source_kind(source)
            progress_base = 12 + int((idx - 1) * 52 / total_sources)

            _set_run_status(run_id, f"Converting {source_label}", progress_base)
            pdf_path = normalize_to_pdf(source, converted_dir / f"extract_{idx}")
            pdf_paths.append(pdf_path)

            _set_run_status(run_id, f"Rendering preview pages for {source_label}", min(62, progress_base + 10))
            page_imgs = render_pages(str(pdf_path), str(work / f"pages_extract_{idx}"))
            all_page_imgs.extend(page_imgs)
            _RUNS[run_id]["page_imgs"] = all_page_imgs

            _set_run_status(run_id, f"Extracting text, tables, and image content from {source_label}", min(70, progress_base + 24))
            blocks = extract_blocks_from_source(source, pdf_path, extract_blocks)
            blocks = _adjust_extraction_blocks(
                blocks,
                doc_index=idx,
                label=source_label,
                page_offset=page_offset,
            )
            all_blocks.extend(blocks)

            _set_run_status(run_id, f"Checking extraction coverage for {source_label}", min(76, progress_base + 38))
            coverage = coverage_for_source(source, pdf_path, blocks, coverage_pct)
            coverages.append(coverage)
            documents_meta.append(
                {
                    "index": idx,
                    "label": source_label,
                    "filename": source.name,
                    "source_format": fmt,
                    "pdf_path": str(pdf_path),
                    "page_start": page_offset + 1,
                    "page_count": len(page_imgs),
                    "native_pages": _max_block_page(blocks),
                    "coverage": coverage,
                }
            )
            page_offset += len(page_imgs)

        blocks = all_blocks
        page_imgs = all_page_imgs
        fmt = source_kind(sources[0]) if len(sources) == 1 else "mixed"
        coverage = round(sum(coverages) / len(coverages), 2) if coverages else 0.0
        summary = _extraction_summary(blocks, coverage, len(page_imgs), fmt)
        summary["document_count"] = len(sources)
        summary["documents"] = documents_meta

        ai_analysis = None
        if use_ai:
            _set_run_status(run_id, "Preparing AI-assisted document analysis", 78)
            ai_analysis = _ai_extraction_summary(blocks, summary)

        _set_run_status(run_id, "Finalizing extraction workspace", 92)

        _RUNS[run_id].update(
            {
                "status": "complete",
                "status_message": "Extraction complete",
                "progress": 100,
                "kind": "extraction",
                "work": work,
                "label": label,
                "source": sources[0] if sources else None,
                "sources": sources,
                "pdf": pdf_paths[0] if pdf_paths else None,
                "pdfs": pdf_paths,
                "source_format": fmt,
                "documents": documents_meta,
                "page_imgs": page_imgs,
                "native_pages": _max_block_page(blocks),
                "blocks": blocks,
                "coverage": coverage,
                "summary": summary,
                "ai_analysis": ai_analysis,
            }
        )
    except Exception as exc:
        _RUNS[run_id].update(
            {
                "status": "failed",
                "status_message": "Extraction failed",
                "progress": _RUNS.get(run_id, {}).get("progress", 0),
                "error": str(exc),
                "traceback": traceback.format_exc(),
            }
        )


def _process_compare(
    run_id: str,
    work: Path,
    base_source: Path,
    target_source: Path,
    base_label: str,
    target_label: str,
    use_llm: bool,
) -> None:
    try:
        _set_run_status(run_id, "Preparing uploaded documents", 10)

        converted_dir = work / "converted"
        base_pdf = normalize_to_pdf(base_source, converted_dir / "base")
        target_pdf = normalize_to_pdf(target_source, converted_dir / "target")

        _RUNS[run_id].update(
            {
                "base_source": base_source,
                "target_source": target_source,
                "base_pdf": base_pdf,
                "target_pdf": target_pdf,
                "base_format": source_kind(base_source),
                "target_format": source_kind(target_source),
            }
        )

        _set_run_status(run_id, "Rendering document pages", 18)

        base_imgs = render_pages(str(base_pdf), str(work / "pages_base"))
        target_imgs = render_pages(str(target_pdf), str(work / "pages_target"))

        _RUNS[run_id].update(
            {
                "base_imgs": base_imgs,
                "target_imgs": target_imgs,
            }
        )

        _set_run_status(run_id, "Extracting text, tables, and document structure", 36)

        base_blocks = extract_blocks_from_source(base_source, base_pdf, extract_blocks)
        target_blocks = extract_blocks_from_source(target_source, target_pdf, extract_blocks)

        _set_run_status(run_id, "Checking extraction coverage", 50)

        cov_b = coverage_for_source(base_source, base_pdf, base_blocks, coverage_pct)
        cov_t = coverage_for_source(target_source, target_pdf, target_blocks, coverage_pct)

        _set_run_status(run_id, "Comparing semantic changes", 64)

        diffs = diff_blocks(base_blocks, target_blocks)
        stats = diff_stats(diffs)

        _set_run_status(
            run_id,
            "Preparing AI review summary" if use_llm else "Preparing review summary",
            78,
        )

        summary = summarize(diffs, base_blocks, target_blocks, use_llm=use_llm)

        _set_run_status(run_id, "Storing extracted tables and comparison data", 88)

        coverage = {"base": cov_b, "target": cov_t}
        db_run_id, db_error = _persist_run_safely(
            run_id=run_id,
            base_label=base_label,
            target_label=target_label,
            base_pdf=base_pdf,
            target_pdf=target_pdf,
            base_blocks=base_blocks,
            target_blocks=target_blocks,
            diffs=diffs,
            summary=summary,
            stats=stats,
            coverage=coverage,
            base_page_count=len(base_imgs),
            target_page_count=len(target_imgs),
            enable_embeddings=use_llm,
        )

        _RUNS[run_id].update(
            {
                "status": "complete",
                "status_message": "Comparison complete",
                "progress": 100,
                "work": work,
                "base_pdf": base_pdf,
                "target_pdf": target_pdf,
                "base_source": base_source,
                "target_source": target_source,
                "base_format": source_kind(base_source),
                "target_format": source_kind(target_source),
                "base_native_pages": _max_block_page(base_blocks),
                "target_native_pages": _max_block_page(target_blocks),
                "base_label": base_label,
                "target_label": target_label,
                "base_imgs": base_imgs,
                "target_imgs": target_imgs,
                "base_blocks": base_blocks,
                "target_blocks": target_blocks,
                "diffs": diffs,
                "stats": stats,
                "summary": summary,
                "coverage": coverage,
                "db_run_id": db_run_id,
                "db_error": db_error,
            }
        )

    except Exception as exc:
        _RUNS[run_id].update(
            {
                "status": "failed",
                "status_message": "Comparison failed",
                "progress": _RUNS.get(run_id, {}).get("progress", 0),
                "error": str(exc),
                "traceback": traceback.format_exc(),
            }
        )


@app.get("/")
def root():
    return {
        "status": "ok",
        "name": "doculens-ai-agent",
        "endpoints": [
            "POST /extract",
            "GET /extract-runs/{id}",
            "GET /extract-runs/{id}/blocks",
            "GET /extract-runs/{id}/tables",
            "GET /extract-runs/{id}/images",
            "GET /extract-runs/{id}/structured-json",
            "GET /extract-runs/{id}/json",
            "POST /compare",
            "GET /db-health",
            "GET /ai-health",
            "GET /runs/{id}",
            "GET /runs/{id}/diff",
            "GET /runs/{id}/summary",
            "GET /runs/{id}/report.pdf",
            "POST /runs/{id}/ai-summary.pdf",
            "POST /runs/{id}/query",
            "GET /runs/{id}/pages/{side}/{n}",
            "GET /runs/{id}/native-page/{side}/{n}",
            "GET /runs/{id}/overlay/{side}/{n}",
            "GET /runs/{id}/tables",
            "POST /runs/{id}/table-view",
            "POST /runs/{id}/compare-tables",
            "POST /runs/{id}/compare-table-columns",
            "POST /runs/{id}/table-report.pdf",
        ],
        "supported_upload_formats": supported_input_extensions(),
    }


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/db-health")
def db_health():
    return _db_health_payload()


@app.get("/ai-health")
def get_ai_health():
    return ai_health()


@app.post("/compare", response_model=CompareResponse)
async def compare(
    base: UploadFile = File(..., description="Older / previous version document"),
    target: UploadFile = File(..., description="Newer / current version document"),
    use_llm: bool = Form(False),
):
    if not base.filename or not target.filename:
        raise HTTPException(400, "Both files required")

    run_id = str(uuid.uuid4())
    work = Path(tempfile.mkdtemp(prefix=f"specdiff_{run_id}_"))

    base_label = Path(base.filename).stem
    target_label = Path(target.filename).stem

    _RUNS[run_id] = {
        "status": "queued",
        "status_message": "Uploading documents",
        "progress": 5,
        "work": work,
        "base_label": base_label,
        "target_label": target_label,
        "base_imgs": [],
        "target_imgs": [],
        "stats": {},
        "coverage": {},
        "supported_upload_formats": supported_input_extensions(),
    }

    try:
        base_source = save_upload_to_source(base, work, "base")
        target_source = save_upload_to_source(target, work, "target")
        _RUNS[run_id].update(
            {
                "base_source": base_source,
                "target_source": target_source,
                "base_format": source_kind(base_source),
                "target_format": source_kind(target_source),
            }
        )
    except Exception as exc:
        _RUNS[run_id].update(
            {
                "status": "failed",
                "status_message": "Could not save uploaded documents",
                "progress": 0,
                "error": str(exc),
                "traceback": traceback.format_exc(),
            }
        )
        raise HTTPException(500, "Could not save uploaded documents")

    worker = threading.Thread(
        target=_process_compare,
        args=(
            run_id,
            work,
            base_source,
            target_source,
            base_label,
            target_label,
            use_llm,
        ),
        daemon=True,
    )
    worker.start()

    return CompareResponse(
        run_id=run_id,
        status="queued",
        status_message="Documents uploaded. Comparison is starting.",
        progress=5,
    )


@app.post("/extract", response_model=ExtractResponse)
async def extract_document(
    document: list[UploadFile] = File(..., description="One or more documents/images to extract"),
    use_ai: bool = Form(False),
):
    uploads = [item for item in document if item and item.filename]
    if not uploads:
        raise HTTPException(400, "At least one document file is required")

    run_id = str(uuid.uuid4())
    work = Path(tempfile.mkdtemp(prefix=f"doc_extract_{run_id}_"))
    label = Path(uploads[0].filename or "document").stem if len(uploads) == 1 else f"{len(uploads)} documents"

    _RUNS[run_id] = {
        "kind": "extraction",
        "status": "queued",
        "status_message": "Uploading document",
        "progress": 5,
        "work": work,
        "label": label,
        "page_imgs": [],
        "coverage": None,
        "summary": {},
        "supported_upload_formats": supported_input_extensions(),
    }

    try:
        sources = [
            save_upload_to_source(upload, work, f"extract_{idx + 1}")
            for idx, upload in enumerate(uploads)
        ]
        _RUNS[run_id].update(
            {
                "source": sources[0],
                "sources": sources,
                "source_format": source_kind(sources[0]) if len(sources) == 1 else "mixed",
                "documents": [
                    {
                        "index": idx + 1,
                        "label": Path(upload.filename or source.name).stem,
                        "filename": upload.filename,
                        "source_format": source_kind(source),
                    }
                    for idx, (upload, source) in enumerate(zip(uploads, sources))
                ],
            }
        )
    except Exception as exc:
        _RUNS[run_id].update(
            {
                "status": "failed",
                "status_message": "Could not save uploaded document",
                "progress": 0,
                "error": str(exc),
                "traceback": traceback.format_exc(),
            }
        )
        raise HTTPException(500, "Could not save uploaded document")

    worker = threading.Thread(
        target=_process_extract,
        args=(run_id, work, sources, label, use_ai),
        daemon=True,
    )
    worker.start()

    return ExtractResponse(
        run_id=run_id,
        status="queued",
        status_message="Document uploaded. Extraction is starting.",
        progress=5,
    )


@app.get("/extract-runs/{run_id}")
def extract_run_meta(run_id: str):
    r = _ensure_run(run_id)

    if r.get("kind") != "extraction":
        raise HTTPException(404, "no such extraction run")

    return {
        "run_id": run_id,
        "kind": "extraction",
        "status": r.get("status", "running"),
        "status_message": r.get("status_message", "Working"),
        "progress": r.get("progress", 0),
        "error": r.get("error"),
        "traceback": r.get("traceback"),
        "label": r.get("label"),
        "source_format": r.get("source_format"),
        "documents": r.get("documents", []),
        "supported_upload_formats": supported_input_extensions(),
        "coverage": r.get("coverage"),
        "summary": r.get("summary", {}),
        "ai_analysis": r.get("ai_analysis"),
        "n_pages": len(r.get("page_imgs", [])),
        "native_pages": r.get("native_pages") or _max_block_page(r.get("blocks", [])),
    }


@app.get("/extract-runs/{run_id}/pages/{n}")
def get_extract_page(run_id: str, n: int):
    r = _ensure_extraction_complete(run_id)
    imgs = r.get("page_imgs", [])

    if n < 1 or n > len(imgs):
        raise HTTPException(404, "page out of range")

    return FileResponse(imgs[n - 1], media_type="image/png")


@app.get("/extract-runs/{run_id}/blocks")
def get_extract_blocks(
    run_id: str,
    block_type: Optional[str] = None,
    page: Optional[int] = None,
    limit: int = 500,
):
    r = _ensure_extraction_complete(run_id)
    blocks = r.get("blocks", [])
    out = []

    for block in blocks:
        if block_type and block.block_type.value != block_type:
            continue
        if page and block.page_number != page:
            continue
        out.append(_block_record(block, include_payload=True))
        if len(out) >= max(1, min(limit, 2000)):
            break

    return {"blocks": out, "count": len(out), "total_blocks": len(blocks)}


@app.get("/extract-runs/{run_id}/tables")
def get_extract_tables(run_id: str, include_rows: bool = False):
    r = _ensure_extraction_complete(run_id)
    blocks = r.get("blocks", [])
    tables = [
        _table_matrix(block, blocks, include_rows=include_rows)
        for block in blocks
        if block.block_type.value == "table"
    ]
    return {"tables": tables, "count": len(tables)}


@app.get("/extract-runs/{run_id}/images")
def get_extract_images(run_id: str):
    r = _ensure_extraction_complete(run_id)
    figures = [
        _block_record(block, include_payload=True)
        for block in r.get("blocks", [])
        if block.block_type.value == "figure"
    ]
    return {"images": figures, "count": len(figures)}


@app.get("/extract-runs/{run_id}/json")
def download_extract_json(run_id: str):
    r = _ensure_extraction_complete(run_id)
    blocks = r.get("blocks", [])
    tables = [
        _table_matrix(block, blocks, include_rows=True)
        for block in blocks
        if block.block_type.value == "table"
    ]
    payload = {
        "run_id": run_id,
        "label": r.get("label"),
        "source_format": r.get("source_format"),
        "documents": r.get("documents", []),
        "coverage": r.get("coverage"),
        "summary": r.get("summary", {}),
        "ai_analysis": r.get("ai_analysis"),
        "structured_json": _structured_extraction_json(r, run_id),
        "blocks": [_block_record(block, include_payload=True) for block in blocks],
        "tables": tables,
    }
    filename = f"document_extraction_{run_id}.json"
    return Response(
        content=json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/extract-runs/{run_id}/structured-json")
def get_extract_structured_json(run_id: str):
    r = _ensure_extraction_complete(run_id)
    return _structured_extraction_json(r, run_id)


@app.get("/runs/{run_id}")
def run_meta(run_id: str):
    r = _ensure_run(run_id)

    return {
        "run_id": run_id,
        "status": r.get("status", "running"),
        "status_message": r.get("status_message", "Working"),
        "progress": r.get("progress", 0),
        "error": r.get("error"),
        "traceback": r.get("traceback"),
        "base_label": r.get("base_label"),
        "target_label": r.get("target_label"),
        "base_format": r.get("base_format"),
        "target_format": r.get("target_format"),
        "supported_upload_formats": supported_input_extensions(),
        "stats": r.get("stats", {}),
        "coverage": r.get("coverage", {}),
        "db_run_id": r.get("db_run_id"),
        "db_error": r.get("db_error"),
        "n_pages_base": len(r.get("base_imgs", [])),
        "n_pages_target": len(r.get("target_imgs", [])),
        "base_native_pages": r.get("base_native_pages") or _max_block_page(r.get("base_blocks", [])),
        "target_native_pages": r.get("target_native_pages") or _max_block_page(r.get("target_blocks", [])),
    }


@app.get("/runs/{run_id}/diff")
def get_diff(
    run_id: str,
    change_type: Optional[str] = None,
    section: Optional[str] = None,
    stable_key: Optional[str] = None,
    limit: int = 200,
):
    r = _ensure_complete(run_id)

    base_by_id = {b.id: b for b in r["base_blocks"]}
    target_by_id = {b.id: b for b in r["target_blocks"]}
    out = []

    for d in r["diffs"]:
        if change_type and d.change_type.value != change_type.upper():
            continue

        b = base_by_id.get(d.base_block_id) if d.base_block_id else None
        t = target_by_id.get(d.target_block_id) if d.target_block_id else None
        block = b or t

        if not block:
            continue
        if section and section.lower() not in (block.path or "").lower():
            continue
        if stable_key and (block.stable_key or "").upper() != stable_key.upper():
            continue

        out.append(
            {
                "change_type": d.change_type.value,
                "stable_key": block.stable_key,
                "block_type": block.block_type.value,
                "path": block.path,
                "page_base": b.page_number if b else None,
                "page_target": t.page_number if t else None,
                "before": b.text if b else None,
                "after": t.text if t else None,
                "field_diffs": [_dump_model(fd) for fd in d.field_diffs],
                "token_diff": [_dump_model(td) for td in d.token_diff],
                "similarity": d.similarity,
                "impact": d.impact_score,
                "bbox_base": b.bbox if b else None,
                "bbox_target": t.bbox if t else None,
            }
        )

        if len(out) >= limit:
            break

    return {"diffs": out, "count": len(out)}


@app.get("/runs/{run_id}/summary")
def get_summary(run_id: str):
    r = _ensure_complete(run_id)
    return {"summary": [_dump_model(s) for s in r["summary"]]}


@app.get("/runs/{run_id}/report.pdf")
def get_report_pdf(run_id: str):
    r = _ensure_complete(run_id)

    try:
        from .report import build_pdf_report
    except Exception as exc:
        raise HTTPException(
            500,
            f"PDF report generation is not available because the report dependency failed to load: {exc}",
        )

    pdf_bytes = build_pdf_report(run_id, r)
    filename = f"document_comparison_report_{run_id}.pdf"

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _ai_pdf_cell(row: dict[str, Any], column: str) -> str:
    if not isinstance(row, dict):
        return ""
    if column in row:
        return "" if row[column] is None else str(row[column])
    wanted = column.strip().lower()
    for key, value in row.items():
        if str(key).strip().lower() == wanted:
            return "" if value is None else str(value)
    return ""


def _ai_pdf_confidence(value: Optional[float]) -> Optional[int]:
    if value is None:
        return None
    try:
        n = float(value)
    except (TypeError, ValueError):
        return None
    if n <= 1:
        n *= 100
    return max(0, min(100, round(n)))


@app.post("/runs/{run_id}/ai-summary.pdf")
def get_ai_summary_pdf(run_id: str, req: AiSummaryPdfReq):
    _ensure_complete(run_id)

    try:
        from html import escape

        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.lib.units import inch
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont
        from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
    except Exception as exc:
        raise HTTPException(
            500,
            f"AI summary PDF generation is not available because the report dependency failed to load: {exc}",
        )

    font_name = "Helvetica"
    for font_path in (
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
    ):
        try:
            if Path(font_path).exists():
                pdfmetrics.registerFont(TTFont("DocuLensUnicode", font_path))
                font_name = "DocuLensUnicode"
                break
        except Exception:
            font_name = "Helvetica"

    page_size = landscape(A4)
    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=page_size,
        leftMargin=0.45 * inch,
        rightMargin=0.45 * inch,
        topMargin=0.45 * inch,
        bottomMargin=0.45 * inch,
        title=req.title or "AI Summary",
    )

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "DocuLensTitle",
        parent=styles["Title"],
        fontName=font_name,
        fontSize=16,
        leading=20,
        textColor=colors.HexColor("#1f2937"),
        spaceAfter=8,
    )
    meta_style = ParagraphStyle(
        "DocuLensMeta",
        parent=styles["BodyText"],
        fontName=font_name,
        fontSize=8,
        leading=10,
        textColor=colors.HexColor("#667085"),
        spaceAfter=8,
    )
    body_style = ParagraphStyle(
        "DocuLensBody",
        parent=styles["BodyText"],
        fontName=font_name,
        fontSize=9,
        leading=12,
        textColor=colors.HexColor("#344054"),
        spaceAfter=8,
    )
    header_style = ParagraphStyle(
        "DocuLensHeader",
        parent=body_style,
        fontName=font_name,
        fontSize=8.5,
        leading=10,
        textColor=colors.white,
    )
    cell_style = ParagraphStyle(
        "DocuLensCell",
        parent=body_style,
        fontName=font_name,
        fontSize=8,
        leading=10,
        textColor=colors.HexColor("#1f2937"),
    )

    story = []
    title = (req.title or "AI Summary").strip() or "AI Summary"
    story.append(Paragraph(escape(title), title_style))

    confidence = _ai_pdf_confidence(req.confidence)
    meta_parts = [f"Run: {run_id}", "Source: extracted comparison evidence"]
    if confidence is not None:
        meta_parts.append(f"Confidence: {confidence}%")
    story.append(Paragraph(escape(" | ".join(meta_parts)), meta_style))

    if req.answer:
        for paragraph in str(req.answer).splitlines():
            if paragraph.strip():
                story.append(Paragraph(escape(paragraph.strip()), body_style))
        story.append(Spacer(1, 8))

    columns = [str(c) for c in (req.columns or []) if str(c).strip()]
    rows = [row for row in (req.rows or []) if isinstance(row, dict)]

    if columns and rows:
        usable_width = page_size[0] - doc.leftMargin - doc.rightMargin
        col_width = usable_width / max(1, len(columns))
        data = [[Paragraph(escape(col), header_style) for col in columns]]

        for row in rows[:200]:
            data.append(
                [
                    Paragraph(escape(_ai_pdf_cell(row, col)).replace("\n", "<br/>"), cell_style)
                    for col in columns
                ]
            )

        table = Table(data, colWidths=[col_width] * len(columns), repeatRows=1)
        table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f2937")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("FONTNAME", (0, 0), (-1, -1), font_name),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#ded6c8")),
                    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#fbfaf6")]),
                    ("LEFTPADDING", (0, 0), (-1, -1), 6),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                    ("TOPPADDING", (0, 0), (-1, -1), 5),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ]
            )
        )
        story.append(table)
    elif not req.answer:
        story.append(Paragraph("No AI summary content was provided for this run.", body_style))

    doc.build(story)
    filename = f"ai_summary_{run_id}.pdf"

    return Response(
        content=buffer.getvalue(),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.post("/runs/{run_id}/query")
def post_query(run_id: str, req: QueryReq):
    r = _ensure_complete(run_id)

    result = nl_query(
        req.question,
        r["diffs"],
        r["base_blocks"],
        r["target_blocks"],
        db_run_id=r.get("db_run_id"),
        mode=req.mode,
        response_language=req.response_language,
    )

    if isinstance(result, dict):
        return result

    return {
        "answer": f"I found {len(result)} matching changes.",
        "view": "evidence",
        "columns": [],
        "rows": result[:200],
        "count": len(result),
        "plan": {},
    }


@app.get("/runs/{run_id}/pages/{side}/{n}")
def get_page(run_id: str, side: str, n: int):
    r = _ensure_complete(run_id)

    if side not in ("base", "target"):
        raise HTTPException(400, "side must be base|target")

    imgs = r["base_imgs"] if side == "base" else r["target_imgs"]
    if n < 1 or n > len(imgs):
        raise HTTPException(404, "page out of range")

    return FileResponse(imgs[n - 1], media_type="image/png")


def _native_change_maps(
    r: dict,
    side: str,
) -> tuple[dict[Any, ChangeType], dict[Any, list[dict[str, Any]]], dict[Any, list[dict[str, Any]]]]:
    change_by_id: dict[Any, ChangeType] = {}
    fields_by_id: dict[Any, list[dict[str, Any]]] = {}
    tokens_by_id: dict[Any, list[dict[str, Any]]] = {}

    for d in r["diffs"]:
        if d.change_type == ChangeType.UNCHANGED:
            continue

        if side == "base":
            if d.change_type == ChangeType.ADDED or not d.base_block_id:
                continue
            block_id = d.base_block_id
        else:
            if d.change_type == ChangeType.DELETED or not d.target_block_id:
                continue
            block_id = d.target_block_id

        change_by_id[block_id] = d.change_type
        fields_by_id[block_id] = [
            {"field": fd.field, "before": fd.before, "after": fd.after}
            for fd in d.field_diffs
        ]
        tokens_by_id[block_id] = [_dump_model(td) for td in d.token_diff]

    return change_by_id, fields_by_id, tokens_by_id


def _native_color(change_type: Optional[ChangeType]) -> str:
    if change_type == ChangeType.ADDED:
        return "added"
    if change_type == ChangeType.DELETED:
        return "deleted"
    if change_type == ChangeType.MODIFIED:
        return "modified"
    return "unchanged"


def _native_block_payload(block: Block) -> dict[str, Any]:
    payload = _safe_payload(block)
    out = {}

    for key, value in payload.items():
        key = str(key)
        if key in {"page_width", "page_height", "anchors", "__anchors__", "__pages__"}:
            continue
        if key.startswith("__") and key not in {"__table_title__", "__table_context__", "__row_index__"}:
            continue
        out[key] = value

    return out


def _native_row_payload(row: Block, fields_by_id: dict[Any, list[dict[str, Any]]], change_by_id: dict[Any, ChangeType]) -> dict[str, Any]:
    values = _row_values(row)
    change_type = change_by_id.get(row.id)
    return {
        "id": str(row.id),
        "type": row.block_type.value,
        "change_type": change_type.value if change_type else "UNCHANGED",
        "highlight": _native_color(change_type),
        "stable_key": row.stable_key,
        "text": row.text,
        "values": values,
        "field_diffs": fields_by_id.get(row.id, []),
        "row_index": _row_payload_index(row),
    }


def _native_viewer_type(fmt: str | None) -> str:
    if fmt == "spreadsheet":
        return "spreadsheet"
    if fmt == "word":
        return "document"
    return "structured"


@app.get("/runs/{run_id}/native-page/{side}/{n}")
def get_native_page(run_id: str, side: str, n: int):
    r = _ensure_complete(run_id)

    if side not in ("base", "target"):
        raise HTTPException(400, "side must be base|target")

    blocks = r["base_blocks"] if side == "base" else r["target_blocks"]
    fmt = r.get("base_format") if side == "base" else r.get("target_format")
    change_by_id, fields_by_id, tokens_by_id = _native_change_maps(r, side)

    table_by_id = {
        b.id: b for b in blocks
        if b.block_type.value == "table"
    }
    rows_by_table_on_page: dict[Any, list[Block]] = defaultdict(list)

    for row in blocks:
        if row.block_type.value != "table_row":
            continue
        if row.page_number == n and row.parent_id in table_by_id:
            rows_by_table_on_page[row.parent_id].append(row)

    table_ids_rendered = {
        b.id for b in blocks
        if b.block_type.value == "table" and (b.page_number == n or b.id in rows_by_table_on_page)
    }

    items = []
    for block in sorted(blocks, key=lambda b: (b.page_number, b.sequence)):
        if block.page_number != n and block.id not in rows_by_table_on_page:
            continue

        if block.block_type.value == "table_row" and block.parent_id in table_ids_rendered:
            continue

        if block.block_type.value == "table_row":
            continue

        change_type = change_by_id.get(block.id)
        item = {
            "id": str(block.id),
            "type": block.block_type.value,
            "path": block.path,
            "text": block.text,
            "stable_key": block.stable_key,
            "change_type": change_type.value if change_type else "UNCHANGED",
            "highlight": _native_color(change_type),
            "payload": _native_block_payload(block),
            "field_diffs": fields_by_id.get(block.id, []),
            "token_diff": tokens_by_id.get(block.id, []),
        }

        if block.block_type.value == "table":
            rows = rows_by_table_on_page.get(block.id) or [
                row for row in _table_rows(block, blocks)
                if row.page_number == n or row.page_number == block.page_number
            ]
            header = _column_names(block, rows)
            item["header"] = header
            item["rows"] = [
                _native_row_payload(row, fields_by_id, change_by_id)
                for row in rows
            ]

        items.append(item)

    rendered_table_ids = {item["id"] for item in items if item.get("type") == "table"}
    missing_table_ids = [
        table_id for table_id in rows_by_table_on_page.keys()
        if str(table_id) not in rendered_table_ids
    ]

    for table_id in missing_table_ids:
        table = table_by_id.get(table_id)
        if not table:
            continue

        rows = rows_by_table_on_page.get(table_id, [])
        change_type = change_by_id.get(table.id)
        header = _column_names(table, rows)
        items.append(
            {
                "id": str(table.id),
                "type": table.block_type.value,
                "path": table.path,
                "text": table.text,
                "stable_key": table.stable_key,
                "change_type": change_type.value if change_type else "UNCHANGED",
                "highlight": _native_color(change_type),
                "payload": _native_block_payload(table),
                "field_diffs": fields_by_id.get(table.id, []),
                "token_diff": tokens_by_id.get(table.id, []),
                "header": header,
                "rows": [
                    _native_row_payload(row, fields_by_id, change_by_id)
                    for row in rows
                ],
            }
        )

    max_native_page = max([b.page_number for b in blocks], default=1)

    return {
        "page": n,
        "side": side,
        "format": fmt,
        "viewer": "native",
        "viewer_type": _native_viewer_type(fmt),
        "max_page": max_native_page,
        "items": items,
    }


def _page_dimensions_for(blocks: list[Block], page_number: int) -> tuple[Optional[float], Optional[float]]:
    for block in blocks:
        if block.page_number != page_number:
            continue
        if not isinstance(block.payload, dict):
            continue

        page_width = block.payload.get("page_width")
        page_height = block.payload.get("page_height")

        if page_width and page_height:
            return page_width, page_height

    return None, None


@app.get("/runs/{run_id}/overlay/{side}/{n}")
def get_overlay(run_id: str, side: str, n: int):
    r = _ensure_complete(run_id)

    if side not in ("base", "target"):
        raise HTTPException(400, "side must be base|target")

    base_by_id = {b.id: b for b in r["base_blocks"]}
    target_by_id = {b.id: b for b in r["target_blocks"]}
    side_blocks = r["base_blocks"] if side == "base" else r["target_blocks"]

    page_width, page_height = _page_dimensions_for(side_blocks, n)

    color_map = {
        "ADDED": "rgba(40,180,40,0.30)",
        "DELETED": "rgba(220,40,40,0.30)",
        "MODIFIED": "rgba(220,200,40,0.30)",
    }

    regions = []

    for d in r["diffs"]:
        if d.change_type == ChangeType.UNCHANGED:
            continue

        if side == "base":
            blk = base_by_id.get(d.base_block_id) if d.base_block_id else None
            if not blk or blk.page_number != n:
                continue
            if d.change_type == ChangeType.ADDED:
                continue
        else:
            blk = target_by_id.get(d.target_block_id) if d.target_block_id else None
            if not blk or blk.page_number != n:
                continue
            if d.change_type == ChangeType.DELETED:
                continue

        if not blk.bbox:
            continue

        has_row_children = any(
            c.parent_id == blk.id and c.block_type.value == "table_row"
            for c in side_blocks
        )
        if blk.block_type.value == "table" and has_row_children:
            continue

        region_page_width = None
        region_page_height = None
        if isinstance(blk.payload, dict):
            region_page_width = blk.payload.get("page_width")
            region_page_height = blk.payload.get("page_height")

        regions.append(
            {
                "bbox": blk.bbox,
                "change_type": d.change_type.value,
                "color": color_map[d.change_type.value],
                "stable_key": blk.stable_key,
                "block_type": blk.block_type.value,
                "page_width": region_page_width or page_width,
                "page_height": region_page_height or page_height,
            }
        )

    return {
        "page": n,
        "side": side,
        "page_width": page_width,
        "page_height": page_height,
        "regions": regions,
    }


# ---------------- table intelligence helpers ----------------

_INTERNAL_TABLE_FIELDS = {
    "__anchors__",
    "__pages__",
    "__row_index__",
    "__table_title__",
    "__table_context__",
    "anchors",
    "page_width",
    "page_height",
}

_ROW_LABEL_HINTS = (
    "feature",
    "description",
    "item",
    "name",
    "order",
    "code",
    "part",
    "pcv",
    "pcb",
    "model",
    "series",
    "equipment",
    "group",
    "package",
    "option",
    "content",
)

_VALUE_COLUMN_HINTS = (
    "pcv",
    "pcb",
    "value",
    "column",
    "package",
    "series",
    "model",
    "price",
    "cost",
    "amount",
    "status",
    "availability",
)


def _norm_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().lower()


def _display_text(value: Any, limit: int = 180) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "..."


def _path_label(path: str | None) -> str:
    if not path:
        return "Document"

    parts = [p for p in path.split("/") if p]
    if not parts:
        return "Document"

    cleaned = []
    for part in parts:
        if part.startswith("table_") or part.startswith("row_"):
            continue
        cleaned.append(part.replace("_", " ").title())

    return " / ".join(cleaned[:4]) if cleaned else "Document"


def _safe_payload(block: Block) -> dict:
    return block.payload if isinstance(block.payload, dict) else {}


def _table_header(block: Block) -> list[str]:
    payload = _safe_payload(block)
    return [str(h or "").strip() for h in payload.get("header", [])]


def _table_rows(table: Block, blocks: list[Block]) -> list[Block]:
    return [
        b for b in blocks
        if b.parent_id == table.id and b.block_type.value == "table_row"
    ]


def _table_payload_rows(table: Block) -> list[list[Any]]:
    payload = _safe_payload(table)
    rows = payload.get("rows")

    if not isinstance(rows, list):
        return []

    normalized = []
    for row in rows:
        if isinstance(row, list):
            normalized.append(row)
        elif isinstance(row, tuple):
            normalized.append(list(row))
        elif row is not None:
            normalized.append([row])

    return normalized


def _row_payload_index(row: Optional[Block]) -> Optional[int]:
    if not row:
        return None

    if isinstance(row.payload, dict):
        idx = row.payload.get("__row_index__")
        if isinstance(idx, int):
            return idx
        if isinstance(idx, str) and idx.isdigit():
            return int(idx)

    m = re.search(r"/row_(\d+)$", row.path or "")
    if m:
        return int(m.group(1))

    return None


def _row_values(row: Optional[Block]) -> dict[str, Any]:
    if not row or not isinstance(row.payload, dict):
        return {}

    out = {}
    for key, value in row.payload.items():
        if key in _INTERNAL_TABLE_FIELDS:
            continue
        if str(key).startswith("__"):
            continue
        out[str(key)] = value

    return out


def _row_values_for_table(table: Optional[Block], row: Optional[Block], columns: Optional[list[str]] = None) -> dict[str, Any]:
    values = _row_values(row)
    if values:
        return values

    if not table or not row:
        return {}

    raw_rows = _table_payload_rows(table)
    row_idx = _row_payload_index(row)

    if row_idx is None or row_idx < 0 or row_idx >= len(raw_rows):
        return {}

    raw = raw_rows[row_idx]
    cols = columns or _column_names(table, _table_rows(table, []))
    if not cols:
        cols = [f"Column {i + 1}" for i in range(len(raw))]

    out = {}
    for idx, col in enumerate(cols):
        out[col] = raw[idx] if idx < len(raw) else ""

    return out


def _is_generic_column_name(name: str) -> bool:
    return bool(re.match(r"^(col|column|value)\s*[_-]?\s*\d+$", str(name or ""), re.I))


def _column_names(table: Block, rows: list[Block]) -> list[str]:
    names = _table_header(table)
    seen = set()
    out = []

    for name in names:
        name = str(name or "").strip()
        if not name:
            continue
        if name not in seen:
            out.append(name)
            seen.add(name)

    for row in rows:
        for key in _row_values(row).keys():
            if key not in seen:
                out.append(key)
                seen.add(key)

    raw_rows = _table_payload_rows(table)
    max_width = max([len(r) for r in raw_rows] + [0])
    for idx in range(max_width):
        name = out[idx] if idx < len(out) and out[idx] else f"Column {idx + 1}"
        if name not in seen:
            out.append(name)
            seen.add(name)

    return out


def _column_quality(columns: list[str]) -> float:
    if not columns:
        return 0.0

    useful = 0
    for col in columns:
        if not _is_generic_column_name(col):
            useful += 1

    return useful / max(1, len(columns))


def _table_title(table: Block, rows: Optional[list[Block]] = None) -> str:
    payload = _safe_payload(table)

    for key in ("table_title", "title", "caption"):
        value = _display_text(payload.get(key), 160)
        if value:
            return value

    near_texts = payload.get("near_texts")
    if isinstance(near_texts, list):
        for text in near_texts:
            value = _display_text(text, 160)
            if value:
                return value

    context = _display_text(payload.get("table_context"), 160)
    if context:
        return context

    path_label = _path_label(table.path)
    if path_label and path_label != "Document":
        return path_label

    rows = rows or []
    columns = _column_names(table, rows)
    useful_columns = [c for c in columns if not _is_generic_column_name(c)]

    if useful_columns:
        return " / ".join(useful_columns[:3])[:160]

    return f"Table on page {table.page_number}"


def _table_context(table: Block) -> str:
    payload = _safe_payload(table)
    context = _display_text(payload.get("table_context"), 260)
    if context:
        return context
    return _path_label(table.path)


def _table_pages(table: Block) -> list[int]:
    payload = _safe_payload(table)
    pages = payload.get("spans_pages")
    if isinstance(pages, list) and pages:
        return [int(p) for p in pages if p]
    return [table.page_number]


def _table_display_name(table: Block, rows: list[Block]) -> str:
    pages = _table_pages(table)
    page_label = f"p{pages[0]}" if len(pages) == 1 else f"p{pages[0]}-{pages[-1]}"
    columns = _column_names(table, rows)
    title = _table_title(table, rows)
    return f"{page_label} - {title} ({len(columns)} columns, {len(rows)} rows)"


def _row_key(row: Optional[Block], row_columns: Optional[list[str]] = None) -> str:
    if not row:
        return ""

    values = _row_values(row)

    if row_columns:
        parts = [_display_text(values.get(col), 120) for col in row_columns if _display_text(values.get(col), 120)]
        if parts:
            return " | ".join(parts)

    if row.stable_key:
        return str(row.stable_key).strip()

    for value in values.values():
        text = _display_text(value, 120)
        if text:
            return text

    return _display_text(row.text, 120)


def _row_key_for_table(table: Optional[Block], row: Optional[Block], row_columns: Optional[list[str]] = None) -> str:
    if not row:
        return ""

    columns = _column_names(table, _table_rows(table, [])) if table else []
    values = _row_values_for_table(table, row, columns)

    if row_columns:
        parts = [_display_text(values.get(col), 120) for col in row_columns if _display_text(values.get(col), 120)]
        if parts:
            return " | ".join(parts)

    if row.stable_key:
        return str(row.stable_key).strip()

    for value in values.values():
        text = _display_text(value, 120)
        if text:
            return text

    return _display_text(row.text, 120)


def _row_definition(row: Optional[Block], row_columns: Optional[list[str]] = None) -> str:
    if not row:
        return ""

    values = _row_values(row)
    parts = []

    source_items = []
    if row_columns:
        source_items.extend((col, values.get(col)) for col in row_columns)
    source_items.extend(values.items())

    seen = set()
    for key, value in source_items:
        if key in seen:
            continue
        seen.add(key)

        v = str(value or "").strip()
        if not v:
            continue

        if _is_generic_column_name(str(key)):
            parts.append(v)
        else:
            parts.append(f"{key}: {v}")

        if len(parts) >= 4:
            break

    if parts:
        return " | ".join(parts)

    return _display_text(row.text, 260)


def _row_definition_for_table(table: Optional[Block], row: Optional[Block], row_columns: Optional[list[str]] = None) -> str:
    if not row:
        return ""

    columns = _column_names(table, _table_rows(table, [])) if table else []
    values = _row_values_for_table(table, row, columns)
    parts = []

    source_items = []
    if row_columns:
        source_items.extend((col, values.get(col)) for col in row_columns)
    source_items.extend(values.items())

    seen = set()
    for key, value in source_items:
        if key in seen:
            continue
        seen.add(key)

        v = str(value or "").strip()
        if not v:
            continue

        if _is_generic_column_name(str(key)):
            parts.append(v)
        else:
            parts.append(f"{key}: {v}")

        if len(parts) >= 4:
            break

    if parts:
        return " | ".join(parts)

    return _display_text(row.text, 260)


def _row_summary(row: Block, index: int, columns: Optional[list[str]] = None, row_columns: Optional[list[str]] = None, table: Optional[Block] = None) -> dict:
    values = _row_values_for_table(table, row, columns)
    selected_values = {col: values.get(col, "") for col in columns} if columns else values

    return {
        "id": str(row.id),
        "row_index": index,
        "stable_key": row.stable_key,
        "row_key": _row_key_for_table(table, row, row_columns),
        "definition": _row_definition_for_table(table, row, row_columns),
        "page": row.page_number,
        "path": row.path,
        "text": _display_text(row.text, 500),
        "values": selected_values,
        "bbox": row.bbox,
    }


def _guess_row_label_columns(columns: list[str], rows: list[Block], table: Optional[Block] = None) -> list[str]:
    if not columns:
        return []

    scored = []

    for idx, col in enumerate(columns):
        col_low = _norm_text(col)
        non_empty = 0
        unique_values = set()
        text_len = 0
        numericish = 0

        for row in rows[:100]:
            value = _display_text(_row_values_for_table(table, row, columns).get(col), 160)
            if value:
                non_empty += 1
                unique_values.add(value.lower())
                text_len += len(value)
                compact = re.sub(r"[\s,$%/().:-]", "", value)
                if compact and sum(ch.isdigit() for ch in compact) >= max(1, sum(ch.isalpha() for ch in compact) * 2):
                    numericish += 1

        uniqueness = len(unique_values) / max(1, non_empty)
        avg_len = text_len / max(1, non_empty)
        numeric_ratio = numericish / max(1, non_empty)
        hint = 1.0 if any(term in col_low for term in _ROW_LABEL_HINTS) else 0.0
        left_bias = max(0.0, 1.0 - (idx / max(1, len(columns))))

        score = (
            hint * 0.40
            + uniqueness * 0.20
            + min(avg_len / 45.0, 1.0) * 0.20
            + left_bias * 0.15
            - numeric_ratio * 0.20
        )
        scored.append((score, col))

    scored.sort(key=lambda x: x[0], reverse=True)
    best = [col for score, col in scored[:1] if score >= 0.25]

    return best or [columns[0]]


def _guess_value_columns(columns: list[str], row_columns: list[str]) -> list[str]:
    candidates = [c for c in columns if c not in row_columns]

    if not candidates:
        return []

    hinted = [c for c in candidates if any(term in _norm_text(c) for term in _VALUE_COLUMN_HINTS)]
    if hinted:
        return hinted

    return candidates


def _column_details(columns: list[str], rows: list[Block], table: Optional[Block] = None) -> list[dict]:
    details = []

    for col in columns:
        non_empty = 0
        samples = []
        distinct = set()

        for row in rows[:120]:
            value = _display_text(_row_values_for_table(table, row, columns).get(col), 120)
            if not value:
                continue

            non_empty += 1
            distinct.add(value.lower())
            if len(samples) < 5 and value not in samples:
                samples.append(value)

        details.append(
            {
                "name": col,
                "is_generic": _is_generic_column_name(col),
                "non_empty": non_empty,
                "distinct_count": len(distinct),
                "sample_values": samples,
            }
        )

    return details


def _table_matrix(table: Block, blocks: list[Block], include_rows: bool = False) -> dict:
    rows = _table_rows(table, blocks)
    columns = _column_names(table, rows)
    row_label_columns = _guess_row_label_columns(columns, rows, table)
    value_columns = _guess_value_columns(columns, row_label_columns)
    pages = _table_pages(table)
    payload = _safe_payload(table)
    header_preview = " | ".join(str(h)[:40] for h in columns[:8])

    matrix = {
        "id": str(table.id),
        "page_first": table.page_number,
        "spans_pages": pages,
        "page_label": f"Page {pages[0]}" if len(pages) == 1 else f"Pages {pages[0]}-{pages[-1]}",
        "path": table.path,
        "title": _table_title(table, rows),
        "context": _table_context(table),
        "area": _path_label(table.path),
        "display_name": _table_display_name(table, rows),
        "n_columns": len(columns),
        "n_rows": len(rows),
        "columns": columns,
        "column_details": _column_details(columns, rows, table),
        "header": columns,
        "header_preview": header_preview,
        "header_source": payload.get("header_sources", [None])[0] if isinstance(payload.get("header_sources"), list) and payload.get("header_sources") else None,
        "header_quality": round(_column_quality(columns), 2),
        "suggested_row_columns": row_label_columns,
        "suggested_value_columns": value_columns,
        "row_keys": [_row_key_for_table(table, r, row_label_columns) for r in rows[:150]],
        "row_preview": [_row_summary(r, i, columns=columns, row_columns=row_label_columns, table=table) for i, r in enumerate(rows[:12])],
        "near_texts": payload.get("near_texts", []),
        "source_tables": payload.get("source_tables", []),
    }

    if include_rows:
        matrix["rows"] = [_row_summary(r, i, columns=columns, row_columns=row_label_columns, table=table) for i, r in enumerate(rows)]

    return matrix


def _find_table_by_id(blocks: list[Block], table_id: str | None) -> Optional[Block]:
    if not table_id:
        return None

    for block in blocks:
        if block.block_type.value == "table" and str(block.id) == str(table_id):
            return block

    return None


def _find_table_by_header(blocks: list[Block], query: str | None) -> Optional[Block]:
    if not query:
        return None

    q = _norm_text(query)
    if not q:
        return None

    best = None
    best_score = 0.0

    for block in blocks:
        if block.block_type.value != "table":
            continue

        rows = _table_rows(block, blocks)
        columns = _column_names(block, rows)
        searchable = " ".join(
            [
                _table_title(block, rows),
                _table_context(block),
                " ".join(columns),
                block.path or "",
            ]
        )

        score = fuzz.partial_ratio(q, _norm_text(searchable)) / 100.0

        if score > best_score:
            best_score = score
            best = block

    return best if best_score >= 0.45 else None


def _resolve_table(blocks: list[Block], table_id: str | None, header_query: str | None) -> Optional[Block]:
    return _find_table_by_id(blocks, table_id) or _find_table_by_header(blocks, header_query)


def _find_row(rows: list[Block], row_key: str | None, row_columns: Optional[list[str]] = None) -> Optional[Block]:
    if not row_key:
        return None

    q = _norm_text(row_key)
    if not q:
        return None

    scored = []

    for row in rows:
        values = _row_values(row)
        key = _norm_text(_row_key(row, row_columns))
        stable = _norm_text(row.stable_key)
        text = _norm_text(row.text)
        values_text = _norm_text(" ".join(str(v or "") for v in values.values()))

        score = max(
            1.0 if q in {key, stable} else 0.0,
            0.94 if q and q in key else 0.0,
            0.88 if q and q in values_text else 0.0,
            fuzz.partial_ratio(q, key) / 100.0,
            fuzz.partial_ratio(q, stable) / 100.0,
            fuzz.partial_ratio(q, text) / 100.0,
            fuzz.partial_ratio(q, values_text) / 100.0,
        )
        scored.append((score, row))

    scored.sort(key=lambda item: item[0], reverse=True)
    if scored and scored[0][0] >= 0.62:
        return scored[0][1]

    return None


def _align_columns(base_cols: list[str], target_cols: list[str]) -> list[dict]:
    """
    Align selected value columns.

    If users intentionally select different names, e.g. baseline PCV 205 and
    revised PCV 203, fuzzy matching alone would call them unrelated. This
    function first matches obvious same-name columns, then pairs remaining
    selected columns by position when that is the only useful comparison.
    """
    used_target = set()
    alignment = []
    unmatched_base = []

    for base_col in base_cols:
        best_col = None
        best_score = 0.0

        for target_col in target_cols:
            if target_col in used_target:
                continue

            score = fuzz.token_set_ratio(_norm_text(base_col), _norm_text(target_col)) / 100.0
            if score > best_score:
                best_score = score
                best_col = target_col

        if best_col is not None and best_score >= 0.72:
            used_target.add(best_col)
            alignment.append(
                {
                    "base_col": base_col,
                    "target_col": best_col,
                    "score": round(best_score, 2),
                    "status": "matched",
                }
            )
        else:
            unmatched_base.append(base_col)

    unmatched_target = [c for c in target_cols if c not in used_target]

    # Intentional custom comparison: pair remaining selected columns by position.
    while unmatched_base and unmatched_target:
        base_col = unmatched_base.pop(0)
        target_col = unmatched_target.pop(0)
        score = fuzz.token_set_ratio(_norm_text(base_col), _norm_text(target_col)) / 100.0
        alignment.append(
            {
                "base_col": base_col,
                "target_col": target_col,
                "score": round(score, 2),
                "status": "selected_pair",
            }
        )

    for base_col in unmatched_base:
        alignment.append(
            {
                "base_col": base_col,
                "target_col": None,
                "score": 0.0,
                "status": "base_only",
            }
        )

    for target_col in unmatched_target:
        alignment.append(
            {
                "base_col": None,
                "target_col": target_col,
                "score": 0.0,
                "status": "target_only",
            }
        )

    return alignment


def _row_match_score(
    base_row: Block,
    target_row: Block,
    base_row_cols: list[str],
    target_row_cols: list[str],
    base_table: Optional[Block] = None,
    target_table: Optional[Block] = None,
) -> float:
    base_key = _norm_text(_row_key_for_table(base_table, base_row, base_row_cols))
    target_key = _norm_text(_row_key_for_table(target_table, target_row, target_row_cols))
    base_text = _norm_text(base_row.text)
    target_text = _norm_text(target_row.text)

    return max(
        1.0 if base_key and base_key == target_key else 0.0,
        fuzz.token_set_ratio(base_key, target_key) / 100.0,
        fuzz.token_set_ratio(base_text, target_text) / 100.0,
    )


def _align_rows(
    base_rows: list[Block],
    target_rows: list[Block],
    base_row_cols: list[str],
    target_row_cols: list[str],
    base_table: Optional[Block] = None,
    target_table: Optional[Block] = None,
) -> list[tuple[Optional[Block], Optional[Block], float]]:
    pairs = []
    used_base = set()
    used_target = set()
    scored = []

    for base_row in base_rows:
        for target_row in target_rows:
            score = _row_match_score(base_row, target_row, base_row_cols, target_row_cols, base_table, target_table)
            if score >= 0.55:
                scored.append((score, base_row, target_row))

    scored.sort(key=lambda x: x[0], reverse=True)

    for score, base_row, target_row in scored:
        if base_row.id in used_base or target_row.id in used_target:
            continue
        pairs.append((base_row, target_row, score))
        used_base.add(base_row.id)
        used_target.add(target_row.id)

    for base_row in base_rows:
        if base_row.id not in used_base:
            pairs.append((base_row, None, 0.0))

    for target_row in target_rows:
        if target_row.id not in used_target:
            pairs.append((None, target_row, 0.0))

    return pairs


def _compare_row_values(
    base_row: Optional[Block],
    target_row: Optional[Block],
    value_alignment: list[dict],
    base_table: Optional[Block] = None,
    target_table: Optional[Block] = None,
) -> list[dict]:
    base_columns = _column_names(base_table, _table_rows(base_table, [])) if base_table else None
    target_columns = _column_names(target_table, _table_rows(target_table, [])) if target_table else None
    base_values = _row_values_for_table(base_table, base_row, base_columns) if base_row else {}
    target_values = _row_values_for_table(target_table, target_row, target_columns) if target_row else {}
    changes = []

    if base_row is None:
        for item in value_alignment:
            col = item.get("target_col")
            if not col:
                continue
            after = target_values.get(col)
            if _norm_text(after):
                changes.append({"field": col, "before": None, "after": after, "change_type": "ADDED"})
        return changes

    if target_row is None:
        for item in value_alignment:
            col = item.get("base_col")
            if not col:
                continue
            before = base_values.get(col)
            if _norm_text(before):
                changes.append({"field": col, "before": before, "after": None, "change_type": "DELETED"})
        return changes

    for item in value_alignment:
        base_col = item.get("base_col")
        target_col = item.get("target_col")

        if base_col and target_col:
            before = base_values.get(base_col)
            after = target_values.get(target_col)
            if _norm_text(before) != _norm_text(after):
                changes.append(
                    {
                        "field": base_col if base_col == target_col else f"{base_col} -> {target_col}",
                        "before": before,
                        "after": after,
                        "change_type": "MODIFIED",
                    }
                )
        elif base_col:
            before = base_values.get(base_col)
            if _norm_text(before):
                changes.append({"field": base_col, "before": before, "after": None, "change_type": "DELETED"})
        elif target_col:
            after = target_values.get(target_col)
            if _norm_text(after):
                changes.append({"field": target_col, "before": None, "after": after, "change_type": "ADDED"})

    return changes


def _table_review_rows(row_results: list[dict], limit: int = 25) -> list[dict]:
    review_rows = []

    for row in row_results[:limit]:
        change_type = str(row.get("change_type") or "MODIFIED").upper()
        row_key = row.get("row_key") if isinstance(row.get("row_key"), dict) else {}
        row_definition = row.get("row_definition") if isinstance(row.get("row_definition"), dict) else {}
        feature = (
            row_key.get("base")
            or row_key.get("target")
            or row_definition.get("base")
            or row_definition.get("target")
            or "Table row"
        )
        field_diffs = row.get("field_diffs") or []

        if change_type == "ADDED":
            change = f"New row/value appears in the revised table: {feature}"
            clarification = "Confirm whether this newly added table entry is expected and applicable."
        elif change_type == "DELETED":
            change = f"Baseline row/value is no longer present in the revised table: {feature}"
            clarification = "Confirm whether this removed table entry is intentionally discontinued or moved."
        elif field_diffs:
            examples = []
            for fd in field_diffs[:3]:
                field = fd.get("field") or "value"
                before = str(fd.get("before") or "-")
                after = str(fd.get("after") or "-")
                examples.append(f"{field}: {before} -> {after}")
            change = "; ".join(examples)
            clarification = "Confirm the selected table value changes with the responsible owner."
        else:
            change = "No selected value change detected."
            clarification = "No clarification required for the selected columns."

        match_score = row.get("match_score")
        review_rows.append(
            {
                "Feature": str(feature),
                "Change": change,
                "Seek Clarification": clarification,
                "Change Type": change_type,
                "Confidence": f"{round(float(match_score) * 100)}%" if match_score is not None else "-",
            }
        )

    return review_rows


def _table_header_insights(value_alignment: list[dict], row_results: list[dict]) -> list[dict[str, Any]]:
    insights = []

    for item in value_alignment:
        base_col = item.get("base_col")
        target_col = item.get("target_col")
        if not base_col or not target_col:
            continue
        if _norm_text(base_col) == _norm_text(target_col):
            continue

        field_name = f"{base_col} -> {target_col}"
        changed_cells = 0
        for row in row_results:
            for fd in row.get("field_diffs") or []:
                if fd.get("field") == field_name or fd.get("field") == base_col:
                    changed_cells += 1

        insights.append(
            {
                "Baseline Header": base_col,
                "Revised Header": target_col,
                "Header Match": f"{round(float(item.get('score') or 0) * 100)}%",
                "Observation": (
                    f"Header changed from '{base_col}' to '{target_col}' and {changed_cells} selected row value(s) also changed."
                    if changed_cells
                    else f"Header changed from '{base_col}' to '{target_col}', while selected row values appear unchanged in the compared slice."
                ),
                "Seek Clarification": "Confirm whether this is only a label/header rename or a business meaning change.",
            }
        )

    for item in value_alignment:
        if item.get("status") == "base_only" and item.get("base_col"):
            insights.append(
                {
                    "Baseline Header": item.get("base_col"),
                    "Revised Header": "-",
                    "Header Match": "0%",
                    "Observation": f"Selected baseline column '{item.get('base_col')}' has no revised counterpart in the selected table slice.",
                    "Seek Clarification": "Confirm whether the column was removed, moved, renamed, or excluded from the revised template.",
                }
            )
        elif item.get("status") == "target_only" and item.get("target_col"):
            insights.append(
                {
                    "Baseline Header": "-",
                    "Revised Header": item.get("target_col"),
                    "Header Match": "0%",
                    "Observation": f"Selected revised column '{item.get('target_col')}' has no baseline counterpart in the selected table slice.",
                    "Seek Clarification": "Confirm whether this is a newly introduced value/attribute or a renamed baseline column.",
                }
            )

    return insights


def _compact_table_rows_for_ai(row_results: list[dict], limit: int = 80) -> list[dict[str, Any]]:
    rows = []

    for row in row_results[:limit]:
        rows.append(
            {
                "change_type": row.get("change_type"),
                "match_score": row.get("match_score"),
                "row_key": row.get("row_key"),
                "row_definition": row.get("row_definition"),
                "field_diffs": row.get("field_diffs", [])[:12],
                "base_values": row.get("base_row", {}).get("values") if isinstance(row.get("base_row"), dict) else row.get("base_values"),
                "target_values": row.get("target_row", {}).get("values") if isinstance(row.get("target_row"), dict) else row.get("target_values"),
            }
        )

    return rows


def _ai_selected_table_review(
    *,
    question: str,
    base_table: dict[str, Any],
    target_table: dict[str, Any],
    base_row_columns: list[str],
    target_row_columns: list[str],
    base_value_columns: list[str],
    target_value_columns: list[str],
    value_alignment: list[dict],
    counts: dict[str, int],
    row_results: list[dict],
    header_insights: list[dict[str, Any]],
) -> dict[str, Any]:
    endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    api_key = os.getenv("AZURE_OPENAI_API_KEY")
    deployment = (
        os.getenv("AZURE_OPENAI_DEPLOYMENT")
        or os.getenv("AZURE_OPENAI_CHAT_DEPLOYMENT")
        or os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")
        or os.getenv("AZURE_OPENAI_MODEL")
    )

    if not (endpoint and api_key and deployment):
        return {
            "available": False,
            "error": "Azure OpenAI is not configured.",
        }

    prompt_payload = {
        "question": question or "Review the selected table comparison and summarize meaningful changes.",
        "base_table": {
            "name": base_table.get("display_name"),
            "page": base_table.get("page_label"),
            "area": base_table.get("area"),
            "columns": base_table.get("columns"),
        },
        "target_table": {
            "name": target_table.get("display_name"),
            "page": target_table.get("page_label"),
            "area": target_table.get("area"),
            "columns": target_table.get("columns"),
        },
        "selected_columns": {
            "base_row_columns": base_row_columns,
            "target_row_columns": target_row_columns,
            "base_value_columns": base_value_columns,
            "target_value_columns": target_value_columns,
        },
        "column_alignment": value_alignment,
        "header_insights": header_insights,
        "counts": counts,
        "changed_rows": _compact_table_rows_for_ai(row_results),
    }

    try:
        from openai import AzureOpenAI

        client = AzureOpenAI(
            api_key=api_key,
            azure_endpoint=endpoint,
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-08-01-preview"),
        )
        response = client.chat.completions.create(
            model=deployment,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are DocuLens table reviewer. Use only the selected table evidence. "
                        "Do not invent rows, columns, values, or business meaning. "
                        "If headers changed but values stayed the same, say that explicitly. "
                        "Return strict JSON only with keys answer, columns, rows, confidence. "
                        "Rows should be useful business review rows."
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps(prompt_payload, ensure_ascii=False, default=str),
                },
            ],
            temperature=0.1,
            max_tokens=2200,
            response_format={"type": "json_object"},
        )
        data = json.loads(response.choices[0].message.content or "{}")
        if not isinstance(data, dict):
            raise ValueError("AI returned a non-object response")
        return {
            "available": True,
            "answer": str(data.get("answer") or "").strip(),
            "columns": [str(c) for c in data.get("columns", [])] if isinstance(data.get("columns"), list) else [],
            "rows": data.get("rows", []) if isinstance(data.get("rows"), list) else [],
            "confidence": data.get("confidence"),
        }
    except Exception as exc:
        return {
            "available": False,
            "error": f"Azure OpenAI selected-table review failed: {type(exc).__name__}: {exc}",
        }


def _row_matches_filter(row: Block, row_columns: list[str], row_filter: Optional[str], table: Optional[Block] = None, columns: Optional[list[str]] = None) -> bool:
    if not row_filter:
        return True

    q = _norm_text(row_filter)
    values = _row_values_for_table(table, row, columns)
    searchable = " ".join(
        [
            _row_key_for_table(table, row, row_columns),
            row.text or "",
            " ".join(str(v or "") for v in values.values()),
        ]
    )

    text = _norm_text(searchable)
    if q in text:
        return True

    return fuzz.partial_ratio(q, text) / 100.0 >= 0.55


def _table_view_payload(
    table: Block,
    blocks: list[Block],
    columns: Optional[list[str]] = None,
    row_filter: Optional[str] = None,
    limit: int = 300,
) -> dict:
    rows = _table_rows(table, blocks)
    all_columns = _column_names(table, rows)
    row_columns = _guess_row_label_columns(all_columns, rows, table)

    if columns:
        selected_columns = [c for c in columns if c in all_columns]
    else:
        selected_columns = all_columns

    filtered_rows = [
        row for row in rows
        if _row_matches_filter(row, row_columns, row_filter, table, all_columns)
    ]

    output_rows = []
    for idx, row in enumerate(filtered_rows[: max(1, min(limit, 1000))]):
        values = _row_values_for_table(table, row, all_columns)
        output_rows.append(
            {
                "row_index": idx,
                "row_key": _row_key_for_table(table, row, row_columns),
                "definition": _row_definition_for_table(table, row, row_columns),
                "page": row.page_number,
                "values": {col: values.get(col, "") for col in selected_columns},
            }
        )

    matrix = _table_matrix(table, blocks, include_rows=False)

    return {
        "view": "table",
        "table": matrix,
        "title": matrix["display_name"],
        "columns": selected_columns,
        "row_columns": row_columns,
        "rows": output_rows,
        "count": len(output_rows),
        "total_rows": len(filtered_rows),
        "row_filter": row_filter,
    }


# ---------------- table endpoints ----------------

@app.get("/runs/{run_id}/tables")
def list_tables(run_id: str, include_rows: bool = False):
    r = _ensure_complete(run_id)

    def _summarize(blocks):
        out = []
        for block in blocks:
            if block.block_type.value != "table":
                continue
            out.append(_table_matrix(block, blocks, include_rows=include_rows))
        return out

    return {
        "base": _summarize(r["base_blocks"]),
        "target": _summarize(r["target_blocks"]),
    }


@app.post("/runs/{run_id}/table-view")
def table_view(run_id: str, req: TableViewReq):
    r = _ensure_complete(run_id)

    if req.side not in ("base", "target"):
        raise HTTPException(400, "side must be base or target")

    blocks = r["base_blocks"] if req.side == "base" else r["target_blocks"]
    table = _find_table_by_id(blocks, req.table_id)

    if not table:
        raise HTTPException(404, "Selected table could not be found. Re-run the comparison and select a table from the current result.")

    return _table_view_payload(
        table,
        blocks,
        columns=req.columns,
        row_filter=req.row_filter,
        limit=req.limit,
    )


@app.post("/runs/{run_id}/compare-table-columns")
def compare_table_columns(run_id: str, req: CompareTableColumnsReq):
    r = _ensure_complete(run_id)

    base_table = _find_table_by_id(r["base_blocks"], req.base_table_id)
    target_table = _find_table_by_id(r["target_blocks"], req.target_table_id)

    if not base_table or not target_table:
        raise HTTPException(
            404,
            {
                "message": "Selected table could not be found. Re-run the comparison and select tables from the current result.",
                "base_found": bool(base_table),
                "target_found": bool(target_table),
            },
        )

    base_rows = _table_rows(base_table, r["base_blocks"])
    target_rows = _table_rows(target_table, r["target_blocks"])

    base_columns = _column_names(base_table, base_rows)
    target_columns = _column_names(target_table, target_rows)

    base_row_columns = req.base_row_columns or _guess_row_label_columns(base_columns, base_rows, base_table)
    target_row_columns = req.target_row_columns or _guess_row_label_columns(target_columns, target_rows, target_table)

    base_value_columns = req.base_value_columns or [c for c in base_columns if c not in base_row_columns]
    target_value_columns = req.target_value_columns or [c for c in target_columns if c not in target_row_columns]

    base_row_columns = list(dict.fromkeys(base_row_columns))
    target_row_columns = list(dict.fromkeys(target_row_columns))
    base_value_columns = [c for c in dict.fromkeys(base_value_columns) if c not in base_row_columns]
    target_value_columns = [c for c in dict.fromkeys(target_value_columns) if c not in target_row_columns]

    invalid_base = [c for c in base_row_columns + base_value_columns if c not in base_columns]
    invalid_target = [c for c in target_row_columns + target_value_columns if c not in target_columns]

    if invalid_base or invalid_target:
        raise HTTPException(
            400,
            {
                "message": "One or more selected columns were not found in the selected tables.",
                "invalid_base_columns": invalid_base,
                "invalid_target_columns": invalid_target,
                "base_columns": base_columns,
                "target_columns": target_columns,
            },
        )

    base_rows = [row for row in base_rows if _row_matches_filter(row, base_row_columns, req.row_filter, base_table, base_columns)]
    target_rows = [row for row in target_rows if _row_matches_filter(row, target_row_columns, req.row_filter, target_table, target_columns)]

    value_alignment = _align_columns(base_value_columns, target_value_columns)

    row_results = []
    counts = {"ADDED": 0, "DELETED": 0, "MODIFIED": 0, "UNCHANGED": 0}

    for base_row, target_row, match_score in _align_rows(base_rows, target_rows, base_row_columns, target_row_columns, base_table, target_table):
        field_diffs = _compare_row_values(base_row, target_row, value_alignment, base_table, target_table)

        if base_row is None and target_row is not None:
            change_type = "ADDED"
        elif target_row is None and base_row is not None:
            change_type = "DELETED"
        elif field_diffs:
            change_type = "MODIFIED"
        else:
            change_type = "UNCHANGED"

        counts[change_type] += 1

        if change_type == "UNCHANGED":
            continue

        selected_base_columns = base_row_columns + base_value_columns
        selected_target_columns = target_row_columns + target_value_columns

        row_results.append(
            {
                "change_type": change_type,
                "match_score": round(match_score, 2),
                "row_key": {
                    "base": _row_key_for_table(base_table, base_row, base_row_columns) if base_row else None,
                    "target": _row_key_for_table(target_table, target_row, target_row_columns) if target_row else None,
                },
                "row_definition": {
                    "base": _row_definition_for_table(base_table, base_row, base_row_columns) if base_row else None,
                    "target": _row_definition_for_table(target_table, target_row, target_row_columns) if target_row else None,
                },
                "base_row": _row_summary(base_row, 0, selected_base_columns, base_row_columns, base_table) if base_row else None,
                "target_row": _row_summary(target_row, 0, selected_target_columns, target_row_columns, target_table) if target_row else None,
                "base_values": _row_values_for_table(base_table, base_row, base_columns) if base_row else {},
                "target_values": _row_values_for_table(target_table, target_row, target_columns) if target_row else {},
                "field_diffs": field_diffs,
            }
        )

        if len(row_results) >= max(1, min(req.limit, 1000)):
            break

    review_rows = _table_review_rows(row_results)
    header_insights = _table_header_insights(value_alignment, row_results)
    base_table_matrix = _table_matrix(base_table, r["base_blocks"], include_rows=False)
    target_table_matrix = _table_matrix(target_table, r["target_blocks"], include_rows=False)
    ai_review = None

    if req.use_ai:
        ai_review = _ai_selected_table_review(
            question=req.question or "Review this selected table comparison. Highlight changed values, unchanged values with changed headers, and clarification questions.",
            base_table=base_table_matrix,
            target_table=target_table_matrix,
            base_row_columns=base_row_columns,
            target_row_columns=target_row_columns,
            base_value_columns=base_value_columns,
            target_value_columns=target_value_columns,
            value_alignment=value_alignment,
            counts=counts,
            row_results=row_results,
            header_insights=header_insights,
        )

    return {
        "view": "table_comparison",
        "answer": (
            f"Compared {len(base_rows)} baseline row(s) with {len(target_rows)} revised row(s). "
            f"Found {counts['ADDED']} added, {counts['DELETED']} deleted, and {counts['MODIFIED']} modified row(s)."
        ),
        "review_summary": (
            f"Selected table slice review: {counts['ADDED']} added, {counts['DELETED']} deleted, "
            f"{counts['MODIFIED']} modified, and {counts['UNCHANGED']} unchanged aligned row(s). "
            "Use the review rows below to confirm business impact with the responsible owner."
        ),
        "review_columns": ["Feature", "Change", "Seek Clarification", "Change Type", "Confidence"],
        "review_rows": review_rows,
        "header_insight_columns": ["Baseline Header", "Revised Header", "Header Match", "Observation", "Seek Clarification"],
        "header_insights": header_insights,
        "ai_review": ai_review,
        "base_table": base_table_matrix,
        "target_table": target_table_matrix,
        "base_preview": _table_view_payload(base_table, r["base_blocks"], base_row_columns + base_value_columns, req.row_filter, limit=30),
        "target_preview": _table_view_payload(target_table, r["target_blocks"], target_row_columns + target_value_columns, req.row_filter, limit=30),
        "base_row_columns": base_row_columns,
        "target_row_columns": target_row_columns,
        "base_value_columns": base_value_columns,
        "target_value_columns": target_value_columns,
        "value_column_alignment": value_alignment,
        "counts": counts,
        "rows": row_results,
        "row_diffs": row_results,
    }


@app.post("/runs/{run_id}/table-report.pdf")
def table_report_pdf(run_id: str, req: CompareTableColumnsReq):
    try:
        from html import escape

        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.lib.units import inch
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont
        from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
    except Exception as exc:
        raise HTTPException(500, f"Table PDF generation is not available: {exc}")

    result = compare_table_columns(run_id, req)

    font_name = "Helvetica"
    for font_path in (
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
    ):
        try:
            if Path(font_path).exists():
                pdfmetrics.registerFont(TTFont("DocuLensTableUnicode", font_path))
                font_name = "DocuLensTableUnicode"
                break
        except Exception:
            font_name = "Helvetica"

    def _cell(value: Any, limit: int = 500) -> str:
        if value is None:
            return "-"
        if isinstance(value, (dict, list)):
            text = json.dumps(value, ensure_ascii=False, default=str)
        else:
            text = str(value)
        text = re.sub(r"\s+", " ", text).strip()
        return text[:limit] + "..." if len(text) > limit else text

    page_size = landscape(A4)
    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=page_size,
        leftMargin=0.42 * inch,
        rightMargin=0.42 * inch,
        topMargin=0.42 * inch,
        bottomMargin=0.42 * inch,
        title=f"Table comparison - {run_id}",
    )

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle("TableReportTitle", parent=styles["Title"], fontName=font_name, fontSize=15, leading=18, textColor=colors.HexColor("#1f2937"))
    meta_style = ParagraphStyle("TableReportMeta", parent=styles["BodyText"], fontName=font_name, fontSize=8, leading=10, textColor=colors.HexColor("#667085"))
    body_style = ParagraphStyle("TableReportBody", parent=styles["BodyText"], fontName=font_name, fontSize=8.2, leading=10.5, textColor=colors.HexColor("#344054"))
    header_style = ParagraphStyle("TableReportHeader", parent=body_style, fontName=font_name, textColor=colors.white)

    story = [
        Paragraph("Selected Table Comparison Report", title_style),
        Paragraph(escape(f"Run ID: {run_id}"), meta_style),
        Spacer(1, 6),
        Paragraph(escape(result.get("answer") or "Selected table comparison"), body_style),
        Paragraph(escape(result.get("review_summary") or ""), body_style),
        Spacer(1, 8),
    ]

    def _add_table(title: str, columns: list[str], rows: list[dict], max_rows: int = 80):
        story.append(Paragraph(escape(title), body_style))
        if not columns or not rows:
            story.append(Paragraph("No rows available for this selection.", body_style))
            story.append(Spacer(1, 6))
            return

        usable_width = page_size[0] - doc.leftMargin - doc.rightMargin
        col_width = usable_width / max(1, len(columns))
        data = [[Paragraph(escape(str(col)), header_style) for col in columns]]
        for row in rows[:max_rows]:
            data.append([Paragraph(escape(_cell(row.get(col))).replace("\n", "<br/>"), body_style) for col in columns])

        table = Table(data, colWidths=[col_width] * len(columns), repeatRows=1)
        table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f2937")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("FONTNAME", (0, 0), (-1, -1), font_name),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#ded6c8")),
                    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#fbfaf6")]),
                    ("LEFTPADDING", (0, 0), (-1, -1), 5),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                    ("TOPPADDING", (0, 0), (-1, -1), 4),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ]
            )
        )
        story.append(table)
        story.append(Spacer(1, 9))

    _add_table("Review and Clarification Summary", result.get("review_columns") or [], result.get("review_rows") or [], max_rows=60)

    diff_rows = []
    for row in (result.get("rows") or [])[:100]:
        row_key = row.get("row_key") if isinstance(row.get("row_key"), dict) else {}
        diff_rows.append(
            {
                "Change Type": row.get("change_type"),
                "Baseline Row": row_key.get("base"),
                "Revised Row": row_key.get("target"),
                "Changed Values": "; ".join(
                    f"{fd.get('field')}: {fd.get('before') or '-'} -> {fd.get('after') or '-'}"
                    for fd in (row.get("field_diffs") or [])[:4]
                ) or "-",
                "Match": f"{round(float(row.get('match_score') or 0) * 100)}%",
            }
        )
    _add_table("Compared Row Changes", ["Change Type", "Baseline Row", "Revised Row", "Changed Values", "Match"], diff_rows, max_rows=100)

    doc.build(story)
    filename = f"table_comparison_{run_id}.pdf"
    return Response(
        content=buffer.getvalue(),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.post("/runs/{run_id}/compare-tables")
def compare_tables_endpoint(run_id: str, req: CompareTablesReq):
    r = _ensure_complete(run_id)

    base_table = _resolve_table(r["base_blocks"], req.base_table_id, req.base_header_query)
    target_table = _resolve_table(r["target_blocks"], req.target_table_id, req.target_header_query)

    if not base_table or not target_table:
        raise HTTPException(
            404,
            {
                "message": "Selected table could not be found.",
                "base_found": bool(base_table),
                "target_found": bool(target_table),
                "hint": "Use table IDs from GET /runs/{run_id}/tables, or provide header/title text that appears near the table.",
            },
        )

    base_rows = _table_rows(base_table, r["base_blocks"])
    target_rows = _table_rows(target_table, r["target_blocks"])

    base_columns = _column_names(base_table, base_rows)
    target_columns = _column_names(target_table, target_rows)
    base_row_columns = _guess_row_label_columns(base_columns, base_rows, base_table)
    target_row_columns = _guess_row_label_columns(target_columns, target_rows, target_table)

    if req.base_row_key or req.target_row_key:
        base_row = _find_row(base_rows, req.base_row_key or req.target_row_key, base_row_columns)
        target_row = _find_row(target_rows, req.target_row_key or req.base_row_key, target_row_columns)

        if not base_row or not target_row:
            raise HTTPException(
                404,
                {
                    "message": "Selected row could not be found in one or both tables.",
                    "base_row_found": bool(base_row),
                    "target_row_found": bool(target_row),
                    "base_table": _table_matrix(base_table, r["base_blocks"]),
                    "target_table": _table_matrix(target_table, r["target_blocks"]),
                },
            )

        value_alignment = _align_columns(
            [c for c in base_columns if c not in base_row_columns],
            [c for c in target_columns if c not in target_row_columns],
        )
        field_diffs = _compare_row_values(base_row, target_row, value_alignment, base_table, target_table)

        return {
            "mode": "selected_rows",
            "base_table": _table_matrix(base_table, r["base_blocks"]),
            "target_table": _table_matrix(target_table, r["target_blocks"]),
            "base_header": base_columns,
            "target_header": target_columns,
            "header_alignment": value_alignment,
            "counts": {"ADDED": 0, "DELETED": 0, "MODIFIED": 1 if field_diffs else 0, "UNCHANGED": 0 if field_diffs else 1},
            "row_diffs": [
                {
                    "change_type": "MODIFIED" if field_diffs else "UNCHANGED",
                    "key": _row_key_for_table(base_table, base_row, base_row_columns),
                    "match_score": 1.0,
                    "base_row": _row_summary(base_row, 0, base_columns, base_row_columns, base_table),
                    "target_row": _row_summary(target_row, 0, target_columns, target_row_columns, target_table),
                    "definition": _row_definition_for_table(base_table, base_row, base_row_columns),
                    "field_diffs": field_diffs,
                }
            ] if field_diffs else [],
        }

    # Backward-compatible all-row compare using guessed label columns and all value columns.
    req2 = CompareTableColumnsReq(
        base_table_id=str(base_table.id),
        target_table_id=str(target_table.id),
        base_row_columns=base_row_columns,
        target_row_columns=target_row_columns,
        base_value_columns=[c for c in base_columns if c not in base_row_columns],
        target_value_columns=[c for c in target_columns if c not in target_row_columns],
    )
    return compare_table_columns(run_id, req2)
