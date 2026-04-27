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
import shutil
import tempfile
import threading
import traceback
import uuid
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


def _dump_model(obj):
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    if hasattr(obj, "dict"):
        return obj.dict()
    return obj


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
            "GET /runs/{id}/overlay/{side}/{n}",
            "GET /runs/{id}/tables",
            "POST /runs/{id}/table-view",
            "POST /runs/{id}/compare-tables",
            "POST /runs/{id}/compare-table-columns",
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

    return {
        "view": "table_comparison",
        "answer": (
            f"Compared {len(base_rows)} baseline row(s) with {len(target_rows)} revised row(s). "
            f"Found {counts['ADDED']} added, {counts['DELETED']} deleted, and {counts['MODIFIED']} modified row(s)."
        ),
        "base_table": _table_matrix(base_table, r["base_blocks"], include_rows=False),
        "target_table": _table_matrix(target_table, r["target_blocks"], include_rows=False),
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
