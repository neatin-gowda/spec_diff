"""
FastAPI app — orchestrates upload, extraction, diff, reports, and queries.

Endpoints:
  POST /compare                         upload two PDFs, run the full pipeline
  GET  /runs/{run_id}                   status + stats
  GET  /runs/{run_id}/diff              list of block diffs
  GET  /runs/{run_id}/summary           review summary rows
  POST /runs/{run_id}/query             NL query against the diff
  GET  /runs/{run_id}/report.pdf        downloadable PDF report
  GET  /runs/{run_id}/pages/{side}/{n}  rendered page image
  GET  /runs/{run_id}/overlay/{side}/{n} overlay regions for a page
"""
from __future__ import annotations

import os
import shutil
import tempfile
import uuid
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, HTTPException, UploadFile, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response
from pydantic import BaseModel

from .differ_v2 import diff_blocks, diff_stats
from .extractor_v2 import coverage_pct, extract_blocks_v2 as extract_blocks, render_pages
from .models import (
    Block,
    BlockDiff,
    ChangeType,
)
from .query import query as nl_query
from .report import build_pdf_report
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
    stats: dict[str, int]
    coverage: dict[str, float]


@app.post("/compare", response_model=CompareResponse)
async def compare(
    base: UploadFile = File(..., description="Older / previous version PDF"),
    target: UploadFile = File(..., description="Newer / current version PDF"),
    use_llm: bool = Form(False),
):
    if not base.filename or not target.filename:
        raise HTTPException(400, "Both files required")

    run_id = str(uuid.uuid4())
    work = Path(tempfile.mkdtemp(prefix=f"specdiff_{run_id}_"))
    base_pdf = work / "base.pdf"
    target_pdf = work / "target.pdf"

    _RUNS[run_id] = {
        "status": "running",
        "status_message": "Preparing uploaded documents",
        "progress": 5,
    }

    with base_pdf.open("wb") as f:
        shutil.copyfileobj(base.file, f)
    with target_pdf.open("wb") as f:
        shutil.copyfileobj(target.file, f)

    _RUNS[run_id].update({
        "status_message": "Reading pages",
        "progress": 15,
    })

    base_imgs = render_pages(str(base_pdf), str(work / "pages_base"))
    target_imgs = render_pages(str(target_pdf), str(work / "pages_target"))

    _RUNS[run_id].update({
        "status_message": "Finding sections and tables",
        "progress": 35,
    })

    base_blocks = extract_blocks(str(base_pdf))
    target_blocks = extract_blocks(str(target_pdf))

    cov_b = coverage_pct(str(base_pdf), base_blocks)
    cov_t = coverage_pct(str(target_pdf), target_blocks)

    _RUNS[run_id].update({
        "status_message": "Comparing changes",
        "progress": 60,
    })

    diffs = diff_blocks(base_blocks, target_blocks)
    stats = diff_stats(diffs)

    _RUNS[run_id].update({
        "status_message": "Preparing review summary",
        "progress": 78,
    })

    summary = summarize(diffs, base_blocks, target_blocks, use_llm=use_llm)

    _RUNS[run_id] = {
        "status": "complete",
        "status_message": "Comparison complete",
        "progress": 100,
        "work": work,
        "base_pdf": base_pdf,
        "target_pdf": target_pdf,
        "base_label": Path(base.filename).stem,
        "target_label": Path(target.filename).stem,
        "base_imgs": base_imgs,
        "target_imgs": target_imgs,
        "base_blocks": base_blocks,
        "target_blocks": target_blocks,
        "diffs": diffs,
        "stats": stats,
        "summary": summary,
        "coverage": {"base": cov_b, "target": cov_t},
    }

    return CompareResponse(
        run_id=run_id,
        stats=stats,
        coverage={"base": cov_b, "target": cov_t},
    )


@app.get("/runs/{run_id}")
def run_meta(run_id: str):
    r = _RUNS.get(run_id)
    if not r:
        raise HTTPException(404, "no such run")

    return {
        "run_id": run_id,
        "status": r.get("status", "complete"),
        "status_message": r.get("status_message", "Comparison complete"),
        "progress": r.get("progress", 100),
        "base_label": r.get("base_label"),
        "target_label": r.get("target_label"),
        "stats": r.get("stats", {}),
        "coverage": r.get("coverage", {}),
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
    r = _RUNS.get(run_id)
    if not r:
        raise HTTPException(404, "no such run")
    if r.get("status") != "complete":
        raise HTTPException(409, r.get("status_message", "Comparison is still running"))

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

        out.append({
            "change_type": d.change_type.value,
            "stable_key": block.stable_key,
            "block_type": block.block_type.value,
            "path": block.path,
            "page_base": b.page_number if b else None,
            "page_target": t.page_number if t else None,
            "before": b.text if b else None,
            "after": t.text if t else None,
            "field_diffs": [fd.dict() for fd in d.field_diffs],
            "token_diff": [td.dict() for td in d.token_diff],
            "similarity": d.similarity,
            "impact": d.impact_score,
            "bbox_base": b.bbox if b else None,
            "bbox_target": t.bbox if t else None,
        })

        if len(out) >= limit:
            break

    return {"diffs": out, "count": len(out)}


@app.get("/runs/{run_id}/summary")
def get_summary(run_id: str):
    r = _RUNS.get(run_id)
    if not r:
        raise HTTPException(404, "no such run")
    if r.get("status") != "complete":
        raise HTTPException(409, r.get("status_message", "Comparison is still running"))

    return {"summary": [s.dict() for s in r["summary"]]}


@app.get("/runs/{run_id}/report.pdf")
def get_report_pdf(run_id: str):
    r = _RUNS.get(run_id)
    if not r:
        raise HTTPException(404, "no such run")
    if r.get("status") != "complete":
        raise HTTPException(409, r.get("status_message", "Comparison is still running"))

    pdf_bytes = build_pdf_report(run_id, r)
    filename = f"spec_diff_report_{run_id}.pdf"

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"'
        },
    )


class QueryReq(BaseModel):
    question: str


@app.post("/runs/{run_id}/query")
def post_query(run_id: str, req: QueryReq):
    r = _RUNS.get(run_id)
    if not r:
        raise HTTPException(404, "no such run")
    if r.get("status") != "complete":
        raise HTTPException(409, r.get("status_message", "Comparison is still running"))

    result = nl_query(req.question, r["diffs"], r["base_blocks"], r["target_blocks"])

    # query.py now returns a dict. Keep backward safety if older query.py returns a list.
    if isinstance(result, dict):
        return result

    return {
        "answer": f"I found {len(result)} matching changes.",
        "rows": result[:200],
        "count": len(result),
        "plan": {},
    }


@app.get("/runs/{run_id}/pages/{side}/{n}")
def get_page(run_id: str, side: str, n: int):
    r = _RUNS.get(run_id)
    if not r:
        raise HTTPException(404, "no such run")
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
    r = _RUNS.get(run_id)
    if not r:
        raise HTTPException(404, "no such run")
    if r.get("status") != "complete":
        raise HTTPException(409, r.get("status_message", "Comparison is still running"))
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

        regions.append({
            "bbox": blk.bbox,
            "change_type": d.change_type.value,
            "color": color_map[d.change_type.value],
            "stable_key": blk.stable_key,
            "block_type": blk.block_type.value,
            "page_width": region_page_width or page_width,
            "page_height": region_page_height or page_height,
        })

    return {
        "page": n,
        "side": side,
        "page_width": page_width,
        "page_height": page_height,
        "regions": regions,
    }


@app.get("/")
def root():
    return {"status": "ok", "name": "spec-diff", "endpoints": [
        "POST /compare",
        "GET /runs/{id}",
        "GET /runs/{id}/diff",
        "GET /runs/{id}/summary",
        "GET /runs/{id}/report.pdf",
        "POST /runs/{id}/query",
        "GET /runs/{id}/pages/{side}/{n}",
        "GET /runs/{id}/overlay/{side}/{n}",
        "POST /runs/{id}/compare-tables",
        "GET /runs/{id}/tables",
    ]}


class CompareTablesReq(BaseModel):
    base_header_query: str
    target_header_query: Optional[str] = None


@app.post("/runs/{run_id}/compare-tables")
def compare_tables_endpoint(run_id: str, req: CompareTablesReq):
    r = _RUNS.get(run_id)
    if not r:
        raise HTTPException(404, "no such run")
    if r.get("status") != "complete":
        raise HTTPException(409, r.get("status_message", "Comparison is still running"))

    from .differ_v2 import compare_table_headers

    return compare_table_headers(
        r["base_blocks"],
        r["target_blocks"],
        req.base_header_query,
        req.target_header_query,
    )


@app.get("/runs/{run_id}/tables")
def list_tables(run_id: str):
    r = _RUNS.get(run_id)
    if not r:
        raise HTTPException(404, "no such run")
    if r.get("status") != "complete":
        raise HTTPException(409, r.get("status_message", "Comparison is still running"))

    def _summarize(blocks):
        out = []
        for b in blocks:
            if b.block_type.value != "table":
                continue

            header = b.payload.get("header", []) if isinstance(b.payload, dict) else []
            spans = b.payload.get("spans_pages", [b.page_number]) if isinstance(b.payload, dict) else [b.page_number]
            n_rows = sum(
                1 for c in blocks
                if c.parent_id == b.id and c.block_type.value == "table_row"
            )
            preview = " | ".join(str(h)[:40] for h in header[:6])

            out.append({
                "id": str(b.id),
                "page_first": b.page_number,
                "spans_pages": spans,
                "n_columns": len(header),
                "n_rows": n_rows,
                "header_preview": preview,
            })

        return out

    return {
        "base": _summarize(r["base_blocks"]),
        "target": _summarize(r["target_blocks"]),
    }
