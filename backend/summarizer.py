"""
Summarizer — turns raw block-level diffs into a generic user-facing
review report.

The output remains compatible with the original:
  Feature | Change | Seek Clarification

But now each row also carries generic fields:
  area, item, change_type, category, impact, confidence, before, after,
  citation, page_base, page_target, stable_key, block_type, path,
  needs_review, review_reason.

This makes the summary reusable across:
  - vehicle specs / order guides
  - contracts and leases
  - RFPs and proposals
  - policy documents
  - pricing catalogs
  - compliance documents
  - product specifications
"""
from __future__ import annotations

import json
import os
import re
from typing import Any

from .models import (
    Block,
    BlockDiff,
    ChangeType,
    SummaryRow,
)


GENERIC_SUMMARY_PROMPT = """\
You are a senior document review analyst.

You are comparing two versions of a business document. The document may be a
vehicle specification, contract, RFP, pricing catalog, policy, compliance file,
product guide, or another structured PDF.

You will receive JSON evidence already extracted and diffed by deterministic code.
Do not invent changes. Use only the evidence provided.

Your task:
Create a concise review report for business users.

Output STRICT JSON only:
{
  "rows": [
    {
      "feature": "short display label for backward compatibility",
      "change": "one-sentence plain-language description",
      "seek_clarification": "question if ambiguity/risk exists, otherwise None",

      "area": "section, topic, clause, table, or business area",
      "item": "specific item, row, clause, field, product, requirement, term, or concept",
      "change_type": "ADDED | DELETED | MODIFIED",
      "category": "pricing | dates | availability | requirement | legal | table | wording | operational | product | other",
      "impact": "low | medium | high",
      "confidence": 0.0,

      "before": "short before text or null",
      "after": "short after text or null",
      "citation": "base p.X → target p.Y · path · key/code if available",
      "page_base": null,
      "page_target": null,
      "stable_key": null,
      "block_type": "section | paragraph | table_row | kv_pair | list_item | figure | table",
      "path": "source path",

      "needs_review": true,
      "review_reason": "why review is needed, or null"
    }
  ]
}

Rules:
- Make the language business-friendly and generic.
- Do not assume the document is about vehicles.
- Preserve exact codes, dates, prices, quantities, legal references, section references, and identifiers.
- Group duplicate or near-duplicate changes when they clearly describe the same business item.
- Prefer fewer, higher-quality rows over noisy exhaustive rows.
- Always include citation details from the evidence.
- Confidence must be between 0 and 1.
- High confidence: stable key, matching pages/paths, clear field change, or high similarity.
- Lower confidence: fuzzy text alignment, low similarity, missing side, ambiguous extracted text.
- needs_review should be true when:
  - a deleted item may imply business impact
  - wording is ambiguous
  - dates/prices/availability/obligations changed
  - confidence is below 0.70
  - the change references TBD, delay, late availability, exception, not available, required, optional, removed, deleted
- If no clarification is needed, seek_clarification must be "None".
- Cap output at 40 rows.

Evidence JSON:
{evidence_json}
"""


_KEYWORDS_CATEGORY = {
    "pricing": ("price", "cost", "$", "fee", "amount", "rate", "msrp", "invoice"),
    "dates": ("date", "delay", "late", "month", "january", "february", "march", "april",
              "may", "june", "july", "august", "september", "october", "november", "december"),
    "availability": ("available", "availability", "optional", "standard", "not available", "requires", "included"),
    "requirement": ("required", "requires", "must", "shall", "should", "mandatory"),
    "legal": ("section", "clause", "article", "agreement", "tenant", "supplier", "liability", "warranty"),
    "operational": ("process", "procedure", "workflow", "approval", "submitted", "review"),
    "product": ("engine", "package", "feature", "model", "series", "equipment", "paint", "color"),
}


def _text_preview(s: str | None, limit: int = 420) -> str | None:
    if not s:
        return None
    s = re.sub(r"\s+", " ", str(s)).strip()
    if len(s) <= limit:
        return s
    return s[: limit - 1].rstrip() + "…"


def _trim_payload(payload: dict) -> dict:
    out: dict[str, Any] = {}
    for k, v in payload.items():
        if k in {"page_width", "page_height", "__pages__", "__anchors__", "anchors"}:
            continue
        if isinstance(v, str):
            out[k] = _text_preview(v, 220)
        elif isinstance(v, list):
            out[k] = [_text_preview(str(x), 160) for x in v[:8]]
        else:
            out[k] = v
    return out


def _path_label(path: str | None) -> str:
    if not path:
        return "Document"
    parts = [p for p in path.split("/") if p]
    if not parts:
        return "Document"
    return " / ".join(p.replace("_", " ").title() for p in parts[:4])


def _infer_category(text: str) -> str:
    t = (text or "").lower()
    for category, words in _KEYWORDS_CATEGORY.items():
        if any(w in t for w in words):
            return category
    return "table" if "|" in t else "other"


def _confidence_for(d: BlockDiff, block: Block, has_field_diff: bool) -> float:
    if d.change_type in {ChangeType.ADDED, ChangeType.DELETED}:
        base = 0.78
    else:
        base = 0.58 + min(0.30, max(0.0, d.similarity) * 0.30)

    if block.stable_key:
        base += 0.08
    if has_field_diff:
        base += 0.08
    if block.block_type.value in {"table_row", "kv_pair"}:
        base += 0.04
    if d.similarity and d.similarity < 0.45 and d.change_type == ChangeType.MODIFIED:
        base -= 0.15

    return round(max(0.35, min(0.98, base)), 2)


def _impact_for(category: str, change_type: str, confidence: float, text: str) -> str:
    t = (text or "").lower()
    high_terms = (
        "price", "cost", "fee", "date", "delay", "required", "requires",
        "not available", "deleted", "removed", "shall", "must", "warranty",
        "liability", "standard", "optional", "availability"
    )
    if category in {"pricing", "dates", "availability", "requirement", "legal"}:
        return "high"
    if change_type in {"ADDED", "DELETED"} and any(term in t for term in high_terms):
        return "high"
    if confidence < 0.70:
        return "medium"
    return "medium" if change_type in {"ADDED", "DELETED", "MODIFIED"} else "low"


def _needs_review(category: str, impact: str, confidence: float, text: str, change_type: str) -> tuple[bool, str | None]:
    t = (text or "").lower()
    review_terms = (
        "tbd", "delay", "late", "not available", "deleted", "removed",
        "requires", "required", "optional", "standard", "exception",
        "warranty", "liability", "fee", "price", "cost", "date"
    )

    if confidence < 0.70:
        return True, "Low confidence change; confirm against the source document."
    if impact == "high":
        return True, "High-impact business change; confirm interpretation and downstream impact."
    if any(term in t for term in review_terms):
        return True, "Change contains terms that usually require business review."
    if change_type == "DELETED":
        return True, "Deleted content may affect prior assumptions or obligations."

    return False, None


def _citation(base: Block | None, target: Block | None, block: Block) -> str:
    page_base = base.page_number if base else None
    page_target = target.page_number if target else None

    if page_base and page_target:
        page_part = f"base p.{page_base} → target p.{page_target}"
    elif page_base:
        page_part = f"base p.{page_base}"
    elif page_target:
        page_part = f"target p.{page_target}"
    else:
        page_part = "page unknown"

    key_part = f" · key {block.stable_key}" if block.stable_key else ""
    path_part = f" · {_path_label(block.path)}" if block.path else ""

    return f"{page_part}{path_part}{key_part}"


def _select_evidence(
    diffs: list[BlockDiff],
    base_blocks: list[Block],
    target_blocks: list[Block],
    max_items: int = 260,
) -> list[dict]:
    base_by_id = {b.id: b for b in base_blocks}
    tgt_by_id = {b.id: b for b in target_blocks}

    rows: list[tuple[float, dict]] = []

    for d in diffs:
        if d.change_type == ChangeType.UNCHANGED:
            continue

        b = base_by_id.get(d.base_block_id) if d.base_block_id else None
        t = tgt_by_id.get(d.target_block_id) if d.target_block_id else None
        block = b or t
        if block is None:
            continue

        before_text = _text_preview(b.text if b else None)
        after_text = _text_preview(t.text if t else None)
        combined_text = " ".join(x for x in [before_text, after_text] if x)

        category = _infer_category(combined_text)
        confidence = _confidence_for(d, block, bool(d.field_diffs))
        impact = _impact_for(category, d.change_type.value, confidence, combined_text)
        needs_review, review_reason = _needs_review(
            category,
            impact,
            confidence,
            combined_text,
            d.change_type.value,
        )

        rec = {
            "change_type": d.change_type.value,
            "area": _path_label(block.path),
            "item": block.stable_key or _text_preview(block.text, 120) or _path_label(block.path),
            "category": category,
            "impact": impact,
            "confidence": confidence,
            "needs_review": needs_review,
            "review_reason": review_reason,
            "stable_key": block.stable_key,
            "block_type": block.block_type.value,
            "path": block.path,
            "page_base": b.page_number if b else None,
            "page_target": t.page_number if t else None,
            "citation": _citation(b, t, block),
            "before": before_text,
            "after": after_text,
            "similarity": round(d.similarity, 3),
            "field_changes": [
                {
                    "field": fd.field,
                    "before": _text_preview(str(fd.before), 160),
                    "after": _text_preview(str(fd.after), 160),
                }
                for fd in d.field_diffs[:8]
            ],
        }

        score = d.impact_score
        if needs_review:
            score += 0.25
        if impact == "high":
            score += 0.25
        if block.stable_key:
            score += 0.10
        if d.field_diffs:
            score += 0.10

        rows.append((score, rec))

    rows.sort(key=lambda kv: -kv[0])
    return [r for _, r in rows[:max_items]]


def _call_llm(prompt: str) -> str:
    """
    Calls Azure OpenAI if configured, otherwise raises.
    Required env vars:
      AZURE_OPENAI_ENDPOINT
      AZURE_OPENAI_API_KEY
      AZURE_OPENAI_DEPLOYMENT
    """
    endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    api_key = os.getenv("AZURE_OPENAI_API_KEY")
    deploy = os.getenv("AZURE_OPENAI_DEPLOYMENT")

    if not (endpoint and api_key and deploy):
        raise RuntimeError("Azure OpenAI not configured (set AZURE_OPENAI_*).")

    from openai import AzureOpenAI

    client = AzureOpenAI(
        api_key=api_key,
        azure_endpoint=endpoint,
        api_version="2024-08-01-preview",
    )

    resp = client.chat.completions.create(
        model=deploy,
        messages=[
            {"role": "system", "content": "You output strict JSON only."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.1,
        response_format={"type": "json_object"},
    )

    return resp.choices[0].message.content or '{"rows":[]}'


def summarize(
    diffs: list[BlockDiff],
    base_blocks: list[Block],
    target_blocks: list[Block],
    use_llm: bool = True,
) -> list[SummaryRow]:
    evidence = _select_evidence(diffs, base_blocks, target_blocks)

    if use_llm and evidence:
        try:
            prompt = GENERIC_SUMMARY_PROMPT.format(
                evidence_json=json.dumps(evidence, indent=2, default=str)
            )
            raw = _call_llm(prompt)
            data = json.loads(raw)

            if isinstance(data, dict):
                data = data.get("rows") or data.get("summary") or data.get("changes") or []

            if not isinstance(data, list):
                raise ValueError("expected JSON list or object containing rows")

            return [_coerce_summary_row(row) for row in data[:40]]

        except Exception as exc:
            print(f"[summarizer] LLM path failed ({exc}); falling back to heuristic.")

    return _heuristic_summary(evidence)


def _coerce_summary_row(row: dict) -> SummaryRow:
    """
    Make LLM output safe and backward-compatible.
    """
    feature = row.get("feature") or row.get("item") or row.get("area") or "Document change"
    change = row.get("change") or row.get("what_changed") or "Change detected."
    seek = row.get("seek_clarification")
    if not seek:
        seek = "None"

    confidence = row.get("confidence")
    try:
        confidence = float(confidence) if confidence is not None else None
    except Exception:
        confidence = None

    if confidence is not None:
        confidence = max(0.0, min(1.0, confidence))

    return SummaryRow(
        feature=str(feature),
        change=str(change),
        seek_clarification=str(seek),
        area=row.get("area"),
        item=row.get("item"),
        change_type=row.get("change_type"),
        category=row.get("category"),
        impact=row.get("impact"),
        confidence=confidence,
        before=row.get("before"),
        after=row.get("after"),
        citation=row.get("citation"),
        page_base=row.get("page_base"),
        page_target=row.get("page_target"),
        stable_key=row.get("stable_key"),
        block_type=row.get("block_type"),
        path=row.get("path"),
        needs_review=bool(row.get("needs_review", False)),
        review_reason=row.get("review_reason"),
    )


def _heuristic_summary(evidence: list[dict]) -> list[SummaryRow]:
    rows: list[SummaryRow] = []

    for ev in evidence[:40]:
        change_type = ev.get("change_type") or "MODIFIED"
        item = ev.get("item") or ev.get("stable_key") or "Document item"
        area = ev.get("area") or "Document"
        before = ev.get("before")
        after = ev.get("after")
        field_changes = ev.get("field_changes") or []

        if change_type == "ADDED":
            change = "Added in the newer version."
            if after:
                change = f"Added: {after}"
            seek = "Confirm whether this addition changes business process, availability, obligation, or downstream usage."
        elif change_type == "DELETED":
            change = "Removed from the newer version."
            if before:
                change = f"Removed: {before}"
            seek = "Confirm whether this removal is intentional and whether a replacement or exception exists."
        else:
            if field_changes:
                parts = [
                    f"{fc['field']}: {fc.get('before')} → {fc.get('after')}"
                    for fc in field_changes[:3]
                ]
                change = "; ".join(parts)
            elif before and after:
                change = f"Changed from '{before}' to '{after}'."
            else:
                change = "Content changed between versions."
            seek = "None"

        confidence = ev.get("confidence")
        needs_review = bool(ev.get("needs_review"))
        review_reason = ev.get("review_reason")

        if needs_review and seek == "None":
            seek = review_reason or "Review recommended."

        rows.append(SummaryRow(
            feature=str(item),
            change=change,
            seek_clarification=seek,
            area=area,
            item=str(item),
            change_type=change_type,
            category=ev.get("category"),
            impact=ev.get("impact"),
            confidence=confidence,
            before=before,
            after=after,
            citation=ev.get("citation"),
            page_base=ev.get("page_base"),
            page_target=ev.get("page_target"),
            stable_key=ev.get("stable_key"),
            block_type=ev.get("block_type"),
            path=ev.get("path"),
            needs_review=needs_review,
            review_reason=review_reason,
        ))

    return rows
