"""
Summarizer - turns raw block-level diffs into a generic user-facing
review report.

The output remains compatible with the original:
  Feature | Change | Seek Clarification

But each row also carries generic fields:
  area, item, change_type, category, impact, confidence, before, after,
  citation, page_base, page_target, stable_key, block_type, path,
  needs_review, review_reason.
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
product guide, table-heavy operational document, or another structured PDF.

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
      "citation": "base p.X -> target p.Y - path - key/code if available",
      "page_base": null,
      "page_target": null,
      "stable_key": null,
      "block_type": "section | paragraph | table_row | kv_pair | list_item | figure | table",
      "path": "source path",

      "needs_review": true,
      "review_reason": "specific reason review is needed, or null"
    }
  ]
}

Rules:
- Make the language business-friendly and generic.
- Do not assume the document is about vehicles.
- Preserve exact codes, dates, prices, quantities, section references, and identifiers.
- For table rows, explain the changed field/cell values whenever field_changes are provided.
- Prefer specific language such as "Power changed from 200 HP to 210 HP" over generic "content changed".
- Group duplicate or near-duplicate changes when they clearly describe the same business item.
- Prefer fewer, higher-quality rows over noisy exhaustive rows.
- Always include citation details from the evidence.
- Confidence must be between 0 and 1.
- Mark needs_review true when dates, prices, requirements, availability, obligations, or deleted content changed.
- Do not mark every row high impact. Use medium unless a business-critical signal is present.
- If no clarification is needed, seek_clarification must be "None".
- Cap output at 40 rows.

Evidence JSON:
{evidence_json}
"""


_INTERNAL_FIELDS = {
    "page_width",
    "page_height",
    "__pages__",
    "__anchors__",
    "anchors",
    "spans_pages",
    "stitched_from",
    "ocr",
    "kind",
    "caption",
}

_KEYWORDS_CATEGORY = {
    "pricing": ("price", "cost", "$", "fee", "amount", "rate", "msrp", "invoice", "payment"),
    "dates": (
        "date", "delay", "late", "effective", "month", "year", "january", "february",
        "march", "april", "may", "june", "july", "august", "september",
        "october", "november", "december"
    ),
    "availability": (
        "available", "availability", "optional", "standard", "not available",
        "requires", "included", "excluded", "late availability"
    ),
    "requirement": ("required", "requires", "must", "shall", "should", "mandatory", "minimum"),
    "legal": ("section", "clause", "article", "agreement", "tenant", "supplier", "liability", "warranty"),
    "operational": ("process", "procedure", "workflow", "approval", "submitted", "review", "status"),
    "product": ("engine", "package", "feature", "model", "series", "equipment", "paint", "color"),
    "table": ("table", "row", "column", "code", "part", "pcb", "cell", "value"),
}


def _norm(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _norm_lower(value: Any) -> str:
    return _norm(value).lower()


def _text_preview(value: Any, limit: int = 420) -> str | None:
    text = _norm(value)
    if not text:
        return None
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


def _visible_payload(block: Block | None) -> dict[str, Any]:
    if not block or not isinstance(block.payload, dict):
        return {}

    out = {}
    for key, value in block.payload.items():
        key = str(key)
        if key in _INTERNAL_FIELDS or key.startswith("__"):
            continue
        out[key] = value

    return out


def _trim_payload(payload: dict) -> dict[str, Any]:
    out: dict[str, Any] = {}

    for key, value in payload.items():
        key = str(key)
        if key in _INTERNAL_FIELDS or key.startswith("__"):
            continue

        if isinstance(value, str):
            out[key] = _text_preview(value, 220)
        elif isinstance(value, list):
            out[key] = [_text_preview(str(x), 160) for x in value[:8]]
        elif isinstance(value, dict):
            out[key] = {str(k): _text_preview(str(v), 160) for k, v in list(value.items())[:10]}
        else:
            out[key] = value

    return out


def _infer_category(text: str, block_type: str | None = None, field_changes: list[dict] | None = None) -> str:
    if block_type == "table_row":
        return "table"

    t = _norm_lower(text)
    field_text = " ".join(str(fc.get("field", "")) for fc in (field_changes or []))
    combined = f"{t} {field_text.lower()}"

    for category, words in _KEYWORDS_CATEGORY.items():
        if any(word in combined for word in words):
            return category

    return "other"


def _confidence_for(d: BlockDiff, block: Block, has_field_diff: bool) -> float:
    if d.change_type in {ChangeType.ADDED, ChangeType.DELETED}:
        base = 0.76
    else:
        base = 0.60 + min(0.28, max(0.0, d.similarity) * 0.28)

    if block.stable_key:
        base += 0.08
    if has_field_diff:
        base += 0.08
    if block.block_type.value in {"table_row", "kv_pair"}:
        base += 0.04
    if d.change_type == ChangeType.MODIFIED and d.similarity < 0.45 and not has_field_diff:
        base -= 0.14

    return round(max(0.35, min(0.98, base)), 2)


def _review_signal(text: str, field_changes: list[dict] | None = None) -> bool:
    t = _norm_lower(text)
    field_text = " ".join(_norm_lower(fc.get("field")) for fc in (field_changes or []))
    combined = f"{t} {field_text}"

    review_terms = (
        "tbd", "delay", "late", "not available", "deleted", "removed",
        "requires", "required", "optional", "standard", "exception",
        "warranty", "liability", "fee", "price", "cost", "date",
        "effective", "shall", "must", "availability", "$", "payment",
    )

    return any(term in combined for term in review_terms)


def _impact_for(category: str, change_type: str, confidence: float, text: str, field_changes: list[dict]) -> str:
    t = _norm_lower(text)
    critical_field = any(
        any(term in _norm_lower(fc.get("field")) for term in ("price", "cost", "date", "required", "availability", "fee"))
        for fc in field_changes
    )

    if category in {"pricing", "dates", "requirement", "legal"}:
        return "high"

    if category == "availability" and (change_type in {"ADDED", "DELETED"} or critical_field):
        return "high"

    if critical_field:
        return "high"

    if "not available" in t or "removed" in t or "deleted" in t:
        return "high"

    if confidence < 0.70:
        return "medium"

    if category == "table":
        return "medium"

    return "medium" if change_type in {"ADDED", "DELETED", "MODIFIED"} else "low"


def _needs_review(
    category: str,
    impact: str,
    confidence: float,
    text: str,
    change_type: str,
    field_changes: list[dict],
) -> tuple[bool, str | None]:
    if confidence < 0.70:
        return True, "Lower confidence match; confirm against the source PDF."

    if change_type == "DELETED":
        return True, "Removed content should be confirmed as intentional."

    if category in {"pricing", "dates", "requirement", "legal"}:
        return True, f"{category.title()} change may affect downstream interpretation."

    if category == "availability":
        return True, "Availability or standard/optional status changed."

    if _review_signal(text, field_changes):
        return True, "Change contains business terms that typically require review."

    if impact == "high":
        return True, "High-impact change; confirm business interpretation."

    return False, None


def _citation(base: Block | None, target: Block | None, block: Block) -> str:
    page_base = base.page_number if base else None
    page_target = target.page_number if target else None

    if page_base and page_target:
        page_part = f"base p.{page_base} -> target p.{page_target}"
    elif page_base:
        page_part = f"base p.{page_base}"
    elif page_target:
        page_part = f"target p.{page_target}"
    else:
        page_part = "page unknown"

    key_part = f" - key {block.stable_key}" if block.stable_key else ""
    path_part = f" - {_path_label(block.path)}" if block.path else ""

    return f"{page_part}{path_part}{key_part}"


def _row_key(block: Block) -> str | None:
    if block.stable_key:
        return str(block.stable_key)

    payload = _visible_payload(block)
    for value in payload.values():
        text = _text_preview(value, 100)
        if text:
            return text

    return _text_preview(block.text, 100)


def _item_label(block: Block) -> str:
    if block.block_type.value == "table_row":
        key = _row_key(block)
        return f"Table row {key}" if key else "Table row"

    if block.stable_key:
        return str(block.stable_key)

    return _text_preview(block.text, 120) or _path_label(block.path)


def _field_changes(d: BlockDiff) -> list[dict]:
    out = []

    for fd in d.field_diffs[:12]:
        field = str(fd.field)

        if field in _INTERNAL_FIELDS or field.startswith("__"):
            continue

        out.append(
            {
                "field": field,
                "before": _text_preview(fd.before, 220),
                "after": _text_preview(fd.after, 220),
            }
        )

    return out


def _field_change_sentence(field_changes: list[dict], max_fields: int = 3) -> str:
    parts = []

    for fc in field_changes[:max_fields]:
        field = fc.get("field") or "value"
        before = fc.get("before")
        after = fc.get("after")

        if before is not None and after is not None:
            parts.append(f"{field} changed from {before} to {after}")
        elif before is not None:
            parts.append(f"{field} was removed ({before})")
        elif after is not None:
            parts.append(f"{field} was added ({after})")

    return "; ".join(parts)


def _change_sentence(change_type: str, before: str | None, after: str | None, field_changes: list[dict], block_type: str) -> str:
    if field_changes:
        sentence = _field_change_sentence(field_changes)
        if sentence:
            return sentence + "."

    if change_type == "ADDED":
        if after:
            return f"Added: {after}"
        return "Added in the revised document."

    if change_type == "DELETED":
        if before:
            return f"Removed: {before}"
        return "Removed from the revised document."

    if before and after:
        if block_type == "table_row":
            return f"Table row changed from '{before}' to '{after}'."
        return f"Changed from '{before}' to '{after}'."

    return "Content changed between versions."


def _dedupe_key(rec: dict) -> tuple[str, str, str]:
    item = _norm_lower(rec.get("item"))
    area = _norm_lower(rec.get("area"))
    change_type = _norm_lower(rec.get("change_type"))
    stable = _norm_lower(rec.get("stable_key"))
    return (area, stable or item[:80], change_type)


def _select_evidence(
    diffs: list[BlockDiff],
    base_blocks: list[Block],
    target_blocks: list[Block],
    max_items: int = 260,
) -> list[dict]:
    base_by_id = {b.id: b for b in base_blocks}
    target_by_id = {b.id: b for b in target_blocks}

    scored_rows: list[tuple[float, dict]] = []
    seen: set[tuple[str, str, str]] = set()

    for d in diffs:
        if d.change_type == ChangeType.UNCHANGED:
            continue

        b = base_by_id.get(d.base_block_id) if d.base_block_id else None
        t = target_by_id.get(d.target_block_id) if d.target_block_id else None
        block = b or t

        if block is None:
            continue

        before_text = _text_preview(b.text if b else None)
        after_text = _text_preview(t.text if t else None)
        field_changes = _field_changes(d)
        field_text = " ".join(f"{fc.get('field')} {fc.get('before')} {fc.get('after')}" for fc in field_changes)
        combined_text = " ".join(x for x in [before_text, after_text, field_text] if x)

        category = _infer_category(combined_text, block.block_type.value, field_changes)
        confidence = _confidence_for(d, block, bool(field_changes))
        impact = _impact_for(category, d.change_type.value, confidence, combined_text, field_changes)
        needs_review, review_reason = _needs_review(
            category,
            impact,
            confidence,
            combined_text,
            d.change_type.value,
            field_changes,
        )

        item = _item_label(block)
        area = _path_label(block.path)

        rec = {
            "change_type": d.change_type.value,
            "area": area,
            "item": item,
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
            "field_changes": field_changes,
            "base_payload": _trim_payload(_visible_payload(b)) if b else None,
            "target_payload": _trim_payload(_visible_payload(t)) if t else None,
        }

        key = _dedupe_key(rec)
        if key in seen:
            continue
        seen.add(key)

        score = d.impact_score
        if impact == "high":
            score += 0.22
        if needs_review:
            score += 0.16
        if block.stable_key:
            score += 0.10
        if field_changes:
            score += 0.14
        if block.block_type.value == "table_row":
            score += 0.06

        scored_rows.append((score, rec))

    scored_rows.sort(key=lambda kv: -kv[0])
    return [row for _, row in scored_rows[:max_items]]


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
                evidence_json=json.dumps(evidence[:140], indent=2, default=str)
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
    feature = row.get("feature") or row.get("item") or row.get("area") or "Document change"
    change = row.get("change") or row.get("what_changed") or "Change detected."
    seek = row.get("seek_clarification") or "None"

    confidence = row.get("confidence")
    try:
        confidence = float(confidence) if confidence is not None else None
    except Exception:
        confidence = None

    if confidence is not None:
        confidence = max(0.0, min(1.0, confidence))

    needs_review = bool(row.get("needs_review", False))
    if seek and seek != "None":
        needs_review = True

    return SummaryRow(
        feature=str(feature),
        change=str(change),
        seek_clarification=str(seek),
        area=row.get("area"),
        item=row.get("item") or str(feature),
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
        needs_review=needs_review,
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
        block_type = ev.get("block_type") or "paragraph"

        change = _change_sentence(change_type, before, after, field_changes, block_type)

        confidence = ev.get("confidence")
        impact = ev.get("impact") or "medium"
        category = ev.get("category") or "other"
        needs_review = bool(ev.get("needs_review"))
        review_reason = ev.get("review_reason")

        if needs_review:
            seek = review_reason or "Review recommended."
        else:
            seek = "None"

        rows.append(
            SummaryRow(
                feature=str(item),
                change=change,
                seek_clarification=seek,
                area=area,
                item=str(item),
                change_type=change_type,
                category=category,
                impact=impact,
                confidence=confidence,
                before=before,
                after=after,
                citation=ev.get("citation"),
                page_base=ev.get("page_base"),
                page_target=ev.get("page_target"),
                stable_key=ev.get("stable_key"),
                block_type=block_type,
                path=ev.get("path"),
                needs_review=needs_review,
                review_reason=review_reason,
            )
        )

    return rows
