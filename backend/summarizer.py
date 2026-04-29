"""
Business-facing summarizer for document comparison diffs.

This file deliberately keeps backend extraction/classification metadata out of
the user-facing review report. Extraction intelligence is useful for routing,
quality, and search, but it must not be reported as a lease/spec/document
change.
"""
from __future__ import annotations

import json
import os
import re
from typing import Any

from .models import Block, BlockDiff, ChangeType, SummaryRow


GENERIC_SUMMARY_PROMPT = """\
You are a senior document review analyst.

You are comparing two versions of a business document. The document may be a
lease, contract, supplier specification, purchase order, policy, catalog,
financial document, table-heavy operational document, or another template.

Use only the evidence provided. Do not invent facts.

Return STRICT JSON only:
{
  "rows": [
    {
      "feature": "short business item",
      "change": "plain-language change from baseline to revised",
      "seek_clarification": "question for the responsible team if review is needed, otherwise None",
      "area": "document area/topic/clause/table",
      "item": "specific clause, field, term, row, or concept",
      "change_type": "ADDED | DELETED | MODIFIED",
      "category": "pricing | dates | availability | requirement | legal | table | wording | operational | product | other",
      "impact": "low | medium | high",
      "confidence": 0.0,
      "before": "short before value/text or null",
      "after": "short after value/text or null",
      "citation": "Baseline page X -> Revised page Y - area",
      "page_base": null,
      "page_target": null,
      "stable_key": null,
      "block_type": "section | paragraph | table_row | kv_pair | list_item | figure | table",
      "path": "source path",
      "needs_review": true,
      "review_reason": "specific review reason or null"
    }
  ]
}

Rules:
- Do not mention extraction_intelligence, template confidence, page coordinates,
  bbox, source_format, visual matching, fingerprints, or backend metadata.
- Prefer exact values: dates, prices, quantities, names, clause labels, row codes.
- For table rows, explain changed cells when field_changes are available.
- Keep the output concise and useful for business review.
- Cap output at 50 rows.

Evidence JSON:
{evidence_json}
"""


_BACKEND_METADATA_FIELDS = {
    "anchors",
    "__anchors__",
    "__pages__",
    "__row_index__",
    "__table_title__",
    "__table_context__",
    "page_width",
    "page_height",
    "spans_pages",
    "stitched_from",
    "ocr",
    "kind",
    "caption",
    "source_extraction",
    "source_format",
    "visual_match_score",
    "visual_match_source",
    "extraction_intelligence",
    "table_fingerprint",
    "column_profiles",
    "extraction_confidence",
    "quality_warnings",
    "language",
    "header_rows",
    "header_row_count",
    "header_index",
    "header_strategy",
    "header_sources",
    "strategies",
    "bbox_by_page",
}

_PAYLOAD_TABLE_NON_CONTENT = {
    "rows",
    "header",
    "near_texts",
    "source_tables",
}

_KEYWORDS_CATEGORY = {
    "pricing": ("price", "cost", "$", "€", "£", "fee", "amount", "rate", "rent", "payment"),
    "dates": (
        "date", "delay", "late", "effective", "expiry", "term", "month", "year",
        "january", "february", "march", "april", "may", "june", "july",
        "august", "september", "october", "november", "december",
    ),
    "availability": ("available", "availability", "optional", "standard", "included", "excluded", "removed"),
    "requirement": ("required", "requires", "must", "shall", "mandatory", "obligation", "approval"),
    "legal": ("lease", "tenant", "landlord", "clause", "section", "agreement", "liability", "warranty", "termination"),
    "operational": ("process", "procedure", "workflow", "approval", "submitted", "review", "status"),
    "product": ("engine", "package", "feature", "model", "series", "equipment", "paint", "color", "colour"),
    "table": ("table", "row", "column", "code", "part", "pcv", "pcb", "cell", "value"),
}


def _norm(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _norm_lower(value: Any) -> str:
    return _norm(value).lower()


def _preview(value: Any, limit: int = 420) -> str | None:
    text = _norm(value)
    if not text:
        return None
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "..."


def _path_label(path: str | None) -> str:
    parts = [p for p in (path or "").split("/") if p]
    cleaned = []
    for part in parts:
        if part.startswith("table_") or part.startswith("row_"):
            continue
        cleaned.append(part.replace("_", " ").title())
    return " / ".join(cleaned[:5]) if cleaned else "Document"


def _is_backend_field(field: Any) -> bool:
    key = str(field or "")
    if not key:
        return True
    if key.startswith("__"):
        return True
    if key in _BACKEND_METADATA_FIELDS:
        return True
    if key.startswith("extraction_") or key.endswith("_confidence"):
        return True
    return False


def _visible_payload(block: Block | None) -> dict[str, Any]:
    if not block or not isinstance(block.payload, dict):
        return {}

    out = {}
    for key, value in block.payload.items():
        key = str(key)
        if _is_backend_field(key):
            continue
        if block.block_type.value == "table" and key in _PAYLOAD_TABLE_NON_CONTENT:
            continue
        out[key] = value
    return out


def _trim_payload(payload: dict[str, Any]) -> dict[str, Any]:
    out = {}
    for key, value in payload.items():
        if _is_backend_field(key):
            continue
        if isinstance(value, str):
            out[key] = _preview(value, 220)
        elif isinstance(value, list):
            out[key] = [_preview(item, 160) for item in value[:8]]
        elif isinstance(value, dict):
            out[key] = {str(k): _preview(v, 160) for k, v in list(value.items())[:10] if not _is_backend_field(k)}
        else:
            out[key] = value
    return out


def _field_changes(d: BlockDiff) -> list[dict[str, Any]]:
    changes = []
    for fd in d.field_diffs[:24]:
        if _is_backend_field(fd.field):
            continue
        before = _preview(fd.before, 220)
        after = _preview(fd.after, 220)
        if _norm_lower(before) == _norm_lower(after):
            continue
        changes.append({"field": str(fd.field), "before": before, "after": after})
    return changes


def _block_user_text(block: Block | None) -> str:
    if not block:
        return ""
    parts = [block.text or ""]
    payload = _visible_payload(block)
    for key, value in payload.items():
        if value in (None, "", [], {}):
            continue
        parts.append(f"{key}: {value}")
    return _norm(" | ".join(str(p) for p in parts if p))


def _metadata_only_change(d: BlockDiff, base: Block | None, target: Block | None) -> bool:
    if d.change_type != ChangeType.MODIFIED:
        return False
    if _field_changes(d):
        return False
    return _norm_lower(_block_user_text(base)) == _norm_lower(_block_user_text(target))


def _row_key(block: Block) -> str | None:
    if block.stable_key:
        return str(block.stable_key)
    for value in _visible_payload(block).values():
        text = _preview(value, 120)
        if text:
            return text
    return _preview(block.text, 120)


def _item_label(block: Block) -> str:
    if block.block_type.value == "table_row":
        key = _row_key(block)
        return f"Table row {key}" if key else "Table row"
    if block.stable_key:
        return str(block.stable_key)
    return _preview(block.text, 130) or _path_label(block.path)


def _category(text: str, block_type: str | None, field_changes: list[dict[str, Any]]) -> str:
    if block_type == "table_row":
        return "table"
    combined = f"{_norm_lower(text)} {' '.join(_norm_lower(fc.get('field')) for fc in field_changes)}"
    for name, words in _KEYWORDS_CATEGORY.items():
        if any(word in combined for word in words):
            return name
    return "other"


def _confidence(d: BlockDiff, block: Block, has_field_diff: bool) -> float:
    if d.change_type in {ChangeType.ADDED, ChangeType.DELETED}:
        score = 0.78
    else:
        score = 0.60 + min(0.28, max(0.0, d.similarity) * 0.28)
    if block.stable_key:
        score += 0.08
    if has_field_diff:
        score += 0.08
    if block.block_type.value in {"table_row", "kv_pair"}:
        score += 0.04
    return round(max(0.35, min(0.98, score)), 2)


def _impact(category: str, change_type: str, confidence: float, text: str, fields: list[dict[str, Any]]) -> str:
    field_text = " ".join(_norm_lower(f.get("field")) for f in fields)
    critical = any(term in f"{_norm_lower(text)} {field_text}" for term in (
        "price", "cost", "rent", "fee", "date", "term", "required", "shall",
        "tenant", "landlord", "liability", "availability", "payment",
    ))
    if category in {"pricing", "dates", "requirement", "legal"} or critical:
        return "high"
    if change_type in {"ADDED", "DELETED"}:
        return "medium"
    if confidence < 0.70:
        return "medium"
    return "medium"


def _review_need(category: str, impact: str, confidence: float, change_type: str, text: str) -> tuple[bool, str | None]:
    if confidence < 0.70:
        return True, "Lower confidence match; confirm against the source document."
    if change_type == "DELETED":
        return True, "Removed content should be confirmed as intentional."
    if category == "pricing":
        return True, "Confirm commercial or pricing impact with the responsible team."
    if category == "dates":
        return True, "Confirm effective dates, timelines, or term impact."
    if category == "legal":
        return True, "Confirm legal or lease interpretation with the responsible team."
    if category == "requirement":
        return True, "Confirm obligation, approval, or requirement impact."
    if category == "availability":
        return True, "Confirm availability or standard/optional status."
    if impact == "high":
        return True, "High-impact change; confirm business interpretation."
    if any(term in _norm_lower(text) for term in ("shall", "must", "required", "fee", "rent", "payment")):
        return True, "Business term changed; review recommended."
    return False, None


def _citation(base: Block | None, target: Block | None, block: Block) -> str:
    if base and target:
        page = f"Baseline page {base.page_number} -> Revised page {target.page_number}"
    elif base:
        page = f"Baseline page {base.page_number}"
    elif target:
        page = f"Revised page {target.page_number}"
    else:
        page = "Page unknown"
    key = f" - key {block.stable_key}" if block.stable_key else ""
    return f"{page} - {_path_label(block.path)}{key}"


def _change_sentence(change_type: str, before: str | None, after: str | None, fields: list[dict[str, Any]], block_type: str) -> str:
    parts = []
    for fc in fields[:4]:
        field = fc.get("field") or "value"
        b = fc.get("before")
        a = fc.get("after")
        if b is not None and a is not None:
            parts.append(f"{field} changed from {b} to {a}")
        elif b is not None:
            parts.append(f"{field} was removed ({b})")
        elif a is not None:
            parts.append(f"{field} was added ({a})")
    if parts:
        return "; ".join(parts) + "."
    if change_type == "ADDED":
        return f"Added: {after}" if after else "Added in the revised document."
    if change_type == "DELETED":
        return f"Removed: {before}" if before else "Removed from the revised document."
    if before and after:
        prefix = "Table row changed" if block_type == "table_row" else "Changed"
        return f"{prefix} from '{before}' to '{after}'."
    return "Content changed between versions."


def _dedupe_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        _norm_lower(row.get("area")),
        _norm_lower(row.get("stable_key") or row.get("item"))[:120],
        _norm_lower(row.get("change_type")),
    )


def _select_evidence(diffs: list[BlockDiff], base_blocks: list[Block], target_blocks: list[Block], limit: int = 320) -> list[dict[str, Any]]:
    base_by_id = {b.id: b for b in base_blocks}
    target_by_id = {b.id: b for b in target_blocks}
    scored: list[tuple[float, dict[str, Any]]] = []
    seen = set()

    for d in diffs:
        if d.change_type == ChangeType.UNCHANGED:
            continue

        base = base_by_id.get(d.base_block_id) if d.base_block_id else None
        target = target_by_id.get(d.target_block_id) if d.target_block_id else None
        block = base or target
        if not block:
            continue
        if _metadata_only_change(d, base, target):
            continue

        fields = _field_changes(d)
        before = _preview(_block_user_text(base), 420) if base else None
        after = _preview(_block_user_text(target), 420) if target else None
        combined = " ".join(x for x in [before, after, json.dumps(fields, ensure_ascii=False)] if x)
        category = _category(combined, block.block_type.value, fields)
        confidence = _confidence(d, block, bool(fields))
        impact = _impact(category, d.change_type.value, confidence, combined, fields)
        needs_review, review_reason = _review_need(category, impact, confidence, d.change_type.value, combined)

        row = {
            "change_type": d.change_type.value,
            "area": _path_label(block.path),
            "item": _item_label(block),
            "category": category,
            "impact": impact,
            "confidence": confidence,
            "needs_review": needs_review,
            "review_reason": review_reason,
            "stable_key": block.stable_key,
            "block_type": block.block_type.value,
            "path": block.path,
            "page_base": base.page_number if base else None,
            "page_target": target.page_number if target else None,
            "citation": _citation(base, target, block),
            "before": before,
            "after": after,
            "field_changes": fields,
        }

        key = _dedupe_key(row)
        if key in seen:
            continue
        seen.add(key)

        score = float(d.impact_score or 0)
        score += 0.22 if impact == "high" else 0.0
        score += 0.14 if needs_review else 0.0
        score += 0.12 if fields else 0.0
        score += 0.06 if block.block_type.value == "table_row" else 0.0
        scored.append((score, row))

    scored.sort(key=lambda item: -item[0])
    return [row for _, row in scored[:limit]]


def _call_llm(prompt: str) -> str:
    endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    api_key = os.getenv("AZURE_OPENAI_API_KEY")
    deploy = os.getenv("AZURE_OPENAI_DEPLOYMENT") or os.getenv("AZURE_OPENAI_CHAT_DEPLOYMENT")
    if not (endpoint and api_key and deploy):
        raise RuntimeError("Azure OpenAI not configured.")

    from openai import AzureOpenAI

    client = AzureOpenAI(
        api_key=api_key,
        azure_endpoint=endpoint,
        api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-08-01-preview"),
    )
    resp = client.chat.completions.create(
        model=deploy,
        messages=[
            {"role": "system", "content": "You output strict JSON only."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.1,
        max_tokens=2400,
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
                evidence_json=json.dumps(evidence[:120], ensure_ascii=False, indent=2, default=str)
            )
            raw = _call_llm(prompt)
            data = json.loads(raw)
            rows = data.get("rows") if isinstance(data, dict) else data
            if isinstance(rows, list):
                return [_coerce_summary_row(row) for row in rows[:50] if isinstance(row, dict)]
        except Exception as exc:
            print(f"[summarizer] AI path failed ({exc}); using deterministic summary.")

    return _heuristic_summary(evidence)


def _coerce_summary_row(row: dict[str, Any]) -> SummaryRow:
    feature = row.get("feature") or row.get("item") or row.get("area") or "Document change"
    change = row.get("change") or "Change detected."
    seek = row.get("seek_clarification") or "None"

    try:
        confidence = float(row["confidence"]) if row.get("confidence") is not None else None
    except Exception:
        confidence = None
    if confidence is not None:
        confidence = max(0.0, min(1.0, confidence))

    needs_review = bool(row.get("needs_review")) or (seek != "None")
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


def _heuristic_summary(evidence: list[dict[str, Any]]) -> list[SummaryRow]:
    rows: list[SummaryRow] = []
    for ev in evidence[:50]:
        change_type = ev.get("change_type") or "MODIFIED"
        fields = ev.get("field_changes") or []
        before = ev.get("before")
        after = ev.get("after")
        block_type = ev.get("block_type") or "paragraph"
        change = _change_sentence(change_type, before, after, fields, block_type)
        seek = ev.get("review_reason") if ev.get("needs_review") else "None"

        rows.append(
            SummaryRow(
                feature=str(ev.get("item") or ev.get("area") or "Document change"),
                change=change,
                seek_clarification=seek or "Review recommended.",
                area=ev.get("area"),
                item=str(ev.get("item") or "Document item"),
                change_type=change_type,
                category=ev.get("category") or "other",
                impact=ev.get("impact") or "medium",
                confidence=ev.get("confidence"),
                before=before,
                after=after,
                citation=ev.get("citation"),
                page_base=ev.get("page_base"),
                page_target=ev.get("page_target"),
                stable_key=ev.get("stable_key"),
                block_type=block_type,
                path=ev.get("path"),
                needs_review=bool(ev.get("needs_review")),
                review_reason=ev.get("review_reason"),
            )
        )

    return rows
