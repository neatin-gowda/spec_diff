"""
Natural-language query layer.

Returns business-facing answers over:
  - block-level document diffs
  - table rows and table cells
  - row/key comparisons across old/new documents

Response shape:
{
  "answer": "...",
  "rows": [...],
  "count": 10,
  "plan": {...},
  "mode": "fast"|"ai"
}
"""
from __future__ import annotations

import json
import os
import re
from typing import Any, Optional

from rapidfuzz import fuzz

from .db import db_enabled, get_conn
from .embeddings import embed_query, vector_literal
from .models import Block, BlockDiff, ChangeType


AI_ENV_NAMES = {
    "endpoint": ("AZURE_OPENAI_ENDPOINT",),
    "api_key": ("AZURE_OPENAI_API_KEY",),
    "deployment": (
        "AZURE_OPENAI_DEPLOYMENT",
        "AZURE_OPENAI_CHAT_DEPLOYMENT",
        "AZURE_OPENAI_DEPLOYMENT_NAME",
        "AZURE_OPENAI_MODEL",
    ),
    "api_version": ("AZURE_OPENAI_API_VERSION",),
}


AI_EVIDENCE_BUDGET_CHARS = int(os.getenv("AI_EVIDENCE_BUDGET_CHARS", "60000"))
AI_EVIDENCE_RETRY_BUDGET_CHARS = int(os.getenv("AI_EVIDENCE_RETRY_BUDGET_CHARS", "28000"))


_KEY_RX = re.compile(r"\b([A-Z0-9]{2,6}[A-Z]?)\b")
_NUMBER_RX = re.compile(r"\b\d{2,6}\b")
_QUOTED_RX = re.compile(r"[\"']([^\"']{2,80})[\"']")
_SIDE_SPLIT_RX = re.compile(
    r"(?:old|previous|base|baseline|202\d|19\d\d).{0,80}?(?:new|current|target|revised|latest|202\d|19\d\d)",
    re.I,
)

_INTERNAL_TABLE_FIELDS = {
    "__anchors__",
    "__pages__",
    "anchors",
    "page_width",
    "page_height",
}

KNOWN_SECTIONS = {
    "big bend": ["big_bend"],
    "badlands": ["badlands"],
    "wildtrak": ["wildtrak"],
    "outer banks": ["outer_banks"],
    "raptor": ["raptor"],
    "everglades": ["everglades"],
    "heritage": ["heritage_edition", "heritage_limited_edition", "heritage"],
    "stroppe": ["stroppe_edition", "stroppe"],
    "black diamond": ["black_diamond"],
    "sasquatch": ["sasquatch"],
    "base": ["base"],
    "pricing": ["pricing", "price", "cost"],
    "engine": ["engine", "powertrain", "mechanical"],
    "transmission": ["transmission", "powertrain", "mechanical"],
    "paint": ["paint", "color", "exterior"],
    "safety": ["safety", "security"],
    "legal": ["legal", "clause", "article", "section"],
    "requirement": ["requirement", "requires", "required"],
    "availability": ["availability", "available", "optional", "standard"],
}

INTENT_KEYWORDS = {
    "ADDED": ("add", "added", "new", "included", "introduced"),
    "DELETED": ("delete", "deleted", "removed", "dropped", "no longer"),
    "MODIFIED": ("modif", "changed", "updated", "revised", "different"),
}

CATEGORY_KEYWORDS = {
    "pricing": ("price", "cost", "fee", "amount", "$", "rate", "msrp"),
    "dates": ("date", "delay", "late", "month", "year", "effective"),
    "availability": ("available", "availability", "optional", "standard", "not available", "requires", "included"),
    "requirement": ("required", "requires", "must", "shall", "mandatory"),
    "legal": ("section", "clause", "article", "agreement", "warranty", "liability"),
    "product": ("engine", "package", "feature", "model", "series", "equipment", "paint", "color"),
    "table": ("table", "row", "column", "cell", "code", "pcb", "pcv", "part", "item", "value"),
}

STOP_QUERY_TERMS = {
    "what", "show", "list", "changed", "change", "added", "deleted", "removed",
    "compare", "between", "from", "with", "against", "previous", "current",
    "target", "base", "baseline", "latest", "revised", "file", "document",
    "table", "row", "column", "value", "old", "new", "the", "and", "for",
}


IDENTIFIER_CONTEXT_TERMS = {
    "pcv",
    "pcb",
    "code",
    "part",
    "item",
    "row",
    "column",
    "value",
    "values",
    "package",
    "feature",
}


SUMMARY_TERMS = (
    "summary",
    "summarize",
    "summarise",
    "overview",
    "brief",
    "short",
    "high level",
    "key changes",
    "main changes",
    "important changes",
    "review items",
    "review table",
    "seek clarification",
)

FEATURE_TABLE_TERMS = (
    "feature",
    "change",
    "seek clarification",
)


def _norm(s: Any) -> str:
    return re.sub(r"\s+", " ", str(s or "").lower()).strip()


def _env_first(names: tuple[str, ...], default: str = "") -> str:
    for name in names:
        value = os.getenv(name)
        if value:
            return value.strip()
    return default


def _openai_config() -> dict[str, Any]:
    endpoint = _env_first(AI_ENV_NAMES["endpoint"])
    api_key = _env_first(AI_ENV_NAMES["api_key"])
    deployment = _env_first(AI_ENV_NAMES["deployment"])
    api_version = _env_first(AI_ENV_NAMES["api_version"], "2024-08-01-preview")
    missing = []

    if not endpoint:
        missing.append("AZURE_OPENAI_ENDPOINT")
    if not api_key:
        missing.append("AZURE_OPENAI_API_KEY")
    if not deployment:
        missing.append("AZURE_OPENAI_DEPLOYMENT or AZURE_OPENAI_CHAT_DEPLOYMENT")

    return {
        "configured": not missing,
        "missing": missing,
        "endpoint_set": bool(endpoint),
        "api_key_set": bool(api_key),
        "deployment": deployment,
        "api_version": api_version,
    }


def ai_health() -> dict[str, Any]:
    status = _openai_config()
    if not status["configured"]:
        return {**status, "ok": False, "message": "Azure OpenAI chat is not fully configured."}

    try:
        from openai import AzureOpenAI

        client = AzureOpenAI(
            api_key=_env_first(AI_ENV_NAMES["api_key"]),
            azure_endpoint=_env_first(AI_ENV_NAMES["endpoint"]),
            api_version=status["api_version"],
        )
        resp = client.chat.completions.create(
            model=status["deployment"],
            messages=[
                {"role": "system", "content": "Return JSON only."},
                {"role": "user", "content": '{"ping":"ok"}'},
            ],
            temperature=0,
            max_tokens=16,
            response_format={"type": "json_object"},
        )
        content = resp.choices[0].message.content or ""
        return {**status, "ok": True, "message": "Azure OpenAI chat call succeeded.", "sample": content[:120]}
    except Exception as exc:
        return {**status, "ok": False, "message": f"Azure OpenAI chat call failed: {type(exc).__name__}: {exc}"}


def _preview(s: Any, limit: int = 360) -> str | None:
    if s is None:
        return None
    text = re.sub(r"\s+", " ", str(s)).strip()
    if not text:
        return None
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "..."


def _payload_search_text(payload: Any, limit: int = 3500) -> str:
    if not isinstance(payload, dict):
        return ""

    useful = {}
    for key, value in payload.items():
        key_text = str(key)
        if key_text.startswith("__") or key_text in _INTERNAL_TABLE_FIELDS:
            continue
        if key_text in {"page_width", "page_height", "source_extraction", "visual_match_score", "visual_match_source"}:
            continue
        useful[key_text] = value

    if not useful:
        return ""

    text = json.dumps(useful, ensure_ascii=False, default=str)
    return text[:limit]


def _field_diff_search_text(field_diffs: Any) -> str:
    if not field_diffs:
        return ""

    pieces = []
    for fd in field_diffs[:30]:
        field = getattr(fd, "field", None) if not isinstance(fd, dict) else fd.get("field")
        before = getattr(fd, "before", None) if not isinstance(fd, dict) else fd.get("before")
        after = getattr(fd, "after", None) if not isinstance(fd, dict) else fd.get("after")
        pieces.append(f"{field}: {before} -> {after}")

    return " | ".join(pieces)


def _diff_search_text(d: BlockDiff, base: Block | None, target: Block | None, block: Block) -> str:
    parts = [
        base.text if base else None,
        target.text if target else None,
        block.path,
        block.stable_key,
        _payload_search_text(base.payload if base else None),
        _payload_search_text(target.payload if target else None),
        _field_diff_search_text(d.field_diffs),
    ]
    return " ".join(str(part) for part in parts if part)


def _path_label(path: str | None) -> str:
    if not path:
        return "Document"
    parts = [p for p in path.split("/") if p]
    if not parts:
        return "Document"
    return " / ".join(p.replace("_", " ").title() for p in parts[:4])


def _citation(base: Block | None, target: Block | None, block: Block) -> str:
    if base and target:
        page_part = f"base p.{base.page_number} -> target p.{target.page_number}"
    elif base:
        page_part = f"base p.{base.page_number}"
    elif target:
        page_part = f"target p.{target.page_number}"
    else:
        page_part = "page unknown"

    path = _path_label(block.path)
    key = f" - key {block.stable_key}" if block.stable_key else ""
    return f"{page_part} - {path}{key}"


def _infer_category(text: str) -> str:
    t = _norm(text)
    for category, words in CATEGORY_KEYWORDS.items():
        if any(w in t for w in words):
            return category
    return "other"


def _confidence(d: BlockDiff, block: Block) -> float:
    score = 0.62

    if d.change_type in {ChangeType.ADDED, ChangeType.DELETED}:
        score = 0.78
    elif d.change_type == ChangeType.MODIFIED:
        score = 0.58 + min(0.28, max(0.0, d.similarity) * 0.28)

    if block.stable_key:
        score += 0.08
    if d.field_diffs:
        score += 0.08
    if block.block_type.value in {"table_row", "kv_pair"}:
        score += 0.04
    if d.change_type == ChangeType.MODIFIED and d.similarity < 0.45:
        score -= 0.12

    return round(max(0.35, min(0.98, score)), 2)


# ---------------- table/query helpers ----------------

def _is_table_intent(nl: str) -> bool:
    q = _norm(nl)
    table_terms = (
        "table", "row", "column", "cell", "code", "pcb", "pcv", "part number",
        "item number", "compare", "value", "values"
    )
    has_table_word = any(term in q for term in table_terms)
    has_multiple_keys = len(_extract_identifiers(nl)) >= 1
    return has_table_word and has_multiple_keys


def _extract_identifiers(nl: str) -> list[str]:
    found = []

    for value in _QUOTED_RX.findall(nl):
        value = value.strip()
        if value:
            found.append(value)

    for m in _KEY_RX.finditer(nl.upper()):
        token = m.group(1)
        if token in {
            "AND", "THE", "FOR", "ALL", "OLD", "NEW", "BASE", "FROM",
            "WITH", "PDF", "ROW", "CODE", "TABLE", "VALUE", "VALUES",
            "PREVIOUS", "CURRENT", "TARGET", "LATEST", "REVISED",
        }:
            continue
        # Ignore standalone years as row IDs when there are other identifiers.
        found.append(token)

    for m in _NUMBER_RX.finditer(nl):
        found.append(m.group(0))

    deduped = []
    for item in found:
        clean = item.strip()
        if clean and clean not in deduped:
            deduped.append(clean)

    non_year = [x for x in deduped if not re.fullmatch(r"(?:19|20)\d{2}", x)]
    return non_year or deduped


def _split_side_identifiers(nl: str, identifiers: list[str]) -> tuple[Optional[str], Optional[str]]:
    if not identifiers:
        return None, None

    q = nl.lower()

    if len(identifiers) == 1:
        return identifiers[0], identifiers[0]

    old_positions = [
        q.find("old"),
        q.find("previous"),
        q.find("base"),
        q.find("baseline"),
    ]
    new_positions = [
        q.find("new"),
        q.find("current"),
        q.find("target"),
        q.find("revised"),
        q.find("latest"),
    ]

    old_positions = [p for p in old_positions if p >= 0]
    new_positions = [p for p in new_positions if p >= 0]

    if old_positions and new_positions:
        old_pos = min(old_positions)
        new_pos = min(new_positions)

        scored = []
        for ident in identifiers:
            pos = q.find(ident.lower())
            if pos < 0:
                continue
            scored.append((ident, abs(pos - old_pos), abs(pos - new_pos)))

        if scored:
            base_ident = min(scored, key=lambda x: x[1])[0]
            target_ident = min(scored, key=lambda x: x[2])[0]
            if base_ident != target_ident:
                return base_ident, target_ident

    return identifiers[0], identifiers[1]


def _row_values(row: Block) -> dict[str, Any]:
    if not isinstance(row.payload, dict):
        return {}

    out = {}
    for key, value in row.payload.items():
        key = str(key)
        if key in _INTERNAL_TABLE_FIELDS or key.startswith("__"):
            continue
        out[key] = value

    return out


def _row_key(row: Block) -> str:
    if row.stable_key:
        return str(row.stable_key).strip()

    values = _row_values(row)
    for value in values.values():
        text = str(value or "").strip()
        if text:
            return text[:100]

    return _preview(row.text, 100) or ""


def _row_definition(row: Block) -> str:
    values = _row_values(row)
    parts = []

    for key, value in values.items():
        val = str(value or "").strip()
        if not val:
            continue
        if key.lower().startswith("col_"):
            parts.append(val)
        else:
            parts.append(f"{key}: {val}")
        if len(parts) >= 4:
            break

    if parts:
        return " | ".join(parts)

    return _preview(row.text, 260) or ""


def _row_context(row: Block, blocks: list[Block]) -> dict:
    table = next((b for b in blocks if b.id == row.parent_id), None)
    header = []
    table_id = None
    table_area = _path_label(row.path)

    if table and isinstance(table.payload, dict):
        table_id = str(table.id)
        header = [str(h or "").strip() for h in table.payload.get("header", [])]
        table_area = _path_label(table.path)

    return {
        "table_id": table_id,
        "table_header": header,
        "table_area": table_area,
    }


def _row_record(row: Block, side: str, blocks: list[Block], score: float = 1.0) -> dict:
    context = _row_context(row, blocks)
    values = _row_values(row)

    return {
        "type": "table_row",
        "side": side,
        "change_type": "MATCH",
        "stable_key": row.stable_key,
        "row_key": _row_key(row),
        "definition": _row_definition(row),
        "block_type": row.block_type.value,
        "path": row.path,
        "area": context["table_area"],
        "page": row.page_number,
        "values": values,
        "before": _preview(row.text, 500) if side == "base" else None,
        "after": _preview(row.text, 500) if side == "target" else None,
        "confidence": round(score, 2),
        "citation": f"{side} p.{row.page_number} - {context['table_area']} - key {_row_key(row)}",
        "table_id": context["table_id"],
        "table_header": context["table_header"],
    }


def _find_rows(blocks: list[Block], query_key: str, limit: int = 8) -> list[tuple[float, Block]]:
    q = _norm(query_key)
    if not q:
        return []

    scored = []

    for row in blocks:
        if row.block_type.value != "table_row":
            continue

        values = _row_values(row)
        key = _norm(_row_key(row))
        stable = _norm(row.stable_key)
        definition = _norm(_row_definition(row))
        text = _norm(row.text)
        values_text = _norm(" ".join(str(v or "") for v in values.values()))

        exact_score = 0.0
        if q in {key, stable}:
            exact_score = 1.0
        elif q and (q in key or q in stable):
            exact_score = 0.94
        elif q and q in values_text:
            exact_score = 0.88

        fuzzy_score = max(
            fuzz.partial_ratio(q, key) / 100.0,
            fuzz.partial_ratio(q, stable) / 100.0,
            fuzz.partial_ratio(q, definition) / 100.0,
            fuzz.partial_ratio(q, text) / 100.0,
            fuzz.partial_ratio(q, values_text) / 100.0,
        )

        score = max(exact_score, fuzzy_score)
        if score >= 0.62:
            scored.append((score, row))

    scored.sort(key=lambda item: (-item[0], item[1].page_number, item[1].sequence))
    return scored[:limit]


def _align_columns(base_values: dict[str, Any], target_values: dict[str, Any]) -> list[dict]:
    target_cols = list(target_values.keys())
    used_target = set()
    alignment = []

    for base_col in base_values.keys():
        best_col = None
        best_score = 0.0

        for target_col in target_cols:
            if target_col in used_target:
                continue

            score = fuzz.ratio(_norm(base_col), _norm(target_col)) / 100.0
            if score > best_score:
                best_score = score
                best_col = target_col

        if best_col is not None and best_score >= 0.55:
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
            alignment.append(
                {
                    "base_col": base_col,
                    "target_col": None,
                    "score": 0.0,
                    "status": "base_only",
                }
            )

    for target_col in target_cols:
        if target_col not in used_target:
            alignment.append(
                {
                    "base_col": None,
                    "target_col": target_col,
                    "score": 0.0,
                    "status": "target_only",
                }
            )

    return alignment


def _compare_table_rows(base_row: Block, target_row: Block, base_blocks: list[Block], target_blocks: list[Block]) -> dict:
    base_values = _row_values(base_row)
    target_values = _row_values(target_row)
    alignment = _align_columns(base_values, target_values)

    field_changes = []

    for item in alignment:
        base_col = item.get("base_col")
        target_col = item.get("target_col")

        if base_col and target_col:
            before = base_values.get(base_col)
            after = target_values.get(target_col)
            if _norm(before) != _norm(after):
                field_changes.append(
                    {
                        "field": base_col if base_col == target_col else f"{base_col} -> {target_col}",
                        "before": before,
                        "after": after,
                        "change_type": "MODIFIED",
                    }
                )
        elif base_col:
            before = base_values.get(base_col)
            if _norm(before):
                field_changes.append(
                    {
                        "field": base_col,
                        "before": before,
                        "after": None,
                        "change_type": "DELETED",
                    }
                )
        elif target_col:
            after = target_values.get(target_col)
            if _norm(after):
                field_changes.append(
                    {
                        "field": target_col,
                        "before": None,
                        "after": after,
                        "change_type": "ADDED",
                    }
                )

    base_ctx = _row_context(base_row, base_blocks)
    target_ctx = _row_context(target_row, target_blocks)

    if field_changes:
        change_type = "MODIFIED"
    else:
        change_type = "UNCHANGED"

    return {
        "type": "table_row_comparison",
        "change_type": change_type,
        "stable_key": base_row.stable_key or target_row.stable_key,
        "row_key": f"{_row_key(base_row)} -> {_row_key(target_row)}",
        "block_type": "table_row",
        "area": f"{base_ctx['table_area']} -> {target_ctx['table_area']}",
        "path": base_row.path,
        "page_base": base_row.page_number,
        "page_target": target_row.page_number,
        "before": _preview(base_row.text, 500),
        "after": _preview(target_row.text, 500),
        "definition": {
            "base": _row_definition(base_row),
            "target": _row_definition(target_row),
        },
        "values": {
            "base": base_values,
            "target": target_values,
        },
        "field_changes": field_changes,
        "column_alignment": alignment,
        "impact": 0.85 if field_changes else 0.15,
        "confidence": 0.88,
        "citation": f"base p.{base_row.page_number} -> target p.{target_row.page_number} - {base_ctx['table_area']} -> {target_ctx['table_area']}",
        "base_table_id": base_ctx["table_id"],
        "target_table_id": target_ctx["table_id"],
    }


def _table_query_answer(nl: str, base_blocks: list[Block], target_blocks: list[Block]) -> Optional[dict]:
    if not _is_table_intent(nl):
        return None

    identifiers = _extract_identifiers(nl)
    if not identifiers:
        return None

    base_key, target_key = _split_side_identifiers(nl, identifiers)

    base_matches = _find_rows(base_blocks, base_key or identifiers[0])
    target_matches = _find_rows(target_blocks, target_key or base_key or identifiers[0])

    rows = []
    plan = {
        "intent": "table_row_query",
        "filters": {
            "base_row_key": base_key,
            "target_row_key": target_key,
            "identifiers": identifiers,
            "text": nl,
        },
        "granularity": "table_row_values",
    }

    if base_matches and target_matches:
        base_score, base_row = base_matches[0]
        target_score, target_row = target_matches[0]
        comparison = _compare_table_rows(base_row, target_row, base_blocks, target_blocks)
        comparison["confidence"] = round(min(base_score, target_score, comparison["confidence"]), 2)
        rows.append(comparison)

        if comparison["change_type"] == "UNCHANGED":
            answer = (
                f"I found matching table rows for {base_key} and {target_key}. "
                "The aligned row values are equivalent based on extracted table cells."
            )
        else:
            answer = (
                f"I compared table row {base_key} from the baseline with {target_key} from the revised document. "
                f"I found {len(comparison['field_changes'])} cell-level difference"
                f"{'' if len(comparison['field_changes']) == 1 else 's'}."
            )

        return {
            "answer": answer,
            "rows": rows,
            "count": len(rows),
            "plan": plan,
        }

    for score, row in base_matches:
        rows.append(_row_record(row, "base", base_blocks, score=score))

    for score, row in target_matches:
        rows.append(_row_record(row, "target", target_blocks, score=score))

    if rows:
        found_sides = sorted({r["side"] for r in rows if r.get("side")})
        answer = (
            f"I found {len(rows)} table row match{'es' if len(rows) != 1 else ''} "
            f"for {', '.join(identifiers)} in the {' and '.join(found_sides)} document side"
            f"{'' if len(found_sides) == 1 else 's'}."
        )
    else:
        answer = (
            "I could not find a matching table row for that identifier in the extracted tables. "
            "Try using the exact row code, part number, PCB number, or a phrase from the row."
        )

    return {
        "answer": answer,
        "rows": rows[:50],
        "count": len(rows),
        "plan": plan,
    }


# ---------------- general diff query ----------------

def parse_query(nl: str) -> dict:
    nl_low = nl.lower()

    change_types: list[str] = []
    for ct, words in INTENT_KEYWORDS.items():
        if any(w in nl_low for w in words):
            change_types.append(ct)

    if _is_summary_intent(nl) or any(term in nl_low for term in ("key change", "what changed", "what is changed", "difference", "differences")):
        change_types = ["ADDED", "DELETED", "MODIFIED"]
    elif not change_types:
        change_types = ["ADDED", "DELETED", "MODIFIED"]

    sections: list[str] = []
    for term, paths in KNOWN_SECTIONS.items():
        if term in nl_low:
            sections.extend(paths)

    categories: list[str] = []
    for cat, words in CATEGORY_KEYWORDS.items():
        if any(w in nl_low for w in words):
            categories.append(cat)

    codes: list[str] = []
    for m in _KEY_RX.finditer(nl.upper()):
        token = m.group(1)
        if token in {
            "AND", "THE", "FOR", "ALL", "OR", "NEW", "OLD", "BASE",
            "PDF", "ROW", "CODE", "TABLE", "VALUE", "VALUES",
        }:
            continue
        codes.append(token)

    return {
        "intent": "diff",
        "filters": {
            "change_type": list(dict.fromkeys(change_types)),
            "section": list(dict.fromkeys(sections)),
            "stable_key": list(dict.fromkeys(codes)),
            "category": list(dict.fromkeys(categories)),
            "text": nl,
        },
        "granularity": "business_answer",
    }


def _is_summary_intent(nl: str) -> bool:
    q = _norm(nl)
    return any(term in q for term in SUMMARY_TERMS)


def _is_feature_review_table_intent(nl: str) -> bool:
    q = _norm(nl)
    if "seek clarification" in q:
        return True
    if "feature" in q and "change" in q:
        return True
    if "review" in q and "table" in q:
        return True
    return all(term in q for term in FEATURE_TABLE_TERMS[:2])


def _broad_summary_plan(nl: str) -> dict:
    return {
        "intent": "summary",
        "filters": {
            "change_type": ["ADDED", "DELETED", "MODIFIED"],
            "section": [],
            "stable_key": [],
            "category": [],
            "text": "",
            "original_question": nl,
        },
        "granularity": "business_summary",
    }


def execute_plan(
    plan: dict,
    diffs: list[BlockDiff],
    base_blocks: list[Block],
    target_blocks: list[Block],
) -> list[dict]:
    base_by_id = {b.id: b for b in base_blocks}
    tgt_by_id = {b.id: b for b in target_blocks}

    filters = plan.get("filters", {})
    want_changes = set(filters.get("change_type") or ["ADDED", "DELETED", "MODIFIED"])
    want_sections = [s.lower() for s in filters.get("section", [])]
    want_keys = [k.upper() for k in filters.get("stable_key", [])]
    want_categories = [c.lower() for c in filters.get("category", [])]
    query_text = _norm(filters.get("text", ""))

    query_terms = [
        t for t in re.findall(r"[a-zA-Z0-9]+", query_text)
        if len(t) > 2 and t not in STOP_QUERY_TERMS
    ]

    results: list[dict] = []

    for d in diffs:
        if d.change_type.value not in want_changes:
            continue

        b = base_by_id.get(d.base_block_id) if d.base_block_id else None
        t = tgt_by_id.get(d.target_block_id) if d.target_block_id else None
        block = b or t

        if not block:
            continue

        before = _preview(b.text if b else None)
        after = _preview(t.text if t else None)
        combined_text = _diff_search_text(d, b, t, block)
        combined_low = _norm(combined_text)
        path_low = _norm(block.path)
        key_up = (block.stable_key or "").upper()
        category = _infer_category(combined_text)

        if want_sections and not any(s in path_low or s in combined_low for s in want_sections):
            continue
        if want_keys and key_up not in want_keys and not any(k.lower() in combined_low for k in want_keys):
            continue
        if want_categories and category not in want_categories:
            continue

        has_explicit_filter = bool(want_sections or want_keys or want_categories)
        if not has_explicit_filter and query_terms:
            if not any(term in combined_low for term in query_terms):
                fuzzy = fuzz.partial_ratio(query_text, combined_low) / 100.0 if combined_low else 0.0
                if len(query_terms) > 2 and fuzzy < 0.52:
                    continue

        confidence = _confidence(d, block)

        row = {
            "type": "diff",
            "change_type": d.change_type.value,
            "stable_key": block.stable_key,
            "block_type": block.block_type.value,
            "path": block.path,
            "area": _path_label(block.path),
            "category": category,
            "page": block.page_number,
            "page_base": b.page_number if b else None,
            "page_target": t.page_number if t else None,
            "before": before,
            "after": after,
            "field_changes": [
                {"field": fd.field, "before": fd.before, "after": fd.after}
                for fd in d.field_diffs
            ],
            "impact": d.impact_score,
            "confidence": confidence,
            "citation": _citation(b, t, block),
        }

        if block.block_type.value == "table_row":
            row["values"] = {
                "base": _row_values(b) if b else None,
                "target": _row_values(t) if t else None,
            }
            row["definition"] = {
                "base": _row_definition(b) if b else None,
                "target": _row_definition(t) if t else None,
            }

        results.append(row)

    results.sort(key=lambda r: (-r["impact"], -r["confidence"], r["change_type"]))
    return results


PLAN_PROMPT = """\
Translate the user's document-comparison question into a query plan.
Output strict JSON only.

Schema:
{
  "intent": "diff",
  "filters": {
    "change_type": ["ADDED"|"DELETED"|"MODIFIED"],
    "section": [string],
    "stable_key": [string],
    "category": [string],
    "text": string
  },
  "granularity": "business_answer"
}

Question: {question}
"""


def llm_plan(nl: str) -> Optional[dict]:
    config = _openai_config()
    endpoint = _env_first(AI_ENV_NAMES["endpoint"])
    api_key = _env_first(AI_ENV_NAMES["api_key"])
    deploy = str(config.get("deployment") or "")

    if not config["configured"]:
        return None

    try:
        from openai import AzureOpenAI

        client = AzureOpenAI(
            api_key=api_key,
            azure_endpoint=endpoint,
            api_version=str(config.get("api_version") or "2024-08-01-preview"),
        )
        resp = client.chat.completions.create(
            model=deploy,
            messages=[
                {"role": "system", "content": "Output JSON only."},
                {"role": "user", "content": PLAN_PROMPT.format(question=nl)},
            ],
            temperature=0.0,
            response_format={"type": "json_object"},
        )
        data = json.loads(resp.choices[0].message.content or "{}")
        if isinstance(data, dict):
            data.setdefault("filters", {})
            data["filters"]["text"] = nl
        return data
    except Exception:
        return None


def _semantic_search(nl: str, db_run_id: Optional[str], limit: int = 48) -> list[dict]:
    if not db_run_id or not db_enabled():
        return []

    try:
        vector = vector_literal(embed_query(nl))
    except Exception:
        return []

    if not vector:
        return []

    try:
        with get_conn() as conn:
            found = conn.execute(
                """
                SELECT
                    b.id,
                    b.block_type,
                    b.path,
                    b.stable_key,
                    b.page_number,
                    b.text,
                    b.payload,
                    d.label AS document_label,
                    CASE
                        WHEN b.document_id = cr.base_doc_id THEN 'base'
                        WHEN b.document_id = cr.target_doc_id THEN 'target'
                        ELSE 'document'
                    END AS side,
                    1 - (b.embedding <=> %s::vector) AS similarity
                FROM comparison_run cr
                JOIN doc_block b
                  ON b.document_id IN (cr.base_doc_id, cr.target_doc_id)
                JOIN spec_document d
                  ON d.id = b.document_id
                WHERE cr.id = %s
                  AND b.embedding IS NOT NULL
                ORDER BY b.embedding <=> %s::vector
                LIMIT %s
                """,
                (vector, db_run_id, vector, limit),
            ).fetchall()
    except Exception:
        return []

    rows = []
    for row in found:
        side = row.get("side") or "document"
        similarity = float(row.get("similarity") or 0)
        text = _preview(row.get("text"), 520)
        path = row.get("path")

        rows.append(
            {
                "type": "semantic_match",
                "source": "pgvector",
                "change_type": "MATCH",
                "stable_key": row.get("stable_key"),
                "block_type": row.get("block_type"),
                "path": path,
                "area": _path_label(path),
                "category": _infer_category(text or path or ""),
                "page": row.get("page_number"),
                "side": side,
                "before": text if side == "base" else None,
                "after": text if side == "target" else None,
                "text": text,
                "impact": similarity,
                "confidence": round(max(0.35, min(0.98, similarity)), 2),
                "citation": f"{side} p.{row.get('page_number')} - {_path_label(path)}",
                "payload": row.get("payload") if isinstance(row.get("payload"), dict) else {},
            }
        )

    return rows


def _merge_rows(primary: list[dict], semantic: list[dict], limit: int = 200) -> list[dict]:
    seen = set()
    merged = []

    for row in primary + semantic:
        key = (
            row.get("type"),
            row.get("change_type"),
            row.get("citation"),
            row.get("before"),
            row.get("after"),
            row.get("text"),
        )
        if key in seen:
            continue
        seen.add(key)
        merged.append(row)

    return merged[:limit]


def _merge_many_rows(*row_sets: list[dict], limit: int = 400) -> list[dict]:
    seen = set()
    merged = []

    for rows in row_sets:
        for row in rows or []:
            key = (
                row.get("type"),
                row.get("change_type"),
                row.get("citation"),
                row.get("row_key"),
                row.get("stable_key"),
                row.get("before"),
                row.get("after"),
                row.get("text"),
            )
            if key in seen:
                continue
            seen.add(key)
            merged.append(row)
            if len(merged) >= limit:
                return merged

    return merged


def _identifier_terms(nl: str) -> list[str]:
    terms = []
    terms.extend(_extract_identifiers(nl))

    for token in re.findall(r"[\w\-]{3,80}", str(nl or ""), flags=re.UNICODE):
        low = _norm(token)
        if low and low not in STOP_QUERY_TERMS and (low in IDENTIFIER_CONTEXT_TERMS or re.search(r"\d", low)):
            terms.append(token)

    out = []
    for term in terms:
        clean = str(term or "").strip()
        if clean and clean.lower() not in {x.lower() for x in out}:
            out.append(clean)
    return out


def _row_contains_any(row: dict, terms: list[str]) -> bool:
    if not terms:
        return False

    text_parts = [
        row.get("stable_key"),
        row.get("row_key"),
        row.get("area"),
        row.get("path"),
        row.get("before"),
        row.get("after"),
        row.get("text"),
        row.get("definition"),
        row.get("values"),
        row.get("field_changes"),
    ]
    haystack = _norm(json.dumps(text_parts, ensure_ascii=False, default=str))
    return any(_norm(term) and _norm(term) in haystack for term in terms)


def _focused_identifier_rows(rows: list[dict], nl: str, limit: int = 90) -> list[dict]:
    terms = _identifier_terms(nl)
    if not terms:
        return []

    exact = [row for row in rows if _row_contains_any(row, terms)]
    exact.sort(
        key=lambda r: (
            0 if str(r.get("block_type") or "").lower() == "table_row" else 1,
            -float(r.get("impact") or 0),
            -float(r.get("confidence") or 0),
        )
    )
    return exact[:limit]


def _ai_evidence_limit(nl: str, rows: list[dict]) -> int:
    if _is_table_intent(nl) or _identifier_terms(nl):
        return 90
    if _is_summary_intent(nl) or _is_feature_review_table_intent(nl):
        return 100 if len(rows) > 120 else 70
    if _wants_table_output(nl):
        return 80
    return 45


def _count_changes(rows: list[dict]) -> dict[str, int]:
    counts = {"ADDED": 0, "DELETED": 0, "MODIFIED": 0, "UNCHANGED": 0, "MATCH": 0}
    for row in rows:
        change_type = str(row.get("change_type") or "").upper()
        if change_type in counts:
            counts[change_type] += 1
    return counts


def _human_change(row: dict, limit: int = 280) -> str:
    change_type = str(row.get("change_type") or "").upper()
    before = _preview(row.get("before"), limit)
    after = _preview(row.get("after"), limit)
    field_changes = row.get("field_changes") or []

    if field_changes:
        pieces = []
        for fd in field_changes[:4]:
            field = fd.get("field") or fd.get("column") or "value"
            old = _preview(fd.get("before"), 90)
            new = _preview(fd.get("after"), 90)
            if old and new:
                pieces.append(f"{field}: {old} -> {new}")
            elif new:
                pieces.append(f"{field}: added {new}")
            elif old:
                pieces.append(f"{field}: removed {old}")
        if pieces:
            return "; ".join(pieces)

    if change_type == "ADDED":
        return f"Added: {after or row.get('text') or 'new content'}"
    if change_type == "DELETED":
        return f"Removed: {before or row.get('text') or 'previous content'}"
    if change_type == "MODIFIED":
        if before and after:
            return f"{before} -> {after}"
        return f"Modified: {after or before or row.get('text') or 'content changed'}"

    if row.get("text"):
        return _preview(row.get("text"), limit) or "-"
    return after or before or "-"


def _feature_label(row: dict) -> str:
    stable = row.get("stable_key")
    definition = row.get("definition")

    if isinstance(definition, dict):
        value = definition.get("target") or definition.get("base")
        if value:
            return _preview(value, 150) or str(value)
    elif definition:
        return _preview(definition, 150) or str(definition)

    area = row.get("area") or _path_label(row.get("path"))
    if stable:
        return f"{area} - {stable}"
    return area


def _seek_clarification(row: dict) -> str:
    category = str(row.get("category") or "").lower()
    change_type = str(row.get("change_type") or "").upper()
    confidence = float(row.get("confidence") or 0)

    if category == "dates":
        return "Confirm the effective date/timing and whether downstream milestones or releases change."
    if category == "pricing":
        return "Confirm commercial impact, approval need, and whether pricing communication is required."
    if category == "availability":
        return "Confirm affected variants/packages and whether availability changed from optional, standard, or unavailable."
    if category == "requirement":
        return "Confirm whether this creates a new mandatory requirement, dependency, or compliance impact."
    if category == "legal":
        return "Confirm interpretation with the owning legal/business reviewer."
    if change_type == "ADDED":
        return "Confirm whether this newly added item is applicable, approved, and communicated to impacted teams."
    if change_type == "DELETED":
        return "Confirm whether this removed item is intentionally discontinued and whether any dependency remains."
    if confidence and confidence < 0.72:
        return "Confirm manually because the extracted match confidence is lower."
    return "Confirm business impact and whether follow-up action is needed."


def _business_row(row: dict, feature_mode: bool = False) -> dict:
    confidence = row.get("confidence")
    if isinstance(confidence, (int, float)):
        confidence_text = f"{round((confidence if confidence <= 1 else confidence / 100) * 100)}%"
    else:
        confidence_text = "-"

    if feature_mode:
        return {
            "feature": _feature_label(row),
            "change": _human_change(row),
            "seek_clarification": _seek_clarification(row),
            "citation": row.get("citation") or f"page {row.get('page') or row.get('page_base') or '-'}",
            "confidence": confidence_text,
        }

    return {
        "area": row.get("area") or _path_label(row.get("path")),
        "change_type": str(row.get("change_type") or "-").upper(),
        "change": _human_change(row),
        "evidence": row.get("citation") or f"page {row.get('page') or row.get('page_base') or '-'}",
        "confidence": confidence_text,
        "review": _seek_clarification(row),
    }


def _priority_rows(rows: list[dict], limit: int = 12) -> list[dict]:
    def score(row: dict) -> tuple:
        category = str(row.get("category") or "")
        change_type = str(row.get("change_type") or "")
        impact = float(row.get("impact") or 0)
        confidence = float(row.get("confidence") or 0)
        category_weight = 1 if category in {"dates", "pricing", "availability", "requirement", "legal"} else 0
        type_weight = 1 if change_type in {"ADDED", "DELETED"} else 0
        return (category_weight, type_weight, impact, confidence)

    selected = []
    seen = set()
    for row in sorted(rows, key=score, reverse=True):
        key = (row.get("change_type"), row.get("area"), row.get("before"), row.get("after"), row.get("citation"))
        if key in seen:
            continue
        seen.add(key)
        selected.append(row)
        if len(selected) >= limit:
            break

    return selected


def _summary_answer(question: str, rows: list[dict], selected: list[dict]) -> str:
    if not rows:
        return "I could not find extracted changes to summarize for this comparison."

    counts = _count_changes(rows)
    categories: dict[str, int] = {}
    for row in rows:
        category = str(row.get("category") or "other")
        categories[category] = categories.get(category, 0) + 1

    main_themes = [
        f"{name} ({count})"
        for name, count in sorted(categories.items(), key=lambda kv: -kv[1])
        if name != "other"
    ][:4]

    parts = [
        (
            f"I found {len(rows)} meaningful extracted change{'s' if len(rows) != 1 else ''}: "
            f"{counts['ADDED']} added, {counts['DELETED']} deleted, and {counts['MODIFIED']} modified."
        )
    ]

    if main_themes:
        parts.append("The main themes are " + ", ".join(main_themes) + ".")

    examples = []
    for idx, row in enumerate(selected[:4], start=1):
        feature = _feature_label(row)
        change = _human_change(row, 150)
        citation = row.get("citation") or ""
        examples.append(f"{idx}. {feature}: {change}" + (f" ({citation})" if citation else ""))

    if examples:
        parts.append("Top review items: " + " ".join(examples))

    if "short" in _norm(question) or "brief" in _norm(question):
        return " ".join(parts[:2])

    parts.append("The table below lists the highest-priority items with citations and clarification prompts.")
    return " ".join(parts)


def _summary_response(question: str, rows: list[dict], plan: dict, semantic_rows: list[dict], allow_llm: bool = False) -> dict:
    feature_mode = _is_feature_review_table_intent(question)
    selected = _priority_rows(rows, limit=20 if feature_mode else 12)
    business_rows = [_business_row(row, feature_mode=feature_mode) for row in selected]
    columns = (
        ["feature", "change", "seek_clarification", "citation", "confidence"]
        if feature_mode
        else ["area", "change_type", "change", "evidence", "confidence", "review"]
    )

    return {
        "answer": (llm_answer(question, selected) if allow_llm else None) or _summary_answer(question, rows, selected),
        "columns": columns,
        "rows": business_rows,
        "count": len(rows),
        "plan": plan,
        "semantic_matches": len(semantic_rows),
        "presentation": "feature_review_table" if feature_mode else "business_summary",
    }


def _wants_table_output(nl: str) -> bool:
    q = _norm(nl)
    return (
        "table" in q
        or "tabular" in q
        or "feature, change" in q
        or "feature change" in q
        or "seek clarification" in q
        or ("columns" in q and "rows" in q)
    )


def _is_out_of_scope_question(nl: str) -> bool:
    q = _norm(nl)
    if not q:
        return True

    casual_exact = {
        "hi",
        "hello",
        "hey",
        "how are you",
        "how r u",
        "who are you",
        "what can you do",
        "thank you",
        "thanks",
        "ok",
        "okay",
    }
    if q in casual_exact:
        return True

    document_terms = (
        "change",
        "changed",
        "compare",
        "comparison",
        "summary",
        "summarize",
        "table",
        "row",
        "column",
        "cell",
        "feature",
        "clarification",
        "citation",
        "evidence",
        "added",
        "deleted",
        "removed",
        "modified",
        "baseline",
        "revised",
        "previous",
        "current",
        "old",
        "new",
        "document",
        "page",
        "section",
        "price",
        "date",
        "pcv",
        "pcb",
        "code",
        "part",
        "value",
    )
    identifier_like = bool(re.search(r"\b[A-Z]{1,8}[- ]?\d{2,12}[A-Z]?\b|\b\d{4,}\b", nl, re.I))

    return not (identifier_like or any(term in q for term in document_terms))


def _compact_value(value: Any, limit: int = 420) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return _preview(value, limit)
    if isinstance(value, (int, float, bool)):
        return value
    if isinstance(value, list):
        compact = [_compact_value(v, max(80, limit // 3)) for v in value[:12]]
        if len(value) > 12:
            compact.append(f"... {len(value) - 12} more")
        return compact
    if isinstance(value, dict):
        out = {}
        for idx, (key, val) in enumerate(value.items()):
            if idx >= 18:
                out["__more__"] = f"{len(value) - 18} more fields"
                break
            out[str(key)[:80]] = _compact_value(val, max(80, limit // 3))
        return out
    return _preview(str(value), limit)


def _compact_field_changes(field_changes: Any, limit: int = 8) -> list[dict]:
    out = []
    if not isinstance(field_changes, list):
        return out

    for fd in field_changes[:limit]:
        if not isinstance(fd, dict):
            continue
        out.append(
            {
                "field": _preview(fd.get("field") or fd.get("column") or "value", 120),
                "before": _compact_value(fd.get("before"), 220),
                "after": _compact_value(fd.get("after"), 220),
            }
        )

    if len(field_changes) > limit:
        out.append({"field": "__more__", "before": None, "after": f"{len(field_changes) - limit} more cell changes"})

    return out


def _compact_evidence_row(row: dict, detail_limit: int = 420) -> dict:
    out = {
        "area": _preview(row.get("area") or _path_label(row.get("path")), 180),
        "change_type": row.get("change_type"),
        "category": row.get("category"),
        "before": _compact_value(row.get("before"), detail_limit),
        "after": _compact_value(row.get("after"), detail_limit),
        "change": _compact_value(_human_change(row, detail_limit), detail_limit),
        "citation": _preview(row.get("citation"), 180),
        "confidence": row.get("confidence"),
    }

    for key in ("row_key", "stable_key", "page_base", "page_target"):
        if row.get(key) is not None:
            out[key] = _compact_value(row.get(key), 120)

    field_changes = _compact_field_changes(row.get("field_changes") or row.get("cell_diffs"), limit=10)
    if field_changes:
        out["field_changes"] = field_changes

    values = row.get("values")
    if values:
        out["values"] = _compact_value(values, 260)

    table_header = row.get("table_header")
    if table_header:
        out["table_header"] = _compact_value(table_header, 180)

    column_alignment = row.get("column_alignment")
    if column_alignment:
        out["column_alignment"] = _compact_value(column_alignment, 260)

    definition = row.get("definition")
    if definition:
        out["definition"] = _compact_value(definition, 220)

    return {k: v for k, v in out.items() if v not in (None, "", [], {})}


def _json_char_len(value: Any) -> int:
    return len(json.dumps(value, ensure_ascii=False, default=str))


def _fit_evidence_budget(evidence: list[dict], budget_chars: int) -> list[dict]:
    fitted = []
    total = 2

    for row in evidence:
        row_size = _json_char_len(row) + 1
        if fitted and total + row_size > budget_chars:
            break
        if not fitted and row_size > budget_chars:
            fitted.append(_compact_evidence_row(row, detail_limit=160))
            break
        fitted.append(row)
        total += row_size

    return fitted


def _curated_ai_evidence(nl: str, rows: list[dict], semantic_rows: list[dict], limit: Optional[int] = None) -> list[dict]:
    limit = limit or _ai_evidence_limit(nl, rows)
    focused = _focused_identifier_rows(rows, nl, limit=max(40, min(100, limit // 2)))
    selected = _priority_rows(rows, limit=limit)
    merged = _merge_many_rows(focused, selected, semantic_rows, limit=limit)

    evidence = []
    for row in merged:
        evidence.append(_compact_evidence_row(row, detail_limit=520))

    return _fit_evidence_budget(evidence, AI_EVIDENCE_BUDGET_CHARS)


AI_REVIEW_PROMPT = """\
You are DocuLens AI Agent, an evidence-bound comparison assistant.

Hard rules:
- Use only the extracted comparison evidence below.
- Do not answer from general knowledge, assumptions, or outside context.
- Do not continue casual conversation. Only answer the user's comparison request.
- Do not mention internal paths, UUIDs, block IDs, model behavior, or implementation details.
- If the evidence is not enough, say exactly what cannot be confirmed and what the user should review.
- Prefer concise business language with specific before/after changes.
- Work across languages. If the source evidence or user question is Arabic or another language, preserve the meaning and answer in the user's language when clear; otherwise use clear English.
- For right-to-left text, preserve the original terms, numbers, units, dates, and names exactly as evidence shows them.
- Response language preference: {response_language}. If this is "source", keep the answer in the dominant language of the source evidence/question. If it is a specific language, answer in that language while preserving source names, numbers, codes, legal terms, and table values exactly.
- If the evidence contains multiple languages, do not discard either language. Keep original terms where they matter and explain the mismatch semantically.

Answer style:
- For the standard key-changes request, return a compact table only.
- If the user asks for "Feature, Change, Seek Clarification", return exactly three columns:
  ["Feature", "Change", "Seek Clarification"]
- Do not add citation/confidence columns unless the user explicitly asks for them.
- If the user asks for a short summary, return 3-5 direct bullets.
- If the user asks for a detailed summary, group by business area and include evidence references inside the text.
- If the user asks for a table, return rows and columns suitable for rendering in the UI.
- Each row should represent a meaningful business change, not every low-level text diff.
- "Seek Clarification" means the practical question a business user should ask the relevant team, supplier, legal, finance, engineering, or document owner before accepting the change. It is not a statement about AI uncertainty.
- Prioritize high-impact mismatches: changed numeric values, dates, obligations, availability/status, pricing/cost, requirements, exclusions, names, codes, table cell changes, added/deleted sections, and wording that changes meaning.
- When many changes exist, group or merge similar evidence into concise user-useful rows.
- If the question names a specific PCV, PCB, code, part number, row, column, or numeric identifier, make that identifier the center of the answer and include all available row/cell changes for it from the evidence.
- If the evidence contains table row values or column alignment, compare those cells directly instead of giving a generic document summary.

Question:
{question}

Evidence JSON:
{evidence}

Return strict JSON only with this schema:
{{
  "answer": "plain language answer",
  "columns": ["optional", "table", "columns"],
  "rows": [
    {{"column": "value"}}
  ],
  "confidence": 0.0
}}
"""


def llm_freeform_answer(
    nl: str,
    rows: list[dict],
    semantic_rows: list[dict],
    response_language: str = "source",
) -> tuple[Optional[dict], Optional[str]]:
    config = _openai_config()
    endpoint = _env_first(AI_ENV_NAMES["endpoint"])
    api_key = _env_first(AI_ENV_NAMES["api_key"])
    deploy = str(config.get("deployment") or "")

    if not config["configured"]:
        return None, "Azure OpenAI chat is not configured: missing " + ", ".join(config["missing"])

    evidence_rows = _curated_ai_evidence(nl, rows, semantic_rows)
    if not evidence_rows:
        return None, "No extracted/vector evidence was available to send to Azure OpenAI."

    try:
        from openai import AzureOpenAI

        client = AzureOpenAI(
            api_key=api_key,
            azure_endpoint=endpoint,
            api_version=str(config.get("api_version") or "2024-08-01-preview"),
        )

        def _send(evidence_payload: list[dict]):
            evidence = json.dumps(evidence_payload, ensure_ascii=False, default=str)
            return client.chat.completions.create(
                model=deploy,
                messages=[
                    {"role": "system", "content": "Return strict JSON only. Do not include markdown fences."},
                    {
                        "role": "user",
                        "content": AI_REVIEW_PROMPT.format(
                            question=nl,
                            evidence=evidence,
                            response_language=response_language or "source",
                        ),
                    },
                ],
                temperature=0.15,
                max_tokens=3500,
                response_format={"type": "json_object"},
            )

        try:
            resp = _send(evidence_rows)
        except Exception as first_exc:
            message = str(first_exc).lower()
            if "context" not in message and "maximum" not in message and "too many tokens" not in message and "400" not in message:
                raise
            evidence_rows = _fit_evidence_budget(
                [_compact_evidence_row(row, detail_limit=220) for row in evidence_rows],
                AI_EVIDENCE_RETRY_BUDGET_CHARS,
            )
            resp = _send(evidence_rows)

        data = json.loads(resp.choices[0].message.content or "{}")
    except Exception as exc:
        return None, f"Azure OpenAI chat call failed: {type(exc).__name__}: {exc}"

    if not isinstance(data, dict):
        return None, "Azure OpenAI returned a non-object response."

    answer = str(data.get("answer") or "").strip()
    llm_rows = data.get("rows") if isinstance(data.get("rows"), list) else []
    columns = data.get("columns") if isinstance(data.get("columns"), list) else []

    if not columns and llm_rows:
        columns = list(llm_rows[0].keys())[:8] if isinstance(llm_rows[0], dict) else []

    if not answer:
        answer = _summary_answer(nl, rows, _priority_rows(rows, limit=8))

    return {
        "answer": answer,
        "columns": [str(c) for c in columns],
        "rows": llm_rows[:80],
        "count": len(llm_rows),
        "confidence": data.get("confidence"),
        "presentation": "ai_table" if llm_rows or _wants_table_output(nl) else "ai_answer",
        "evidence_count": len(evidence_rows),
        "ai_deployment": deploy,
    }, None


ANSWER_PROMPT = """\
You are answering a business user's question about a PDF comparison.
Use only the supplied evidence. Keep the answer direct and cite pages.
If the user asks for a table, describe the rows/columns in a compact table-like format.

Question:
{question}

Evidence JSON:
{evidence}

Output strict JSON:
{{
  "answer": "business-facing answer with citations",
  "confidence": 0.0
}}
"""


def llm_answer(nl: str, rows: list[dict]) -> Optional[str]:
    config = _openai_config()
    endpoint = _env_first(AI_ENV_NAMES["endpoint"])
    api_key = _env_first(AI_ENV_NAMES["api_key"])
    deploy = str(config.get("deployment") or "")

    if not (config["configured"] and rows):
        return None

    try:
        from openai import AzureOpenAI

        client = AzureOpenAI(
            api_key=api_key,
            azure_endpoint=endpoint,
            api_version=str(config.get("api_version") or "2024-08-01-preview"),
        )
        evidence = json.dumps(rows[:24], ensure_ascii=False, default=str)
        resp = client.chat.completions.create(
            model=deploy,
            messages=[
                {"role": "system", "content": "Output JSON only."},
                {"role": "user", "content": ANSWER_PROMPT.format(question=nl, evidence=evidence)},
            ],
            temperature=0.1,
            response_format={"type": "json_object"},
        )
        data = json.loads(resp.choices[0].message.content or "{}")
        answer = data.get("answer") if isinstance(data, dict) else None
        return str(answer).strip() if answer else None
    except Exception:
        return None


def _build_answer(question: str, rows: list[dict], plan: dict) -> str:
    if not rows:
        return "I could not find matching changes for that question in the extracted comparison."

    if plan.get("intent") == "table_row_query":
        return f"I found {len(rows)} matching table row result{'s' if len(rows) != 1 else ''}."

    added = sum(1 for r in rows if r["change_type"] == "ADDED")
    deleted = sum(1 for r in rows if r["change_type"] == "DELETED")
    modified = sum(1 for r in rows if r["change_type"] == "MODIFIED")

    categories = {}
    for r in rows:
        categories[r["category"]] = categories.get(r["category"], 0) + 1

    top_categories = sorted(categories.items(), key=lambda kv: -kv[1])[:3]
    cat_text = ", ".join(f"{name} ({count})" for name, count in top_categories if name != "other")

    parts = [f"I found {len(rows)} matching change{'s' if len(rows) != 1 else ''}."]
    counts = []
    if added:
        counts.append(f"{added} added")
    if deleted:
        counts.append(f"{deleted} deleted")
    if modified:
        counts.append(f"{modified} modified")
    if counts:
        parts.append("Breakdown: " + ", ".join(counts) + ".")
    if cat_text:
        parts.append("Most relevant areas: " + cat_text + ".")

    top = rows[0]
    if top.get("citation"):
        parts.append(f"Top supporting citation: {top['citation']}.")

    return " ".join(parts)


def query(
    nl: str,
    diffs: list[BlockDiff],
    base_blocks: list[Block],
    target_blocks: list[Block],
    db_run_id: Optional[str] = None,
    mode: str = "fast",
    response_language: str = "source",
) -> dict:
    if _is_out_of_scope_question(nl):
        return {
            "answer": (
                "I can only answer questions about the uploaded document comparison. "
                "Ask about changes, evidence, tables, sections, pages, values, or summary."
            ),
            "rows": [],
            "count": 0,
            "plan": {"intent": "out_of_scope"},
            "semantic_matches": 0,
            "mode": _norm(mode) or "fast",
        }

    table_result = _table_query_answer(nl, base_blocks, target_blocks)
    normalized_mode = _norm(mode) or "fast"
    use_ai = normalized_mode in {"ai", "openai", "llm", "agent"}

    if table_result is not None and not use_ai:
        table_result["mode"] = "fast"
        return table_result

    is_summary = _is_summary_intent(nl) or _is_feature_review_table_intent(nl)
    plan = _broad_summary_plan(nl) if is_summary else parse_query(nl)
    rows = execute_plan(plan, diffs, base_blocks, target_blocks)
    semantic_rows = _semantic_search(nl, db_run_id)

    if not rows:
        llm = llm_plan(nl)
        if llm:
            plan = llm
            rows = execute_plan(plan, diffs, base_blocks, target_blocks)

    if not rows and is_summary:
        plan = _broad_summary_plan(nl)
        rows = execute_plan(plan, diffs, base_blocks, target_blocks)

    if not rows and not is_summary:
        relaxed_plan = json.loads(json.dumps(plan, ensure_ascii=False, default=str))
        relaxed_plan.setdefault("filters", {})
        relaxed_plan["filters"]["text"] = ""
        rows = execute_plan(relaxed_plan, diffs, base_blocks, target_blocks)
        if rows:
            plan = relaxed_plan

    table_rows = []
    if table_result is not None:
        table_rows = table_result.get("rows") or []

    rows = _merge_many_rows(table_rows, _focused_identifier_rows(rows, nl), rows, semantic_rows, limit=450)

    if use_ai:
        ai_result, ai_error = llm_freeform_answer(nl, rows, semantic_rows, response_language=response_language)
        if ai_result:
            ai_result.update(
                {
                    "mode": "ai",
                    "ai_called": True,
                    "ai_error": None,
                    "response_language": response_language,
                    "plan": plan,
                    "semantic_matches": len(semantic_rows),
                    "source_rows": len(rows),
                }
            )
            return ai_result

        fallback = _summary_response(nl, rows, plan, semantic_rows, allow_llm=False) if (is_summary or _wants_table_output(nl)) else {
            "answer": _build_answer(nl, rows, plan),
            "rows": rows[:80],
            "count": len(rows),
            "plan": plan,
            "semantic_matches": len(semantic_rows),
        }
        fallback["mode"] = "ai"
        fallback["ai_called"] = False
        fallback["ai_unavailable"] = True
        fallback["ai_error"] = ai_error or "Azure OpenAI did not return a usable response."
        fallback["answer"] = "AI Summarization is unavailable right now. I could not generate a model-assisted answer from the extracted evidence."
        fallback["response_language"] = response_language
        return fallback

    if is_summary:
        response = _summary_response(nl, rows, plan, semantic_rows, allow_llm=False)
        response["mode"] = "fast"
        return response

    answer = _build_answer(nl, rows, plan)

    return {
        "answer": answer,
        "rows": rows[:200],
        "count": len(rows),
        "plan": plan,
        "semantic_matches": len(semantic_rows),
        "mode": "fast",
    }
