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
  "plan": {...}
}
"""
from __future__ import annotations

import json
import os
import re
from typing import Any, Optional

from rapidfuzz import fuzz

from .models import Block, BlockDiff, ChangeType


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
    "table": ("table", "row", "column", "cell", "code", "pcb", "part", "item", "value"),
}

STOP_QUERY_TERMS = {
    "what", "show", "list", "changed", "change", "added", "deleted", "removed",
    "compare", "between", "from", "with", "against", "previous", "current",
    "target", "base", "baseline", "latest", "revised", "file", "document",
    "table", "row", "column", "value", "old", "new", "the", "and", "for",
}


def _norm(s: Any) -> str:
    return re.sub(r"\s+", " ", str(s or "").lower()).strip()


def _preview(s: Any, limit: int = 360) -> str | None:
    if s is None:
        return None
    text = re.sub(r"\s+", " ", str(s)).strip()
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
        "table", "row", "column", "cell", "code", "pcb", "part number",
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

    if not change_types:
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
        combined_text = " ".join(str(x) for x in [before, after, block.path, block.stable_key] if x)
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
                if len(query_terms) > 2:
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
    endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    api_key = os.getenv("AZURE_OPENAI_API_KEY")
    deploy = os.getenv("AZURE_OPENAI_DEPLOYMENT")

    if not (endpoint and api_key and deploy):
        return None

    try:
        from openai import AzureOpenAI

        client = AzureOpenAI(
            api_key=api_key,
            azure_endpoint=endpoint,
            api_version="2024-08-01-preview",
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
) -> dict:
    table_result = _table_query_answer(nl, base_blocks, target_blocks)
    if table_result is not None:
        return table_result

    plan = parse_query(nl)
    rows = execute_plan(plan, diffs, base_blocks, target_blocks)

    if not rows:
        llm = llm_plan(nl)
        if llm:
            plan = llm
            rows = execute_plan(plan, diffs, base_blocks, target_blocks)

    answer = _build_answer(nl, rows, plan)

    return {
        "answer": answer,
        "rows": rows[:200],
        "count": len(rows),
        "plan": plan,
    }
