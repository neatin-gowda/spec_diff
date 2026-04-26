"""
Natural-language query layer.

Goal:
Return useful business-facing answers over the comparison result, not just raw
ADDED / DELETED / MODIFIED rows.

The response shape is now:
{
  "answer": "...",
  "rows": [...],
  "count": 10,
  "plan": {...}
}

The frontend can show the answer first and supporting evidence below it.
"""
from __future__ import annotations

import json
import os
import re
from typing import Optional

from .models import Block, BlockDiff, ChangeType


_KEY_RX = re.compile(r"\b([A-Z0-9]{2,4}[A-Z]?)\b")

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
}


def _norm(s: str | None) -> str:
    return re.sub(r"\s+", " ", (s or "").lower()).strip()


def _preview(s: str | None, limit: int = 360) -> str | None:
    if not s:
        return None
    s = re.sub(r"\s+", " ", str(s)).strip()
    if len(s) <= limit:
        return s
    return s[: limit - 1].rstrip() + "…"


def _path_label(path: str | None) -> str:
    if not path:
        return "Document"
    parts = [p for p in path.split("/") if p]
    if not parts:
        return "Document"
    return " / ".join(p.replace("_", " ").title() for p in parts[:4])


def _citation(base: Block | None, target: Block | None, block: Block) -> str:
    if base and target:
        page_part = f"base p.{base.page_number} → target p.{target.page_number}"
    elif base:
        page_part = f"base p.{base.page_number}"
    elif target:
        page_part = f"target p.{target.page_number}"
    else:
        page_part = "page unknown"

    path = _path_label(block.path)
    key = f" · key {block.stable_key}" if block.stable_key else ""
    return f"{page_part} · {path}{key}"


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
    for m in _KEY_RX.finditer(nl):
        token = m.group(1)
        if token in {"AND", "THE", "FOR", "ALL", "OR", "NEW", "OLD"}:
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
        if len(t) > 2 and t not in {"what", "show", "list", "changed", "change", "added", "deleted", "removed"}
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
        combined_text = " ".join(x for x in [before, after, block.path, block.stable_key] if x)
        combined_low = _norm(combined_text)
        path_low = _norm(block.path)
        key_up = (block.stable_key or "").upper()
        category = _infer_category(combined_text)

        if want_sections and not any(s in path_low or s in combined_low for s in want_sections):
            continue
        if want_keys and key_up not in want_keys:
            continue
        if want_categories and category not in want_categories:
            continue

        # If no explicit section/category/key was detected, use broad term matching
        # so "what changed in warranty" or "what changed in towing" still works.
        has_explicit_filter = bool(want_sections or want_keys or want_categories)
        if not has_explicit_filter and query_terms:
            if not any(term in combined_low for term in query_terms):
                # Keep broad "what changed" queries broad.
                if len(query_terms) > 2:
                    continue

        confidence = _confidence(d, block)

        results.append({
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
        })

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
