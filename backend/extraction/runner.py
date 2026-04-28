"""
Local extraction intelligence runner.

The runner enriches existing Block objects in-place. This keeps the current
diff/highlight behavior intact while adding reusable metadata for JSON,
queries, reports, template reuse, and future feedback loops.
"""
from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any

from .quality import score_blocks, score_table_values
from .registry import list_providers, local_provider_names, provider_for_kind
from .schema import (
    classify_field_label,
    classify_template,
    clean_text,
    detect_language_script,
    semantic_role,
    table_profile,
    value_type,
)


def enrich_blocks(
    blocks: list[Any],
    *,
    source_path: Path | None = None,
    source_format: str = "",
    document_label: str = "",
    coverage: float | None = None,
) -> list[Any]:
    """
    Add deterministic intelligence metadata to blocks.

    This function does not remove fields, alter visible text, or change bboxes.
    It only augments payloads, so existing comparison/coloring should continue
    to work exactly as before.
    """
    if not blocks:
        return blocks

    source_kind = source_format or (source_path.suffix.lower().lstrip(".") if source_path else "")
    template = classify_template(blocks, source_kind)
    tables_by_parent: dict[Any, list[Any]] = defaultdict(list)
    tables = []

    for block in blocks:
        block_type = getattr(getattr(block, "block_type", None), "value", getattr(block, "block_type", ""))
        if block_type == "table":
            tables.append(block)
        elif block_type == "table_row":
            tables_by_parent[getattr(block, "parent_id", None)].append(block)

    table_profiles: dict[Any, dict[str, Any]] = {}
    for table in tables:
        profile = table_profile(table, tables_by_parent.get(getattr(table, "id", None), []))
        table_profiles[getattr(table, "id", None)] = profile
        _attach_table_profile(table, profile, template, source_kind, document_label)

    for block in blocks:
        block_type = getattr(getattr(block, "block_type", None), "value", getattr(block, "block_type", ""))
        if block_type == "table":
            continue
        if block_type == "table_row":
            _attach_row_profile(block, table_profiles.get(getattr(block, "parent_id", None), {}), template)
        else:
            _attach_text_profile(block, template)

    quality = score_blocks(blocks, coverage=coverage)
    for block in blocks:
        payload = getattr(block, "payload", {}) if isinstance(getattr(block, "payload", {}), dict) else {}
        intel = payload.setdefault("extraction_intelligence", {})
        intel.setdefault("template", template)
        intel.setdefault("providers", local_provider_names())
        if block is blocks[0]:
            intel["document_quality"] = quality
        block.payload = payload

    return blocks


def extraction_intelligence_summary(
    blocks: list[Any],
    *,
    coverage: float | None = None,
    source_format: str = "",
) -> dict[str, Any]:
    template = classify_template(blocks, source_format)
    quality = score_blocks(blocks, coverage=coverage)
    table_summaries = []

    rows_by_parent: dict[Any, list[Any]] = defaultdict(list)
    for block in blocks:
        if getattr(getattr(block, "block_type", None), "value", getattr(block, "block_type", "")) == "table_row":
            rows_by_parent[getattr(block, "parent_id", None)].append(block)

    for block in blocks:
        if getattr(getattr(block, "block_type", None), "value", getattr(block, "block_type", "")) != "table":
            continue
        payload = getattr(block, "payload", {}) if isinstance(getattr(block, "payload", {}), dict) else {}
        intel = payload.get("extraction_intelligence") if isinstance(payload.get("extraction_intelligence"), dict) else {}
        table_quality = intel.get("table_quality") or table_profile(block, rows_by_parent.get(getattr(block, "id", None), []))
        table_summaries.append(
            {
                "id": str(getattr(block, "id", "")),
                "path": getattr(block, "path", ""),
                "page": getattr(block, "page_number", 1),
                "title": table_quality.get("title"),
                "fingerprint": table_quality.get("fingerprint", {}).get("fingerprint"),
                "confidence": table_quality.get("confidence"),
                "column_count": table_quality.get("column_count"),
                "row_count": table_quality.get("row_count"),
                "warnings": table_quality.get("warnings", []),
            }
        )

    return {
        "mode": "local_deterministic",
        "external_services_required": False,
        "providers": list_providers(),
        "template": template,
        "quality": quality,
        "tables": table_summaries[:200],
    }


def _attach_table_profile(table: Any, profile: dict[str, Any], template: dict[str, Any], source_kind: str, document_label: str) -> None:
    payload = getattr(table, "payload", {}) if isinstance(getattr(table, "payload", {}), dict) else {}
    intel = payload.setdefault("extraction_intelligence", {})

    intel["template"] = template
    intel["table_quality"] = profile
    intel["providers"] = [provider["name"] for provider in provider_for_kind(source_kind) if not provider["external_service"]]

    payload["table_title"] = payload.get("table_title") or profile.get("title") or clean_text(getattr(table, "text", "")) or f"Table on page {getattr(table, 'page_number', 1)}"
    payload["table_context"] = payload.get("table_context") or profile.get("context") or document_label
    payload["table_fingerprint"] = profile.get("fingerprint", {}).get("fingerprint")
    payload["column_profiles"] = profile.get("columns", [])
    payload["extraction_confidence"] = profile.get("confidence")
    payload["quality_warnings"] = profile.get("warnings", [])
    payload["language"] = detect_language_script(" ".join(payload.get("header", []) if isinstance(payload.get("header"), list) else []))
    table.payload = payload


def _attach_row_profile(row: Any, table_prof: dict[str, Any], template: dict[str, Any]) -> None:
    payload = getattr(row, "payload", {}) if isinstance(getattr(row, "payload", {}), dict) else {}
    column_profiles = table_prof.get("columns", []) if isinstance(table_prof, dict) else []
    field_profiles = []

    for col in column_profiles:
        name = col.get("name")
        if not name or str(name).startswith("__"):
            continue
        value = payload.get(name)
        field_profiles.append(
            {
                "field": name,
                "value_type": value_type(value),
                "semantic_role": col.get("semantic_role") or semantic_role(name, col.get("index", 0), [value]),
                "category": classify_field_label(name, value).get("category"),
            }
        )

    text = clean_text(getattr(row, "text", "") or " ".join(str(v or "") for v in payload.values()))
    intel = payload.setdefault("extraction_intelligence", {})
    intel["template"] = template
    intel["row_quality"] = {
        "field_profiles": field_profiles,
        "language": detect_language_script(text),
        "confidence": _row_confidence(payload, field_profiles),
    }
    payload["field_profiles"] = field_profiles
    row.payload = payload


def _attach_text_profile(block: Any, template: dict[str, Any]) -> None:
    payload = getattr(block, "payload", {}) if isinstance(getattr(block, "payload", {}), dict) else {}
    text = clean_text(getattr(block, "text", "") or payload.get("text") or payload.get("layout_text"))
    intel = payload.setdefault("extraction_intelligence", {})
    intel["template"] = template
    intel["text_quality"] = {
        "language": detect_language_script(text),
        "value_type": value_type(text),
        "looks_like_field": ":" in text or "：" in text,
    }
    block.payload = payload


def _row_confidence(payload: dict[str, Any], field_profiles: list[dict[str, Any]]) -> float:
    meaningful_values = [
        value
        for key, value in payload.items()
        if not str(key).startswith("__") and clean_text(value)
    ]
    density = len(meaningful_values) / max(1, len([k for k in payload.keys() if not str(k).startswith("__")]))
    role_bonus = 0.12 if any(item.get("semantic_role") in {"row_label", "code", "pcv"} for item in field_profiles) else 0.0
    return round(min(0.96, 0.48 + density * 0.34 + role_bonus), 3)
