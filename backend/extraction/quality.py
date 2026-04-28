"""
Local extraction quality scoring.

This is intentionally explainable. It creates actionable warnings that can be
shown in JSON/UI or stored for later template-learning, without calling AI.
"""
from __future__ import annotations

from collections import Counter
from typing import Any

from .schema import clean_text, value_type


def score_blocks(blocks: list[Any], coverage: float | None = None) -> dict[str, Any]:
    counts = Counter()
    text_chars = 0
    table_confidences = []
    warnings = []

    for block in blocks:
        block_type = getattr(getattr(block, "block_type", None), "value", getattr(block, "block_type", "unknown"))
        counts[block_type] += 1
        text_chars += len(clean_text(getattr(block, "text", "")))
        payload = getattr(block, "payload", {}) if isinstance(getattr(block, "payload", {}), dict) else {}
        intelligence = payload.get("extraction_intelligence") if isinstance(payload.get("extraction_intelligence"), dict) else {}

        if block_type == "table":
            table_quality = intelligence.get("table_quality", {})
            if isinstance(table_quality, dict) and table_quality.get("confidence") is not None:
                table_confidences.append(float(table_quality.get("confidence") or 0))
            for warning in table_quality.get("warnings", []) if isinstance(table_quality, dict) else []:
                warnings.append(
                    {
                        "level": "warn",
                        "page": getattr(block, "page_number", None),
                        "path": getattr(block, "path", None),
                        "message": warning,
                    }
                )

    coverage_score = 0.0 if coverage is None else max(0.0, min(1.0, float(coverage) / 100.0))
    structure_score = min(1.0, (counts.get("section", 0) * 0.08) + (counts.get("paragraph", 0) * 0.01) + (counts.get("table", 0) * 0.08))
    text_score = min(1.0, text_chars / 2500.0)
    table_score = sum(table_confidences) / len(table_confidences) if table_confidences else (0.72 if counts.get("table", 0) == 0 else 0.45)

    score = (coverage_score * 0.38) + (structure_score * 0.18) + (text_score * 0.16) + (table_score * 0.28)

    if coverage is not None and coverage < 70:
        warnings.append({"level": "warn", "message": "Extraction coverage is low; OCR or source conversion quality may need review."})
    if counts.get("table", 0) and not counts.get("table_row", 0):
        warnings.append({"level": "warn", "message": "Tables were detected but no table rows were extracted."})
    if text_chars < 100 and counts.get("figure", 0):
        warnings.append({"level": "warn", "message": "Document may be image-heavy; local OCR quality determines extraction accuracy."})

    return {
        "score": round(max(0.0, min(1.0, score)), 3),
        "grade": _grade(score),
        "counts": dict(counts),
        "text_characters": text_chars,
        "table_confidence_avg": round(table_score, 3),
        "warnings": warnings[:80],
    }


def score_table_values(rows: list[list[Any]]) -> dict[str, Any]:
    if not rows:
        return {"density": 0.0, "dominant_value_type": "blank", "confidence": 0.0}

    total = 0
    filled = 0
    types = Counter()

    for row in rows:
        for value in row:
            total += 1
            if clean_text(value):
                filled += 1
                types[value_type(value)] += 1

    density = filled / total if total else 0.0
    dominant = types.most_common(1)[0][0] if types else "blank"
    confidence = min(0.96, 0.35 + density * 0.45 + (0.16 if len(rows) >= 3 else 0))
    return {
        "density": round(density, 3),
        "dominant_value_type": dominant,
        "confidence": round(confidence, 3),
    }


def _grade(score: float) -> str:
    if score >= 0.86:
        return "high"
    if score >= 0.68:
        return "medium"
    return "needs_review"
