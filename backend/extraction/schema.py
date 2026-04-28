"""
Shared schema and local classifiers for extraction intelligence.

Everything here is deterministic and service-free. The goal is not to replace
the existing PDF/Word/Excel extractors, but to make their output easier to
query, compare, score, and learn from over time.
"""
from __future__ import annotations

import hashlib
import re
from collections import Counter
from typing import Any


INTERNAL_PAYLOAD_KEYS = {
    "__anchors__",
    "__pages__",
    "__row_index__",
    "__table_title__",
    "__table_context__",
    "anchors",
    "page_width",
    "page_height",
    "source_extraction",
    "source_format",
    "visual_match_score",
    "visual_match_source",
}

ARABIC_RE = re.compile(r"[\u0600-\u06ff]")
LATIN_RE = re.compile(r"[A-Za-z]")
DEVANAGARI_RE = re.compile(r"[\u0900-\u097f]")
CJK_RE = re.compile(r"[\u4e00-\u9fff]")


def clean_text(value: Any) -> str:
    text = str(value or "").replace("\u00a0", " ")
    return re.sub(r"\s+", " ", text).strip()


def norm_text(value: Any) -> str:
    return clean_text(value).casefold()


def slug(value: Any, fallback: str = "item") -> str:
    text = re.sub(r"[^A-Za-z0-9]+", "_", str(value or "")).strip("_").lower()
    return text[:80] or fallback


def short_hash(value: Any, length: int = 12) -> str:
    text = clean_text(value)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:length]


def detect_language_script(text: Any) -> dict[str, Any]:
    raw = clean_text(text)
    if not raw:
        return {"script": "unknown", "direction": "ltr", "confidence": 0.0}

    counts = {
        "arabic": len(ARABIC_RE.findall(raw)),
        "latin": len(LATIN_RE.findall(raw)),
        "devanagari": len(DEVANAGARI_RE.findall(raw)),
        "cjk": len(CJK_RE.findall(raw)),
    }
    script, count = max(counts.items(), key=lambda item: item[1])
    total = sum(counts.values())

    if total == 0 or count == 0:
        return {"script": "unknown", "direction": "ltr", "confidence": 0.25}

    return {
        "script": script,
        "direction": "rtl" if script == "arabic" else "ltr",
        "confidence": round(count / total, 3),
        "mixed_scripts": [name for name, n in counts.items() if n and name != script],
    }


def value_type(value: Any) -> str:
    text = clean_text(value)
    low = text.lower()

    if not text:
        return "blank"
    if low in {"-", "--", "—", "–", ".", "•", "●", "○", "x", "s", "o", "m", "i", "yes", "no"}:
        return "symbol"
    if "$" in text or "€" in text or "£" in text or re.search(r"\b(?:usd|eur|gbp|inr|aed|cad)\b", low):
        return "currency"
    if re.fullmatch(r"\d{1,2}[/-]\d{1,2}[/-]\d{2,4}", text) or re.fullmatch(r"\d{4}-\d{1,2}-\d{1,2}", text):
        return "date"
    if re.fullmatch(r"[-+]?\d[\d,]*(?:\.\d+)?%", text):
        return "percentage"
    compact = re.sub(r"[\s,%(),.-]", "", text)
    if compact and compact.isdigit():
        return "number"
    if re.fullmatch(r"[A-Z]{1,10}[- ]?\d{1,12}[A-Z]?", text, re.I):
        return "code"
    return "text"


def semantic_role(header: Any, column_index: int, samples: list[Any] | None = None) -> str:
    low = norm_text(header)
    samples = samples or []
    sample_types = Counter(value_type(v) for v in samples if clean_text(v))
    common_type = sample_types.most_common(1)[0][0] if sample_types else ""

    if any(term in low for term in ("feature", "description", "item", "content", "name", "particular", "line item")):
        return "row_label"
    if any(term in low for term in ("order", "code", "part", "model", "sku", "ref", "reference", "id", "number", "no.")):
        return "code"
    if "pcv" in low or "pcb" in low:
        return "pcv"
    if any(term in low for term in ("price", "cost", "amount", "msrp", "fee", "value")) or common_type == "currency":
        return "amount"
    if any(term in low for term in ("date", "year", "month", "effective")) or common_type == "date":
        return "date"
    if any(term in low for term in ("status", "availability", "available", "standard", "optional", "included")):
        return "status"
    if any(term in low for term in ("qty", "quantity", "count", "units")):
        return "quantity"
    if common_type in {"symbol", "number", "percentage", "code"}:
        return common_type
    if column_index == 0:
        return "row_label"
    return "value"


def classify_field_label(label: Any, value: Any = "") -> dict[str, Any]:
    low = norm_text(label)
    val_type = value_type(value)

    categories = [
        ("identity", ("name", "id", "number", "code", "reference", "supplier", "customer", "vendor")),
        ("contact", ("address", "email", "phone", "mobile", "city", "country", "postal", "zip")),
        ("commercial", ("price", "cost", "amount", "total", "tax", "discount", "currency", "invoice")),
        ("quantity", ("qty", "quantity", "unit", "weight", "size", "dimension")),
        ("schedule", ("date", "time", "delivery", "effective", "expiry", "start", "end")),
        ("status", ("status", "approval", "available", "optional", "standard", "included")),
        ("legal", ("clause", "term", "condition", "liability", "warranty", "agreement")),
        ("product", ("feature", "model", "part", "item", "material", "color", "colour")),
    ]

    category = "general"
    for name, terms in categories:
        if any(term in low for term in terms):
            category = name
            break

    confidence = 0.62
    if category != "general":
        confidence += 0.2
    if val_type not in {"blank", "text"}:
        confidence += 0.08

    return {
        "label": clean_text(label),
        "category": category,
        "value_type": val_type,
        "confidence": round(min(0.96, confidence), 3),
    }


def header_fingerprint(columns: list[Any]) -> dict[str, Any]:
    normalized = [norm_text(c) for c in columns if clean_text(c)]
    generic_count = sum(1 for c in normalized if re.fullmatch(r"(col|column)\s*_?\d+", c))
    joined = " | ".join(normalized)
    token_counter = Counter()

    for column in normalized:
        for token in re.findall(r"[\w\u0600-\u06ff]+", column):
            if len(token) > 1:
                token_counter[token] += 1

    return {
        "fingerprint": short_hash(joined or "empty-header", 16),
        "normalized_headers": normalized,
        "token_signature": [token for token, _ in token_counter.most_common(12)],
        "generic_header_ratio": round(generic_count / max(1, len(columns)), 3),
    }


def classify_template(blocks: list[Any], source_format: str = "") -> dict[str, Any]:
    text_sample = " ".join(clean_text(getattr(block, "text", "")) for block in blocks[:250])
    payload_text = []
    table_count = 0
    row_count = 0

    for block in blocks:
        block_type = getattr(getattr(block, "block_type", None), "value", getattr(block, "block_type", ""))
        if block_type == "table":
            table_count += 1
        elif block_type == "table_row":
            row_count += 1
        payload = getattr(block, "payload", {}) if isinstance(getattr(block, "payload", {}), dict) else {}
        for key in ("table_title", "table_context", "sheet_name"):
            if payload.get(key):
                payload_text.append(str(payload.get(key)))

    corpus = norm_text(" ".join([text_sample] + payload_text))
    categories = [
        ("purchase_order", ("purchase order", "po number", "ship to", "bill to", "supplier", "quantity", "unit price")),
        ("invoice", ("invoice", "amount due", "tax", "subtotal", "payment terms")),
        ("vehicle_spec", ("engine", "transmission", "equipment", "model", "package", "pcv", "mpf")),
        ("financial_report", ("quarter", "revenue", "operating profit", "cash flow", "backlog")),
        ("legal_contract", ("agreement", "clause", "warranty", "liability", "termination", "shall")),
        ("catalog", ("sku", "product", "size", "color", "colour", "availability")),
        ("spreadsheet", ("sheet", "workbook")),
    ]

    scores = {}
    for name, terms in categories:
        scores[name] = sum(1 for term in terms if term in corpus)

    if source_format in {"spreadsheet", "xlsx", "xlsm", "xlsb", "xls", "csv", "tsv"}:
        scores["spreadsheet"] = scores.get("spreadsheet", 0) + 2

    best, score = max(scores.items(), key=lambda item: item[1]) if scores else ("generic_document", 0)
    if score == 0:
        best = "generic_document"

    language = detect_language_script(corpus)
    confidence = min(0.95, 0.45 + (score * 0.1) + (0.08 if table_count else 0) + (0.04 if row_count else 0))

    return {
        "template_type": best,
        "confidence": round(confidence, 3),
        "table_count": table_count,
        "table_row_count": row_count,
        "language": language,
        "signals": scores,
    }


def table_profile(table_block: Any, row_blocks: list[Any]) -> dict[str, Any]:
    payload = getattr(table_block, "payload", {}) if isinstance(getattr(table_block, "payload", {}), dict) else {}
    header = [clean_text(h) for h in payload.get("header", [])]
    rows = []

    if payload.get("rows"):
        rows = payload.get("rows") or []
    else:
        for row in row_blocks:
            row_payload = getattr(row, "payload", {}) if isinstance(getattr(row, "payload", {}), dict) else {}
            rows.append([row_payload.get(h, "") for h in header])

    samples_by_col: dict[int, list[Any]] = {idx: [] for idx in range(len(header))}
    for row in rows[:80]:
        if isinstance(row, dict):
            values = [row.get(h, "") for h in header]
        else:
            values = list(row)
        for idx, value in enumerate(values[: len(header)]):
            if clean_text(value) and len(samples_by_col.setdefault(idx, [])) < 12:
                samples_by_col[idx].append(value)

    columns = []
    for idx, name in enumerate(header):
        samples = samples_by_col.get(idx, [])
        types = Counter(value_type(v) for v in samples)
        columns.append(
            {
                "index": idx,
                "name": name or f"Column {idx + 1}",
                "normalized": norm_text(name or f"Column {idx + 1}"),
                "semantic_role": semantic_role(name, idx, samples),
                "value_type_hint": types.most_common(1)[0][0] if types else "blank",
                "samples": [clean_text(v) for v in samples[:6]],
            }
        )

    fp = header_fingerprint(header)
    generic_ratio = fp["generic_header_ratio"]
    row_label_count = sum(1 for col in columns if col["semantic_role"] == "row_label")
    value_col_count = sum(1 for col in columns if col["semantic_role"] not in {"row_label"})

    title = clean_text(
        payload.get("table_title")
        or payload.get("title")
        or getattr(table_block, "text", "")
        or f"Table on page {getattr(table_block, 'page_number', 1)}"
    )
    context = clean_text(payload.get("table_context") or getattr(table_block, "path", ""))

    confidence = 0.55
    if header:
        confidence += 0.14
    if generic_ratio < 0.5:
        confidence += 0.12
    if row_label_count:
        confidence += 0.08
    if len(rows) >= 3:
        confidence += 0.06
    if value_col_count:
        confidence += 0.04

    warnings = []
    if generic_ratio >= 0.7:
        warnings.append("Headers look generic; review column names.")
    if len(header) <= 1 and len(rows) > 5:
        warnings.append("Only one column detected; source may be visually compressed or table boundaries may be weak.")
    if not row_label_count and len(header) > 1:
        warnings.append("No clear row/feature column detected.")

    return {
        "title": title,
        "context": context,
        "page": getattr(table_block, "page_number", 1),
        "columns": columns,
        "fingerprint": fp,
        "row_count": len(rows),
        "column_count": len(header),
        "confidence": round(min(0.97, confidence), 3),
        "warnings": warnings,
    }
