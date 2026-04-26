"""
PDF report generation for Spec-Diff.

Produces a business-friendly comparison report:
  - Executive overview
  - Change counts / coverage / confidence
  - Review-priority items
  - Detailed change register with citations

Uses reportlab because it is predictable in Azure/container deployments and
does not require a browser runtime.
"""
from __future__ import annotations

import html
import io
from datetime import datetime
from typing import Any

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from .models import SummaryRow


CHANGE_COLORS = {
    "ADDED": colors.HexColor("#176c38"),
    "DELETED": colors.HexColor("#9f2525"),
    "MODIFIED": colors.HexColor("#735c11"),
    "UNCHANGED": colors.HexColor("#667085"),
}

IMPACT_ORDER = {"high": 3, "medium": 2, "low": 1, None: 0}


def _safe_text(value: Any, fallback: str = "-") -> str:
    if value is None:
        return fallback
    text = str(value).strip()
    return text if text else fallback


def _xml(value: Any, fallback: str = "-") -> str:
    return html.escape(_safe_text(value, fallback), quote=False)


def _short(value: Any, limit: int = 360) -> str:
    text = _safe_text(value)
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "..."


def _confidence_label(value: float | None) -> str:
    if value is None:
        return "Not rated"
    pct = round(max(0.0, min(1.0, value)) * 100)
    return f"{pct}%"


def _coverage_label(value: Any) -> str:
    if isinstance(value, (int, float)):
        return f"{value:.1f}%"
    return "-"


def _avg_confidence(summary: list[SummaryRow]) -> float | None:
    values = [s.confidence for s in summary if s.confidence is not None]
    if not values:
        return None
    return sum(values) / len(values)


def _count_needs_review(summary: list[SummaryRow]) -> int:
    return sum(
        1
        for s in summary
        if s.needs_review or (s.seek_clarification and s.seek_clarification != "None")
    )


def _change_counts(summary: list[SummaryRow]) -> dict[str, int]:
    counts = {"ADDED": 0, "DELETED": 0, "MODIFIED": 0}
    for row in summary:
        if row.change_type in counts:
            counts[row.change_type] += 1
    return counts


def _rank_review_rows(summary: list[SummaryRow]) -> list[SummaryRow]:
    def score(row: SummaryRow) -> tuple[int, int, float]:
        needs = int(row.needs_review or (row.seek_clarification and row.seek_clarification != "None"))
        impact = IMPACT_ORDER.get(row.impact, 0)
        confidence_penalty = 1.0 - row.confidence if row.confidence is not None else 0.35
        return (needs, impact, confidence_penalty)

    return sorted(summary, key=score, reverse=True)


def _style_sheet():
    base = getSampleStyleSheet()

    base.add(
        ParagraphStyle(
            name="ReportTitle",
            parent=base["Title"],
            fontName="Helvetica-Bold",
            fontSize=18,
            leading=22,
            textColor=colors.HexColor("#1f2937"),
            spaceAfter=8,
        )
    )

    base.add(
        ParagraphStyle(
            name="SectionTitle",
            parent=base["Heading2"],
            fontName="Helvetica-Bold",
            fontSize=12,
            leading=15,
            textColor=colors.HexColor("#1f2937"),
            spaceBefore=12,
            spaceAfter=7,
        )
    )

    base.add(
        ParagraphStyle(
            name="BodySmall",
            parent=base["BodyText"],
            fontName="Helvetica",
            fontSize=8.5,
            leading=11,
            textColor=colors.HexColor("#344054"),
            alignment=TA_LEFT,
        )
    )

    base.add(
        ParagraphStyle(
            name="BodyTiny",
            parent=base["BodyText"],
            fontName="Helvetica",
            fontSize=7.4,
            leading=9.3,
            textColor=colors.HexColor("#344054"),
            alignment=TA_LEFT,
        )
    )

    base.add(
        ParagraphStyle(
            name="MutedTiny",
            parent=base["BodyText"],
            fontName="Helvetica",
            fontSize=7,
            leading=8.8,
            textColor=colors.HexColor("#667085"),
            alignment=TA_LEFT,
        )
    )

    return base


def _p(text: Any, style) -> Paragraph:
    return Paragraph(_xml(text), style)


def _rich(text: str, style) -> Paragraph:
    return Paragraph(text, style)


def _footer(canvas, doc):
    canvas.saveState()
    canvas.setFont("Helvetica", 7)
    canvas.setFillColor(colors.HexColor("#667085"))
    footer = f"DocuLens AI Agent report  |  Page {doc.page}"
    canvas.drawRightString(A4[0] - 0.45 * inch, 0.28 * inch, footer)
    canvas.restoreState()


def _summary_metrics_table(run: dict, summary: list[SummaryRow], styles):
    stats = run.get("stats", {}) or {}
    coverage = run.get("coverage", {}) or {}

    avg_conf = _avg_confidence(summary)
    needs_review = _count_needs_review(summary)
    summary_counts = _change_counts(summary)

    data = [
        [
            _p("Added", styles["MutedTiny"]),
            _p(str(stats.get("ADDED", summary_counts.get("ADDED", 0))), styles["BodySmall"]),
            _p("Deleted", styles["MutedTiny"]),
            _p(str(stats.get("DELETED", summary_counts.get("DELETED", 0))), styles["BodySmall"]),
            _p("Modified", styles["MutedTiny"]),
            _p(str(stats.get("MODIFIED", summary_counts.get("MODIFIED", 0))), styles["BodySmall"]),
        ],
        [
            _p("Base Coverage", styles["MutedTiny"]),
            _p(_coverage_label(coverage.get("base")), styles["BodySmall"]),
            _p("Target Coverage", styles["MutedTiny"]),
            _p(_coverage_label(coverage.get("target")), styles["BodySmall"]),
            _p("Unchanged", styles["MutedTiny"]),
            _p(str(stats.get("UNCHANGED", 0)), styles["BodySmall"]),
        ],
        [
            _p("Review Items", styles["MutedTiny"]),
            _p(str(len(summary)), styles["BodySmall"]),
            _p("Needs Review", styles["MutedTiny"]),
            _p(str(needs_review), styles["BodySmall"]),
            _p("Avg Confidence", styles["MutedTiny"]),
            _p(_confidence_label(avg_conf), styles["BodySmall"]),
        ],
    ]

    table = Table(
        data,
        colWidths=[1.0 * inch, 0.72 * inch, 1.05 * inch, 0.72 * inch, 1.05 * inch, 0.72 * inch],
    )
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f7f4ee")),
                ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#d8d0c3")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ]
        )
    )
    return table


def _review_items_table(summary: list[SummaryRow], styles):
    rows = [
        [
            _p("Area / Item", styles["BodySmall"]),
            _p("Change", styles["BodySmall"]),
            _p("Evidence", styles["BodySmall"]),
            _p("Confidence", styles["BodySmall"]),
            _p("Review", styles["BodySmall"]),
        ]
    ]

    review_rows = [
        s
        for s in _rank_review_rows(summary)
        if s.needs_review or (s.seek_clarification and s.seek_clarification != "None")
    ][:18]

    if not review_rows:
        review_rows = _rank_review_rows(summary)[:10]

    if not review_rows:
        rows.append(
            [
                _p("No review-priority items were produced.", styles["BodyTiny"]),
                _p("-", styles["BodyTiny"]),
                _p("-", styles["BodyTiny"]),
                _p("-", styles["BodyTiny"]),
                _p("-", styles["BodyTiny"]),
            ]
        )

    for s in review_rows:
        item = s.item or s.feature
        change_type = _safe_text(s.change_type, "CHANGE")
        impact = _safe_text(s.impact, "medium")
        review = s.seek_clarification if s.seek_clarification and s.seek_clarification != "None" else s.review_reason

        area_item = (
            f"<b>{_xml(s.area, 'Document')}</b><br/>"
            f"{_xml(item)}"
        )
        change = (
            f"<b>{_xml(change_type)}</b> · {_xml(impact)} impact<br/>"
            f"{_xml(_short(s.change, 260))}"
        )
        evidence = (
            f"{_xml(_short(s.citation, 150))}<br/>"
            f"<font color='#9f2525'>Before:</font> {_xml(_short(s.before, 120))}<br/>"
            f"<font color='#176c38'>After:</font> {_xml(_short(s.after, 120))}"
        )

        rows.append(
            [
                _rich(area_item, styles["BodyTiny"]),
                _rich(change, styles["BodyTiny"]),
                _rich(evidence, styles["BodyTiny"]),
                _p(_confidence_label(s.confidence), styles["BodyTiny"]),
                _p(_short(review or "No specific clarification required.", 180), styles["BodyTiny"]),
            ]
        )

    table = Table(
        rows,
        repeatRows=1,
        colWidths=[1.45 * inch, 1.85 * inch, 1.75 * inch, 0.75 * inch, 1.45 * inch],
    )
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#263241")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("GRID", (0, 0), (-1, -1), 0.32, colors.HexColor("#d8d0c3")),
                ("BACKGROUND", (0, 1), (-1, -1), colors.white),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 5),
                ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ]
        )
    )
    return table


def _detailed_changes_table(summary: list[SummaryRow], styles):
    rows = [
        [
            _p("Type", styles["BodySmall"]),
            _p("Area / Item", styles["BodySmall"]),
            _p("Change", styles["BodySmall"]),
            _p("Evidence", styles["BodySmall"]),
            _p("Confidence", styles["BodySmall"]),
        ]
    ]

    if not summary:
        rows.append(
            [
                _p("-", styles["BodyTiny"]),
                _p("No summary rows were generated.", styles["BodyTiny"]),
                _p("-", styles["BodyTiny"]),
                _p("-", styles["BodyTiny"]),
                _p("-", styles["BodyTiny"]),
            ]
        )

    for s in summary[:90]:
        item = s.item or s.feature
        area_item = f"<b>{_xml(s.area, 'Document')}</b><br/>{_xml(item)}"
        evidence = (
            f"{_xml(_short(s.citation, 140))}<br/>"
            f"<font color='#9f2525'>Before:</font> {_xml(_short(s.before, 110))}<br/>"
            f"<font color='#176c38'>After:</font> {_xml(_short(s.after, 110))}"
        )
        conf = (
            f"{_xml(_confidence_label(s.confidence))}<br/>"
            f"{_xml(_safe_text(s.impact, 'medium'))} impact"
        )

        rows.append(
            [
                _p(_safe_text(s.change_type, "CHANGE"), styles["BodyTiny"]),
                _rich(area_item, styles["BodyTiny"]),
                _p(_short(s.change, 270), styles["BodyTiny"]),
                _rich(evidence, styles["BodyTiny"]),
                _rich(conf, styles["BodyTiny"]),
            ]
        )

    table = Table(
        rows,
        repeatRows=1,
        colWidths=[0.72 * inch, 1.55 * inch, 2.15 * inch, 2.0 * inch, 0.85 * inch],
    )
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#263241")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("GRID", (0, 0), (-1, -1), 0.28, colors.HexColor("#d8d0c3")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 4),
                ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )

    for i, s in enumerate(summary[:90], start=1):
        color = CHANGE_COLORS.get(s.change_type or "", colors.HexColor("#667085"))
        table.setStyle(TableStyle([("TEXTCOLOR", (0, i), (0, i), color)]))

    return table


def _executive_text(run: dict, summary: list[SummaryRow]) -> str:
    stats = run.get("stats", {}) or {}
    needs_review = _count_needs_review(summary)
    avg_conf = _avg_confidence(summary)

    return (
        "This report summarizes detected differences using extracted text, tables, semantic matching, "
        "and page citations. Items marked for review should be validated against the source PDFs before "
        "business approval. "
        f"Detected changes include {stats.get('ADDED', 0)} additions, "
        f"{stats.get('DELETED', 0)} removals, and {stats.get('MODIFIED', 0)} modifications. "
        f"{needs_review} item(s) are flagged for review. "
        f"Average summary confidence is {_confidence_label(avg_conf)}."
    )


def build_pdf_report(run_id: str, run: dict) -> bytes:
    """
    Build and return PDF bytes.
    """
    buffer = io.BytesIO()
    styles = _style_sheet()

    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        rightMargin=0.42 * inch,
        leftMargin=0.42 * inch,
        topMargin=0.45 * inch,
        bottomMargin=0.42 * inch,
        title="Document Comparison Report",
    )

    summary: list[SummaryRow] = run.get("summary", []) or []

    story = []

    story.append(_p("Document Comparison Report", styles["ReportTitle"]))
    story.append(
        _rich(
            f"<b>Run ID:</b> {_xml(run_id)}<br/>"
            f"<b>Baseline document:</b> {_xml(run.get('base_label'))}<br/>"
            f"<b>Revised document:</b> {_xml(run.get('target_label'))}<br/>"
            f"<b>Generated:</b> {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
            styles["BodySmall"],
        )
    )
    story.append(Spacer(1, 10))

    story.append(_p("Executive Overview", styles["SectionTitle"]))
    story.append(_summary_metrics_table(run, summary, styles))
    story.append(Spacer(1, 9))
    story.append(_p(_executive_text(run, summary), styles["BodySmall"]))

    story.append(_p("Review Priority Items", styles["SectionTitle"]))
    story.append(_review_items_table(summary, styles))

    story.append(PageBreak())

    story.append(_p("Detailed Change Register", styles["SectionTitle"]))
    story.append(_detailed_changes_table(summary, styles))

    doc.build(story, onFirstPage=_footer, onLaterPages=_footer)

    pdf = buffer.getvalue()
    buffer.close()
    return pdf
