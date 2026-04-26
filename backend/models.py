"""
Data models — the canonical shape of everything extracted and diffed.
Intentionally generic so any supplier template fits.
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Optional, Union
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class BlockType(str, Enum):
    SECTION       = "section"        # logical container
    HEADING       = "heading"
    PARAGRAPH     = "paragraph"
    LIST_ITEM     = "list_item"
    TABLE         = "table"
    TABLE_ROW     = "table_row"
    KV_PAIR       = "kv_pair"        # "Order Code: 765"
    NOTE          = "note"           # footnote, margin note
    FIGURE        = "figure"
    PAGE_HEADER   = "page_header"
    PAGE_FOOTER   = "page_footer"


class ChangeType(str, Enum):
    ADDED     = "ADDED"
    DELETED   = "DELETED"
    MODIFIED  = "MODIFIED"
    UNCHANGED = "UNCHANGED"


class BBox(BaseModel):
    """Normalized to 0..1 of page width/height for resolution-independence."""
    page: int
    x0: float
    y0: float
    x1: float
    y1: float


class Block(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    parent_id: Optional[UUID] = None
    block_type: BlockType
    path: str                             # e.g. /bronco/big_bend/equipment_group/mid_package
    stable_key: Optional[str] = None      # natural identifier when present
    page_number: int
    bbox: Optional[list[float]] = None    # [x0,y0,x1,y1] absolute on page
    text: str = ""
    payload: dict[str, Any] = Field(default_factory=dict)
    content_hash: str = ""
    sequence: int = 0
    children: list[UUID] = Field(default_factory=list)


class TemplateProfile(BaseModel):
    """
    Auto-discovered structural profile of a supplier document family.
    Stored once per family, reused on every subsequent ingest.
    """
    supplier: str
    family_name: str
    section_heading_patterns: list[str]   # regex
    stable_key_patterns: list[dict[str, str]]   # [{name, regex, scope_path}]
    table_signatures: list[dict[str, Any]]      # known table column patterns
    join_priority: list[str] = Field(
        default_factory=lambda: ["stable_key", "path", "embedding"]
    )
    notes: str = ""


class FieldDiff(BaseModel):
    field: str
    before: Any = None
    after: Any = None


class TokenOp(BaseModel):
    op: str                  # equal | insert | delete | replace
    text_a: Optional[str] = None
    text_b: Optional[str] = None


class BlockDiff(BaseModel):
    base_block_id: Optional[UUID] = None
    target_block_id: Optional[UUID] = None
    change_type: ChangeType
    similarity: float = 1.0
    field_diffs: list[FieldDiff] = Field(default_factory=list)
    token_diff: list[TokenOp] = Field(default_factory=list)
    impact_score: float = 0.0


class SummaryRow(BaseModel):
    """
    Generic review/report row.

    The first three fields preserve the original UI contract:
      feature | change | seek_clarification

    The remaining fields make the summary useful across document types:
    vehicle specs, contracts, RFPs, pricing docs, policies, compliance docs,
    catalogs, operating procedures, etc.
    """
    feature: str
    change: str
    seek_clarification: str

    # Generic, reusable summary fields
    area: Optional[str] = None                    # section/topic/business area
    item: Optional[str] = None                    # item, clause, row, feature, term, requirement
    change_type: Optional[str] = None             # ADDED | DELETED | MODIFIED
    category: Optional[str] = None                # pricing, dates, availability, legal, table, wording, etc.
    impact: Optional[str] = None                  # low | medium | high
    confidence: Optional[float] = None            # 0..1

    # Evidence and citations
    before: Optional[str] = None
    after: Optional[str] = None
    citation: Optional[str] = None
    page_base: Optional[int] = None
    page_target: Optional[int] = None
    stable_key: Optional[str] = None
    block_type: Optional[str] = None
    path: Optional[str] = None

    # Review/report UX
    needs_review: bool = False
    review_reason: Optional[str] = None


class ComparisonResult(BaseModel):
    base_label: str
    target_label: str
    stats: dict[str, int]
    block_diffs: list[BlockDiff]
    summary: list[SummaryRow] = Field(default_factory=list)
    finished_at: datetime = Field(default_factory=datetime.utcnow)
