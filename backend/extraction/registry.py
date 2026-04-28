"""
Provider registry for local extraction.

The registry is intentionally small today. It gives the API a single place to
describe available capabilities without forcing Azure Document Intelligence or
LLM providers into the main path.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class ProviderSpec:
    name: str
    file_kinds: tuple[str, ...]
    external_service: bool
    enabled_by_default: bool
    purpose: str


PROVIDERS = (
    ProviderSpec(
        name="pdf_provider",
        file_kinds=("pdf",),
        external_service=False,
        enabled_by_default=True,
        purpose="Extract PDF text, blocks, tables, figures, and visual coordinates using local libraries.",
    ),
    ProviderSpec(
        name="docx_provider",
        file_kinds=("word",),
        external_service=False,
        enabled_by_default=True,
        purpose="Extract Word headings, paragraphs, layout text, and real data tables without using AI.",
    ),
    ProviderSpec(
        name="spreadsheet_provider",
        file_kinds=("spreadsheet",),
        external_service=False,
        enabled_by_default=True,
        purpose="Extract Excel, xlsb, CSV, and TSV sheets as normalized tables.",
    ),
    ProviderSpec(
        name="image_ocr_provider",
        file_kinds=("image", "pdf"),
        external_service=False,
        enabled_by_default=True,
        purpose="Use local OCR/PDF image text capture when available.",
    ),
    ProviderSpec(
        name="azure_document_intelligence_provider",
        file_kinds=("pdf", "image", "word"),
        external_service=True,
        enabled_by_default=False,
        purpose="Optional paid fallback for unusually difficult layouts. Disabled unless explicitly configured.",
    ),
    ProviderSpec(
        name="ai_schema_provider",
        file_kinds=("pdf", "image", "word", "spreadsheet"),
        external_service=True,
        enabled_by_default=False,
        purpose="Optional AI schema enhancement. Disabled unless user selects AI mode and credentials exist.",
    ),
)


def list_providers() -> list[dict]:
    return [asdict(provider) for provider in PROVIDERS]


def local_provider_names() -> list[str]:
    return [provider.name for provider in PROVIDERS if not provider.external_service]


def provider_for_kind(kind: str) -> list[dict]:
    return [
        asdict(provider)
        for provider in PROVIDERS
        if kind in provider.file_kinds
    ]
