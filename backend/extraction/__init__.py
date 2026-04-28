"""
Local extraction intelligence package.

This package deliberately avoids paid services. It enriches already-extracted
blocks with template, table, field, language, fingerprint, and quality metadata
that downstream comparison/query/reporting can reuse.
"""
from .runner import enrich_blocks, extraction_intelligence_summary

__all__ = ["enrich_blocks", "extraction_intelligence_summary"]
