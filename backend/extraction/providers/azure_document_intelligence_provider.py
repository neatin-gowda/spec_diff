"""Disabled optional provider placeholder.

No Azure Document Intelligence calls are made by default. Keep this file as a
future adapter boundary only, so the product remains local-first and cost-safe.
"""

PROVIDER = {
    "name": "azure_document_intelligence_provider",
    "external_service": True,
    "enabled": False,
    "status": "disabled_optional_future_adapter",
}
