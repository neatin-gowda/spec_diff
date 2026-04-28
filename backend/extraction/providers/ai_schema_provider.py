"""Disabled optional AI schema provider placeholder.

Schema enhancement should be explicitly selected by a user/API caller and must
have AI credentials configured. The deterministic extraction path never calls
this provider.
"""

PROVIDER = {
    "name": "ai_schema_provider",
    "external_service": True,
    "enabled": False,
    "status": "disabled_optional_future_adapter",
}
