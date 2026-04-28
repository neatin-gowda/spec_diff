"""Local PDF provider marker.

The actual PDF extraction remains in extractor_v2.py. This module exists so the
registry can expose a clean provider boundary without moving stable code yet.
"""

PROVIDER = {
    "name": "pdf_provider",
    "external_service": False,
    "status": "implemented_by_existing_extractor_v2",
}
