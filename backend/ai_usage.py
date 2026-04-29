"""
Small helpers for tracking Azure OpenAI token usage per job.

Azure OpenAI returns token usage on successful chat and embedding responses.
These helpers normalize that response into one shape that the API and UI can
show without exposing internal prompts, vectors, or model payloads.
"""
from __future__ import annotations

from typing import Any


def _usage_value(usage: Any, *names: str) -> int:
    for name in names:
        value = None
        if isinstance(usage, dict):
            value = usage.get(name)
        else:
            value = getattr(usage, name, None)

        if value is None:
            continue

        try:
            return int(value)
        except (TypeError, ValueError):
            continue

    return 0


def usage_from_response(
    response: Any,
    *,
    operation: str,
    model: str | None = None,
    provider: str = "azure_openai",
) -> dict[str, Any]:
    """
    Normalize token usage from an OpenAI/Azure OpenAI response.

    For chat:
      prompt_tokens + completion_tokens = total_tokens

    For embeddings:
      Azure/OpenAI usually reports prompt_tokens/total_tokens only. Completion
      tokens remain zero because embeddings do not generate text.
    """
    usage = getattr(response, "usage", None)
    if usage is None and isinstance(response, dict):
        usage = response.get("usage")

    prompt_tokens = _usage_value(usage, "prompt_tokens", "input_tokens")
    completion_tokens = _usage_value(usage, "completion_tokens", "output_tokens")
    total_tokens = _usage_value(usage, "total_tokens")
    if not total_tokens:
        total_tokens = prompt_tokens + completion_tokens

    return {
        "calls": 1 if total_tokens or prompt_tokens or completion_tokens else 0,
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
        "operations": [
            {
                "operation": operation,
                "provider": provider,
                "model": model or "",
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": total_tokens,
            }
        ]
        if total_tokens or prompt_tokens or completion_tokens
        else [],
    }


def merge_usage(*items: dict[str, Any] | None) -> dict[str, Any]:
    merged: dict[str, Any] = {
        "calls": 0,
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
        "operations": [],
    }

    for item in items:
        if not isinstance(item, dict):
            continue

        merged["calls"] += int(item.get("calls") or 0)
        merged["prompt_tokens"] += int(item.get("prompt_tokens") or 0)
        merged["completion_tokens"] += int(item.get("completion_tokens") or 0)
        merged["total_tokens"] += int(item.get("total_tokens") or 0)

        for op in item.get("operations") or []:
            if isinstance(op, dict):
                merged["operations"].append(op)

    return merged


def add_usage(run: dict[str, Any], usage: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(run, dict):
        return merge_usage(usage)

    current = run.get("ai_usage")
    merged = merge_usage(current if isinstance(current, dict) else None, usage)
    run["ai_usage"] = merged
    return merged


def empty_usage() -> dict[str, Any]:
    return merge_usage()
