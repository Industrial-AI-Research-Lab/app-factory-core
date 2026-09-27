"""Strict JSON parsing for complete LLM responses."""

from __future__ import annotations

import json
import re
from typing import Any


class LlmJsonParseError(ValueError):
    """An LLM response did not contain one complete JSON object."""


_COMPLETE_JSON_FENCE = re.compile(
    r"\A```(?:json)?[ \t]*\r?\n(?P<body>[\s\S]*?)\r?\n?```[ \t]*\Z",
    re.IGNORECASE,
)


def parse_llm_json_object(text: Any) -> dict:
    """Parse a complete JSON object or one complete fenced JSON block.

    The parser deliberately does not search prose for JSON fragments: callers
    receive either the entire model response as an object or a parse error.
    """
    if not isinstance(text, str):
        raise LlmJsonParseError("LLM JSON response must be a string")
    cleaned = text.strip()
    if cleaned.startswith("```"):
        match = _COMPLETE_JSON_FENCE.fullmatch(cleaned)
        if not match:
            raise LlmJsonParseError("LLM JSON fence must contain the entire response")
        cleaned = match.group("body").strip()
    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError as exc:
        raise LlmJsonParseError("LLM response is not valid JSON") from exc
    if not isinstance(parsed, dict):
        raise LlmJsonParseError("LLM JSON response must be an object")
    return parsed
