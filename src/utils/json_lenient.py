"""Lenient JSON parsing for hand-edited config (e.g. Cursor mcp.json with trailing commas)."""

from __future__ import annotations

import json
from typing import Any

def strip_json_trailing_commas(text: str) -> str:
    s = text.strip()
    out_chars: list[str] = []
    in_string = False
    escaped = False
    i = 0
    n = len(s)
    while i < n:
        ch = s[i]
        if in_string:
            out_chars.append(ch)
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
            i += 1
            continue

        if ch == '"':
            in_string = True
            out_chars.append(ch)
            i += 1
            continue

        if ch == ",":
            j = i + 1
            while j < n and s[j] in " \t\r\n":
                j += 1
            if j < n and s[j] in "}]":
                i += 1
                continue

        out_chars.append(ch)
        i += 1

    return "".join(out_chars)


def json_loads_lenient(text: str) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return json.loads(strip_json_trailing_commas(text))
