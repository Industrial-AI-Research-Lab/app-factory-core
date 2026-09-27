"""Typed LLM-layer errors shared across the runner and plugin system."""

from __future__ import annotations

import re
from typing import Any, Optional


class ContextOverflowError(RuntimeError):
    """The provider refused a request for exceeding the model context window.

    The LLM client parses provider context-length 400s into this type
    (``context_overflow_from_bad_request``). The streaming runner routes it to
    the plugin ``overflow`` hook (one recovery resend), and ``stream_responses`` /
    the chat-completions stream re-raise it instead of converting it to an error
    event, so the seam is load-bearing.

    ``segment`` names the part of the request that could not be compressed away
    when we give up (e.g. ``"tool_definitions"`` or ``"base"`` for an
    incompressible prompt that alone exceeds the window). The provider never tells
    us this — the compaction engine sets it when it raises a non-retryable
    overflow — so a client-parsed error leaves it ``None``.
    """

    def __init__(
        self,
        message: str,
        *,
        model: Optional[str] = None,
        requested_tokens: Optional[int] = None,
        window_tokens: Optional[int] = None,
        segment: Optional[str] = None,
    ):
        super().__init__(message)
        self.model = model
        self.requested_tokens = requested_tokens
        self.window_tokens = window_tokens
        self.segment = segment


# Provider error codes that unambiguously mean "over the context window".
_OVERFLOW_CODES = frozenset({"context_length_exceeded"})

# Phrases across providers/proxies (OpenAI, Anthropic-via-Bifrost) that name a
# context-window overflow. Kept lowercase; matched against the joined message.
_OVERFLOW_PHRASES = (
    "maximum context length",
    "context window",
    "context length",
    "prompt is too long",
    "input is too long",
    "reduce the length of the messages",
    "too many tokens",
)

# "your input of 210000 tokens > 200000 maximum" style — requested first, window
# second. Tried before the OpenAI split patterns below.
_TOKENS_GT_MAX_RE = re.compile(r"(\d[\d,]*)\s*tokens?\s*>\s*(\d[\d,]*)\s*maximum")
_WINDOW_RE = re.compile(r"maximum context length is\s*(\d[\d,]*)\s*tokens")
_REQUESTED_RE = re.compile(
    r"(?:you requested|resulted in|your input of|your messages resulted in)\s*(\d[\d,]*)\s*tokens"
)


def _as_int(raw: str) -> int:
    return int(raw.replace(",", ""))


def _extract_token_counts(text: str):
    """(window_tokens, requested_tokens) parsed from a 400 message, or (None, None).

    Detection never depends on this succeeding — a bare phrase with no numbers is
    still an overflow, just without counts for the failure message.
    """
    gt = _TOKENS_GT_MAX_RE.search(text)
    if gt:
        return _as_int(gt.group(2)), _as_int(gt.group(1))
    window = _WINDOW_RE.search(text)
    requested = _REQUESTED_RE.search(text)
    return (
        _as_int(window.group(1)) if window else None,
        _as_int(requested.group(1)) if requested else None,
    )


def context_overflow_from_bad_request(exc, *, model: Optional[str] = None):
    """Return a ContextOverflowError if this 400 is a context-window overflow, else None.

    Duck-types the openai ``BadRequestError`` fields the sampling-param classifier
    reads (``body``, ``code``, ``message``); a sampling-param or unrelated 400
    returns None so its own strip-and-retry / re-raise path still runs.
    """
    body = getattr(exc, "body", None)
    body = body if isinstance(body, dict) else {}
    nested = body.get("error") if isinstance(body.get("error"), dict) else {}

    code = str(
        getattr(exc, "code", None) or body.get("code") or nested.get("code") or ""
    ).lower()
    message = " ".join(
        str(part)
        for part in (
            getattr(exc, "message", None),
            nested.get("message"),
            body.get("message"),
        )
        if part
    ) or str(exc)

    text = message.lower()
    if code not in _OVERFLOW_CODES and not any(p in text for p in _OVERFLOW_PHRASES):
        return None

    window, requested = _extract_token_counts(text)
    return ContextOverflowError(
        message,
        model=model,
        requested_tokens=requested,
        window_tokens=window,
    )


# AppFactory-316: retry only transient provider/network failures (not 4xx/auth/overflow).
_TRANSIENT_STATUS = frozenset({429, 500, 502, 503, 504})
_NON_TRANSIENT_STATUS = frozenset({400, 401, 403, 404, 422})
# Stream path usually has only str(e); OpenAI APIConnectionError → "Connection error."
_TRANSIENT_PHRASES = (
    "overloaded",
    "temporarily",
    "timeout",
    "timed out",
    "rate limit",
    "rate_limit",
    "too many requests",
    "unavailable",
    "connection error",
    "connection reset",
    "connection refused",
    "connection aborted",
    "connection closed",
    "econnreset",
    "broken pipe",
    "server disconnected",
    "peer closed",
    "provider disconnected",
    "stream disconnected",
    "incomplete message body",
    "mid-stream",
    "remote protocol",
    "unexpected eof",
    "server error",
    "server had an error",
    "server_error",
    "bad gateway",
    "service unavailable",
    "gateway timeout",
    "no healthy upstream",
    "upstream connect",
    "upstream timed out",
)
# call_llm may still hold the exception object (stream path is usually str only).
_TRANSIENT_TYPE_NAMES = frozenset(
    {
        "APIConnectionError",
        "APITimeoutError",
        "RateLimitError",
        "InternalServerError",
        "ConnectError",
        "RemoteProtocolError",
        "ReadTimeout",
        "WriteTimeout",
        "ConnectTimeout",
        "TimeoutException",
    }
)
_NON_TRANSIENT_PHRASES = (
    "reasoning is mandatory",
    "invalid_request",
    "invalid api key",
    "incorrect api key",
    "authentication",
    "unauthorized",
    "forbidden",
    "permission denied",
    "maximum context length",
    "context window",
    "context length",
    "prompt is too long",
)
# Peer/stream resets are transient; bare "Cancelled" (task stop) is not.
_PEER_CANCEL_RE = re.compile(
    r"cancell?ed\s+by\s+peer|stream\s+cancell?ed|stream\s*reset",
    re.I,
)
_USER_CANCEL_RE = re.compile(r"\bcancell?ed\b", re.I)
# Explicit provider status only — never treat a random "500" (e.g. max_tokens) as HTTP 500.
_EXPLICIT_STATUS_RE = re.compile(
    r"(?:error\s*code|status(?:\s*code)?)\s*[:=]?\s*(\d{3})\b"
    r"|^\s*(\d{3})\b"
    r"|(\d{3})\s+(?:"
    r"bad gateway|internal server error|service unavailable|gateway timeout|"
    r"too many requests|bad request|unauthorized|forbidden|not found|"
    r"unprocessable|ok"
    r")\b"
    r"|\bhttp(?:/\d\.\d)?\s+(\d{3})\b",
    re.I,
)
def _http_status_from_text(text: str) -> Optional[int]:
    m = _EXPLICIT_STATUS_RE.search(text)
    if not m:
        return None
    for g in m.groups():
        if g is not None:
            return int(g)
    return None


def is_transient_provider_error(err: Any, *, error_type: Optional[str] = None) -> bool:
    """True when a stream/provider failure is worth retrying (AppFactory-316)."""
    import asyncio

    if isinstance(err, ContextOverflowError):
        return False
    # Lazy: agent_model_params can import this module; keep duck-typed.
    if type(err).__name__ == "AgentModelParamsValidationError":
        return False
    if isinstance(err, asyncio.CancelledError):
        return False
    # Stream path usually yields str(e); callers may pass error_type from the event.
    type_name = error_type
    if not type_name and not isinstance(err, (str, bytes)):
        type_name = type(err).__name__
    if type_name in _TRANSIENT_TYPE_NAMES:
        return True

    code = getattr(err, "status_code", None)
    if code is None:
        code = getattr(err, "status", None)
    if code is not None:
        try:
            c = int(code)
        except (TypeError, ValueError):
            c = None
        if c in _NON_TRANSIENT_STATUS:
            return False
        if c in _TRANSIENT_STATUS:
            return True

    text = str(err or "").strip().lower()
    if not text:
        return False
    # Only explicit provider status (Error code: / leading / "502 Bad Gateway"), never
    # bare digits mid-sentence ("used 429 tokens", "line 502 in file").
    declared = _http_status_from_text(text)
    if declared is not None:
        if declared in _NON_TRANSIENT_STATUS:
            return False
        if declared in _TRANSIENT_STATUS:
            return True
    if _PEER_CANCEL_RE.search(text):
        return True
    if _USER_CANCEL_RE.search(text):
        return False
    if any(p in text for p in _NON_TRANSIENT_PHRASES):
        return False
    return any(p in text for p in _TRANSIENT_PHRASES)
