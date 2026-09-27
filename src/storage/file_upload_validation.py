"""Upload limits and filename checks for user attachments and tenant artifacts."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


DEFAULT_MAX_FILES = 10
DEFAULT_MAX_BYTES = 25 * 1024 * 1024
# Slack for multipart framing + text fields (content, metadata, project create fields).
MULTIPART_TEXT_OVERHEAD_BYTES = 1 * 1024 * 1024
GENERIC_UPLOAD_MIME = "application/octet-stream"

# Product allowlist (F1). Both extension and MIME: renaming .exe to .pdf is not enough.
# kind -> (extensions, MIME types). octet-stream is accepted for any allowed ext.
# Edit here to add a type; UI accept= mirrors extensions.
ALLOWED_UPLOAD_KINDS: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {
    "pdf": ((".pdf",), ("application/pdf",)),
    "docx": (
        (".docx",),
        (
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "application/zip",
        ),
    ),
    "xlsx": (
        (".xlsx",),
        (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "application/zip",
        ),
    ),
    "txt": ((".txt",), ("text/plain",)),
    "md": ((".md",), ("text/markdown", "text/x-markdown", "text/plain")),
    "py": ((".py",), ("text/x-python", "text/plain", "application/x-python")),
    "csv": (
        (".csv",),
        ("text/csv", "application/csv", "text/plain", "application/vnd.ms-excel"),
    ),
    "json": ((".json",), ("application/json", "text/json", "text/plain")),
    "sqlite3": ((".sqlite3",), ("application/vnd.sqlite3", "application/x-sqlite3")),
    "jsonl": (
        (".jsonl",),
        ("application/jsonl", "application/x-ndjson", "application/ndjson", "text/plain"),
    ),
    "html": ((".html",), ("text/html", "application/xhtml+xml", "text/plain")),
    "png": ((".png",), ("image/png",)),
    "jpg": ((".jpg", ".jpeg"), ("image/jpeg", "image/jpg")),
}


def _mime_by_ext() -> dict[str, frozenset[str]]:
    out: dict[str, frozenset[str]] = {}
    for exts, mimes in ALLOWED_UPLOAD_KINDS.values():
        allowed = frozenset(m.lower() for m in mimes)
        for ext in exts:
            out[ext.lower()] = allowed
    return out


ALLOWED_MIME_BY_EXT = _mime_by_ext()


class FileUploadError(Exception):
    """Rejected upload. ``status_code`` is 400 for the caller to map to HTTP."""

    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message)
        self.status_code = status_code


def _reject(message: str) -> None:
    logger.warning("[ATTACH] validation reject — %s", message)
    raise FileUploadError(message)


@dataclass(frozen=True)
class FileUploadLimits:
    max_files: int
    max_bytes: int


def _positive_int(raw: str | None, default: int) -> int:
    if raw is None or not str(raw).strip():
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def load_limits(overrides: dict | None = None) -> FileUploadLimits:
    """Env defaults, optional tenant_settings keys of the same names."""

    src = overrides or {}
    max_files = src.get("file_upload_max_files")
    max_bytes = src.get("file_upload_max_bytes")
    if max_files is None:
        max_files = _positive_int(os.getenv("FILE_UPLOAD_MAX_FILES"), DEFAULT_MAX_FILES)
    if max_bytes is None:
        max_bytes = _positive_int(os.getenv("FILE_UPLOAD_MAX_BYTES"), DEFAULT_MAX_BYTES)
    return FileUploadLimits(int(max_files), int(max_bytes))


def multipart_body_budget_bytes(limits: FileUploadLimits | None = None) -> int:
    """Hard ceiling for one multipart upload: N files × per-file cap + text overhead."""
    rules = limits or load_limits()
    return rules.max_files * rules.max_bytes + MULTIPART_TEXT_OVERHEAD_BYTES


def sanitize_filename(name: str) -> str:
    raw = (name or "").strip()
    if not raw:
        _reject("empty filename")
    if len(raw) > 255:
        _reject("filename too long")
    if "\x00" in raw or any(ord(c) < 32 for c in raw):
        _reject("unsafe filename")
    normalized = raw.replace("\\", "/")
    if "/" in normalized or normalized in (".", ".."):
        _reject("unsafe filename")
    return raw


def check_upload_type(filename: str, content_type: str) -> str:
    """Require allowlisted extension and a MIME that matches that extension."""

    ext = Path(filename).suffix.lower()
    allowed = ALLOWED_MIME_BY_EXT.get(ext)
    if not allowed:
        _reject("file type not allowed")
    ctype = (content_type or "").split(";")[0].strip().lower() or GENERIC_UPLOAD_MIME
    if ctype != GENERIC_UPLOAD_MIME and ctype not in allowed:
        _reject("content type not allowed")
    return "application/jsonl" if ext == ".jsonl" else ctype


def validate_batch(
    files: list[dict],
    limits: FileUploadLimits | None = None,
) -> list[dict]:
    """Return copies with sanitized filename and content_type. Does not copy bytes."""

    rules = limits or load_limits()
    if not files:
        _reject("no files")
    if len(files) > rules.max_files:
        _reject(f"too many files (max {rules.max_files})")
    out: list[dict] = []
    for item in files:
        data = item.get("data") or b""
        if not isinstance(data, (bytes, bytearray)):
            _reject("invalid file body")
        size = len(data)
        if size <= 0:
            _reject("empty file")
        if size > rules.max_bytes:
            _reject(f"file too large (max {rules.max_bytes} bytes)")
        filename = sanitize_filename(str(item.get("filename") or ""))
        content_type = check_upload_type(filename, str(item.get("content_type") or ""))
        out.append({**item, "filename": filename, "content_type": content_type, "size_bytes": size})
    return out
