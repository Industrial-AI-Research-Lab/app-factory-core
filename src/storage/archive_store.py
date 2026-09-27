"""Archive Store — move oversized tool results into S3-compatible object storage.

Why this exists: one ~15 MB tool result (from the urban-space-api server,
2026-06-10) went into the conversation whole — roughly 3M tokens against a 1M
window — and killed the run twice. Separately, once each call/result pair gets
saved to the database, a result larger than the database's 16 MB per-document
limit would crash the save itself. So this runs on every tool result, always
on, and fires before anything is written to the database.

Write side (AppFactory-183): stores the raw blob plus a line-oriented normalized
twin, and hands back a short ref. Read side (AppFactory-148): looks a ref up,
streams+greps the normalized twin with constant memory, and mints short-lived
presigned GET urls. Stored objects are immutable — there is deliberately no
update/overwrite path; a "new version" of anything is a new ref.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import mimetypes
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Callable, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# 512 KB: separates everyday-verbose results (file reads, command output) from
# pathological ones, while staying far under Mongo's 16MB doc cap even when a
# step-2 ledger doc holds many pairs. Env-overridable per deploy.
DEFAULT_SPILL_THRESHOLD_BYTES = 512 * 1024

# Mongo caps a BSON document at 16 MiB and the spilled content is its dominant
# field, so the effective threshold is capped below that: a file this large always
# spills whatever a tenant or the env sets, or the oversized inline write returns.
HARD_SPILL_CEILING_BYTES = 15 * 1024 * 1024

# maybe_spill runs on every tool result, so resolving a tenant's threshold from
# Mongo per call would put a round-trip on the hot path. Cache the resolved value
# briefly instead; a settings change takes at most this long to take effect.
_THRESHOLD_CACHE_TTL_SECONDS = 30.0

# Cap on the failure-signal snippets (error/stderr) copied onto the placeholder,
# so a huge error payload can't re-introduce the bloat the archive exists to remove.
_SIGNAL_SNIPPET_MAX = 500

# Compact fields the terminal gate / agent need on a spill placeholder. Copied
# from the pre-spill result so infra labels survive a successful archive.
_COMPACT_SIGNAL_KEYS = (
    "status",
    "isError",
    "exit_code",
    "error",
    "stderr",
    "error_type",
    "outcome_unknown",
)
# Loud-fail already owns status=error; copying success status/exit_code would
# erase the archive failure and let the gate COMPLETE. Only infra labels move.
_LOUD_FAIL_SIGNAL_KEYS = (
    "error_type",
    "outcome_unknown",
    "stderr",
)


def _copy_capped_signal_fields(
    src: Dict[str, Any], dest: Dict[str, Any], keys: tuple
) -> None:
    for k in keys:
        v = src.get(k)
        if v is None:
            continue
        if isinstance(v, (int, float, bool)):
            dest[k] = v
            continue
        s = v if isinstance(v, str) else str(v)
        if len(s) > _SIGNAL_SNIPPET_MAX:
            s = s[:_SIGNAL_SNIPPET_MAX] + " ...[truncated; full output in archive]"
        dest[k] = s


def _apply_compact_signal_fields(src: Dict[str, Any], dest: Dict[str, Any]) -> None:
    _copy_capped_signal_fields(src, dest, _COMPACT_SIGNAL_KEYS)


def _apply_loud_fail_signal_fields(src: Dict[str, Any], dest: Dict[str, Any]) -> None:
    _copy_capped_signal_fields(src, dest, _LOUD_FAIL_SIGNAL_KEYS)

# Head/tail byte budget for the inline preview of a spilled result. Kept small so
# the preview can't re-inflate the placeholder it exists to shrink; the tail is
# larger than the head because a failed command's diagnostic (stack trace,
# "BUILD FAILED") lands at the END of the output, not the start.
_PREVIEW_HEAD_BYTES = 1024
_PREVIEW_TAIL_BYTES = 2048

# The normalized twin lives right next to the raw object so tenant/project
# prefix operations (delete_prefix, lifecycle rules) cover both without knowing
# about representations.
_NORMALIZED_KEY_SUFFIX = ".norm"

# A field is the "bulk" of a result when it carries at least half the bytes —
# then its own lines (text) or items (list→NDJSON) are what grep should see,
# not a JSON-escaped rendering of them.
_DOMINANCE_RATIO = 0.5

# scan_lines bounds: a single line longer than this is truncated for matching
# and skipped to the next newline (legacy raw blobs are one endless JSON line —
# without the cap the "constant memory" promise dies on exactly those), and
# matched text returned to the agent is clipped so a hit inside a huge line
# cannot re-inflate the context.
_SCAN_LINE_MAX_BYTES = 1024 * 1024
_MATCH_TEXT_MAX_CHARS = 2000
# Max continuous synchronous matching before scan_lines yields to the event
# loop. Within one chunk the line loop has no natural await, so a slow matcher
# would otherwise starve every other coroutine — and the caller's
# asyncio.wait_for budget could never fire.
_SCAN_YIELD_INTERVAL_SECONDS = 0.05

# Storage that is up answers a HEAD long before this, a few botocore retries included;
# unbounded, a dead store would hold the check for 5 attempts × 60 s.
_HEAD_AFTER_PUT_ERROR_TIMEOUT_SECONDS = 10.0


def _result_bytes(result: Dict[str, Any]) -> Tuple[bytes, int]:
    """Serialize a tool result exactly as it would be sent inline, and measure it.

    ensure_ascii=False mirrors streaming_agent_runner._format_tool_output, so the
    measured size equals what would actually hit the context / Mongo — Cyrillic
    and other non-ASCII payloads are not undercounted as \\uXXXX escapes.

    errors="replace": an upstream result can carry a lone UTF-16 surrogate — an
    emoji cut in half by naive truncation is the common way — which is legal JSON
    but cannot be UTF-8 encoded. This runs on every result, so a bare .encode
    would turn a harmless 200-byte reply into a spurious tool error. Replacing
    keeps both the measurement and the archived copy valid UTF-8.
    """
    data = json.dumps(result, ensure_ascii=False, default=str).encode("utf-8", errors="replace")
    return data, len(data)


def _normalize_representation(
    result: Dict[str, Any], raw_size: int
) -> Tuple[bytes, str, str, Optional[str]]:
    """Build the line-oriented twin of a spilled result for archive_query.

    Raw MCP results serialize to ONE line of JSON — useless and OOM-prone for
    line-oriented streaming ops — so normalization happens once at write time
    (write once, read many; ADR-0005). Returns (data, kind, content_type,
    source_field):

    - a string field carrying ≥half the bytes (file read, command stdout) →
      that text VERBATIM, so grep hits the code/log line, not a JSON escape;
    - a list field carrying ≥half the bytes (FeatureCollection.features) →
      NDJSON, one item per line, meta line first — a match line is one
      complete, self-contained item;
    - otherwise → pretty-printed JSON, one key per line.
    """
    # Rank by ENCODED bytes — the unit the dominance test below uses. Ranking
    # by len() chars let a byte-dominant Cyrillic field (2-3 B/char) lose the
    # shortlist to a longer-looking ASCII one and fall through to pretty_json
    # untested.
    best_field, best_encoded = None, b""
    for k, v in result.items():
        if isinstance(v, str):
            encoded = v.encode("utf-8", errors="replace")
            if len(encoded) > len(best_encoded):
                best_field, best_encoded = k, encoded
    if best_field is not None and len(best_encoded) >= raw_size * _DOMINANCE_RATIO:
        return best_encoded, "text", "text/plain; charset=utf-8", best_field

    for k, v in result.items():
        if isinstance(v, list) and v:
            approx = len(json.dumps(v, ensure_ascii=False, default=str).encode("utf-8", errors="replace"))
            if approx >= raw_size * _DOMINANCE_RATIO:
                meta = {
                    "__archive_meta__": {mk: mv for mk, mv in result.items() if mk != k},
                    "list_field": k,
                    "item_count": len(v),
                }
                lines = [json.dumps(meta, ensure_ascii=False, default=str)]
                lines.extend(json.dumps(item, ensure_ascii=False, default=str) for item in v)
                return "\n".join(lines).encode("utf-8", errors="replace"), "ndjson", "application/x-ndjson", k

    pretty = json.dumps(result, ensure_ascii=False, indent=2, default=str)
    return pretty.encode("utf-8", errors="replace"), "pretty_json", "application/json", None


def _head_tail_preview(data: bytes, size: int) -> str:
    """A head+tail glimpse of the archived bytes, so the agent can usually act on a
    spilled result — read the error at the end of a failed build log — without
    fetching the whole thing back (real retrieval comes in a later change). Only
    the two end slices are decoded, never the whole blob, so this stays cheap on a
    multi-MB result; errors="replace" absorbs a multibyte char clipped at the byte
    boundary rather than raising."""
    if size <= _PREVIEW_HEAD_BYTES + _PREVIEW_TAIL_BYTES:
        return data.decode("utf-8", errors="replace")
    head = data[:_PREVIEW_HEAD_BYTES].decode("utf-8", errors="replace")
    tail = data[-_PREVIEW_TAIL_BYTES:].decode("utf-8", errors="replace")
    return f"{head}\n...[middle elided — full {size} bytes archived]...\n{tail}"


def _guess_file_content_type(path: str) -> str:
    """A best-effort MIME type from the file's name, for the download response and
    the locator record. Unknown extensions fall back to a generic binary type."""
    guessed, _ = mimetypes.guess_type(path or "")
    return guessed or "application/octet-stream"


def _looks_binary(sample: bytes) -> bool:
    """A NUL byte in the head is the cheap, reliable tell of a binary file — it
    gates whether a file spill records a text preview, so an image or zip does not
    put mojibake in the monitor while a .geojson/.html/.json still gets one."""
    return b"\x00" in sample


def _is_missing_key(exc: Exception) -> bool:
    resp = getattr(exc, "response", None)
    code = str(((resp.get("Error") or {}).get("Code")) or "") if isinstance(resp, dict) else ""
    return code in ("404", "NoSuchKey", "NotFound")


def _describe_s3_error(exc: Exception) -> str:
    """The error plus the ids storage support needs to find the request."""
    resp = getattr(exc, "response", None)
    if not isinstance(resp, dict):
        return f"{type(exc).__name__}: {exc}"
    meta = resp.get("ResponseMetadata") or {}
    return (
        f"{type(exc).__name__}: {exc} (status={meta.get('HTTPStatusCode')} "
        f"request_id={meta.get('RequestId')} host_id={meta.get('HostId')} "
        f"retries={meta.get('RetryAttempts')})"
    )


def _is_same_blob(head: Dict[str, Any], data: bytes) -> bool:
    # A single-part PUT's ETag is the MD5 of the body; any other ETag form fails
    # the match and costs one extra upload, never a false "stored".
    etag = str(head.get("ETag") or "").strip('"')
    return (
        head.get("ContentLength") == len(data)
        and etag == hashlib.md5(data, usedforsecurity=False).hexdigest()
    )


async def _prepend(first: bytes, rest: AsyncIterator[bytes]) -> AsyncIterator[bytes]:
    try:
        if first:
            yield first
        async for chunk in rest:
            yield chunk
    finally:
        # An interrupted async-for leaves its source open: closing this stream closes the storage GET too.
        await rest.aclose()


class ArchiveStore:
    """Guards the tool-result path: oversized results go to object storage; the
    caller and journal get a short placeholder ref instead of megabytes.

    The write path's S3 calls are isolated in `_put_blob` and, after a failed put,
    `_head_blob` (mockable seams that import aioboto3 lazily), so the threshold /
    loud-fail / placeholder logic is unit testable without the dependency or any network.
    """

    def __init__(
        self,
        db=None,
        *,
        endpoint: Optional[str] = None,
        region: Optional[str] = None,
        bucket: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        addressing_style: str = "path",
        threshold_bytes: int = DEFAULT_SPILL_THRESHOLD_BYTES,
    ):
        self.db = db
        self.endpoint = endpoint
        self.region = region
        self.bucket = bucket
        self._access_key = access_key
        self._secret_key = secret_key
        self.addressing_style = addressing_style
        self.threshold_bytes = min(threshold_bytes, HARD_SPILL_CEILING_BYTES)
        self._refs_collection = None
        self._threshold_cache: Dict[str, Tuple[int, float]] = {}

    @classmethod
    def from_env(cls, db=None) -> "ArchiveStore":
        """Build from ARCHIVE_S3_* env vars (per-env bucket creds via Helm secrets)."""
        raw_threshold = os.getenv("ARCHIVE_SPILL_THRESHOLD_BYTES")
        try:
            threshold = int(raw_threshold) if raw_threshold else DEFAULT_SPILL_THRESHOLD_BYTES
        except ValueError:
            logger.warning(
                "[ARCHIVE] invalid ARCHIVE_SPILL_THRESHOLD_BYTES=%r — using default %d",
                raw_threshold, DEFAULT_SPILL_THRESHOLD_BYTES,
            )
            threshold = DEFAULT_SPILL_THRESHOLD_BYTES
        if threshold <= 0:
            # A zero or negative threshold would spill EVERY result (size is
            # always > 0) — an all-traffic firehose to the bucket. Treat it as a
            # misconfiguration, not an instruction to archive everything.
            logger.warning(
                "[ARCHIVE] ARCHIVE_SPILL_THRESHOLD_BYTES=%d is not positive — using default %d",
                threshold, DEFAULT_SPILL_THRESHOLD_BYTES,
            )
            threshold = DEFAULT_SPILL_THRESHOLD_BYTES
        return cls(
            db,
            endpoint=os.getenv("ARCHIVE_S3_ENDPOINT") or None,
            region=os.getenv("ARCHIVE_S3_REGION") or None,
            bucket=os.getenv("ARCHIVE_S3_BUCKET") or None,
            access_key=os.getenv("ARCHIVE_S3_ACCESS_KEY") or None,
            secret_key=os.getenv("ARCHIVE_S3_SECRET_KEY") or None,
            addressing_style=os.getenv("ARCHIVE_S3_ADDRESSING_STYLE") or "path",
            threshold_bytes=threshold,
        )

    def is_configured(self) -> bool:
        return bool(self.endpoint and self.bucket and self._access_key and self._secret_key)

    @property
    def refs(self):
        """Lazy access to the archive_refs collection (Mongo keeps only the record)."""
        if self._refs_collection is None and self.db is not None:
            self._refs_collection = self.db.db.archive_refs
        return self._refs_collection

    async def resolve_threshold_bytes(self, tenant_id: Optional[str]) -> int:
        """The effective spill threshold for a tenant: its own Settings value when
        set to a positive int, otherwise this store's env/default floor.

        A tenant without its own value, a store with no settings source, an
        unreadable store, or a malformed stored value all resolve to the floor —
        never to something that would spill everything (a non-positive threshold)
        or nothing. bool is rejected explicitly: it is an int subclass, so True
        would otherwise read as a 1-byte threshold. Read at most once per tenant
        per TTL window; a transient read error is not cached, so it retries."""
        if not tenant_id or tenant_id in ("__root__", "__default__"):
            return self.threshold_bytes
        now = time.monotonic()
        cached = self._threshold_cache.get(tenant_id)
        if cached is not None and cached[1] > now:
            return cached[0]
        getter = getattr(self.db, "get_tenant_settings", None)
        if getter is None:
            return self.threshold_bytes
        try:
            settings = await getter(tenant_id)
        except Exception as exc:
            logger.warning(
                "[ARCHIVE] tenant %s settings read failed: %s — using floor %d",
                tenant_id, exc, self.threshold_bytes,
            )
            return self.threshold_bytes
        value = settings.get("spill_threshold_bytes") if isinstance(settings, dict) else None
        resolved = (
            min(value, HARD_SPILL_CEILING_BYTES)
            if isinstance(value, int) and not isinstance(value, bool) and value > 0
            else self.threshold_bytes
        )
        self._threshold_cache[tenant_id] = (resolved, now + _THRESHOLD_CACHE_TTL_SECONDS)
        return resolved

    async def maybe_spill(
        self,
        result: Any,
        *,
        project_id: Optional[str],
        tool_id: Optional[str],
        run_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
        tool_call_id: Optional[str] = None,
    ) -> Any:
        """Return `result` unchanged if within threshold; otherwise stash it in
        object storage and return a small placeholder ref.

        Loud-fail: an oversized result that cannot be archived (bucket
        unconfigured or unreachable) returns an *error* tool-result — it is never
        inlined, because inlining is exactly the Mongo/context bloat this removes.
        """
        if not isinstance(result, dict):
            return result

        data, size = _result_bytes(result)
        if size <= await self.resolve_threshold_bytes(tenant_id):
            return result

        if not self.is_configured():
            logger.error(
                "[ARCHIVE] oversized result (%d B, tool=%s) but object storage not configured — loud-fail",
                size, tool_id,
            )
            out = {
                "status": "error",
                "error": (
                    f"archive unavailable: tool result of {size} bytes exceeds the inline "
                    f"limit and object storage is not configured"
                ),
                "error_type": "unavailable",
            }
            _apply_loud_fail_signal_fields(result, out)
            return out

        ref_id = f"arch_{uuid.uuid4().hex}"
        content_type = "application/json"
        # Tenant is the TOP-level prefix on purpose: it keeps one tenant's blobs
        # in their own key-space, so a future per-tenant lifecycle/retention rule
        # or bucket policy has something to target. Don't reorder to project-first.
        object_key = f"{tenant_id or 'notenant'}/{project_id or 'noproject'}/{run_id or 'norun'}/{ref_id}"

        try:
            await self._put_blob_checked(object_key, data, content_type)
        except Exception as exc:
            # Detail stays in the log; the agent-facing error omits `exc` on
            # purpose — botocore messages can embed the endpoint and bucket, which
            # must never reach the agent (same invariant as the placeholder).
            logger.error("[ARCHIVE] put failed (tool=%s key=%s): %s", tool_id, object_key, exc)
            out = {
                "status": "error",
                "error": f"archive unavailable: failed to store oversized result ({size} bytes)",
                "error_type": "unavailable",
            }
            _apply_loud_fail_signal_fields(result, out)
            return out

        # The line-oriented twin for archive_query. Raw is the record of truth
        # (humans download it byte-identical), so losing only the twin degrades
        # query to "not available" instead of failing the whole spill.
        representations: Dict[str, Any] = {
            "raw": {"object_key": object_key, "content_type": content_type, "size_bytes": size},
        }
        norm_data, norm_kind, norm_ctype, norm_field = _normalize_representation(result, size)
        norm_key = object_key + _NORMALIZED_KEY_SUFFIX
        try:
            await self._put_blob_checked(norm_key, norm_data, norm_ctype)
            representations["normalized"] = {
                "object_key": norm_key,
                "content_type": norm_ctype,
                "size_bytes": len(norm_data),
                "kind": norm_kind,
                "source_field": norm_field,
            }
            # A "text" twin is ONE field verbatim — every other field of the
            # result is unsearchable via archive_query. Recording their names is
            # what lets the query answer say "0 matches in stderr; stdout was
            # not searched" instead of a false "not present". ndjson keeps the
            # rest in its meta line and pretty_json is the whole result, so
            # only text omits anything.
            if norm_kind == "text":
                omitted = [k for k in result if k != norm_field]
                if omitted:
                    representations["normalized"]["omitted_fields"] = omitted
        except Exception as exc:
            logger.warning(
                "[ARCHIVE] normalized representation put failed for %s: %s — raw only", ref_id, exc
            )

        preview = _head_tail_preview(data, size)
        persisted = await self._write_ref({
            "_id": ref_id,
            # Scoping key for later retrieval: without it, finding a tenant's
            # archived results means joining back through the projects collection.
            "tenant_id": tenant_id,
            "project_id": project_id,
            "run_id": run_id,
            "agent_id": agent_id,
            "tool_id": tool_id,
            "size_bytes": size,
            "content_type": content_type,
            "bucket": self.bucket,
            "object_key": object_key,
            # Integrity + consolidation metadata (AppFactory-148 forward reqs):
            # sha256 pins content identity for dedup/replay; the preview makes
            # archive_inspect a Mongo-only read; source is an opaque provenance
            # card, deliberately schema-free beyond "kind".
            "sha256": hashlib.sha256(data).hexdigest(),
            "est_tokens": size // 4,
            "preview": preview,
            "source": {
                "kind": "tool_spill",
                "tool_call_id": tool_call_id,
                "agent_id": agent_id,
                "tool_id": tool_id,
            },
            "representations": representations,
            "created_at": datetime.now(timezone.utc),
        })

        logger.info(
            "[ARCHIVE] spilled %d B tool=%s → ref=%s%s", size, tool_id, ref_id,
            "" if persisted else " (locator NOT persisted; preview-only placeholder)",
        )
        # What the agent and the journal see. No bucket, key, URL, or
        # credentials ever reach the agent.
        placeholder: Dict[str, Any]
        if persisted:
            placeholder = {
                "status": "success",
                "kind": "archive_ref",
                "ref_id": ref_id,
                "size_bytes": size,
                "content_type": content_type,
                "note": (
                    "the full tool result was too large to include inline and was "
                    "archived; `preview` shows its head and tail. To dig deeper: "
                    "archive_inspect(ref_id) for metadata, archive_query(ref_id, "
                    "pattern) to regex-search it line by line, archive_fetch(ref_id) "
                    "to download it into the sandbox for local processing"
                ),
                "preview": preview,
            }
        else:
            # Blob stored, locator did not land → every archive_* tool would 404
            # on this ref. Promise nothing: no ref_id to chase, and dropping
            # `kind: archive_ref` also skips the dispatcher's emit + schema-attach.
            placeholder = {
                "status": "success",
                "size_bytes": size,
                "content_type": content_type,
                "note": (
                    "the full tool result was too large to include inline and was "
                    "archived, but its locator record could not be saved, so it "
                    "cannot be retrieved later; `preview` (head and tail) is all "
                    "that is available"
                ),
                "preview": preview,
            }
        _apply_compact_signal_fields(result, placeholder)
        nested = result.get("data")
        if isinstance(nested, dict):
            signals = {
                key: value
                for key in ("status", "isError")
                if isinstance((value := nested.get(key)), (str, bool))
                and (not isinstance(value, str) or len(value) <= _SIGNAL_SNIPPET_MAX)
            }
            if signals:
                placeholder["data"] = signals
        return placeholder

    async def spill_file(
        self,
        content: bytes,
        *,
        path: str,
        project_id: Optional[str],
        run_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
        agent_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Move one oversized file artifact's bytes into object storage and record a
        locator (source.kind="file_artifact"), so the file_artifacts document can
        hold a small reference instead of content that would breach Mongo's 16 MB
        per-document cap.

        Returns the fields the caller persists in place of inline content
        (ref_id / object_key / size_bytes / sha256 / content_type), or None when
        object storage is unconfigured or the blob PUT / locator write fails — the
        caller then keeps the existing inline behaviour rather than losing the file.

        Reuses the tool-spill blob + record path but stores only the raw object: a
        file is downloaded or hydrated whole, never line-scanned by archive_query,
        so it needs no normalized twin. The record is listable and downloadable
        through the same archive routes a tool spill uses.
        """
        if not self.is_configured():
            return None
        size = len(content)
        ref_id = f"arch_{uuid.uuid4().hex}"
        content_type = _guess_file_content_type(path)
        # Same tenant-first key layout as maybe_spill, so one prefix delete / one
        # lifecycle rule covers a tenant's tool spills and file spills alike.
        object_key = f"{tenant_id or 'notenant'}/{project_id or 'noproject'}/{run_id or 'norun'}/{ref_id}"
        try:
            await self._put_blob_checked(object_key, content, content_type)
        except Exception as exc:
            logger.error("[ARCHIVE] file spill put failed (path=%s key=%s): %s", path, object_key, exc)
            return None
        record: Dict[str, Any] = {
            "_id": ref_id,
            "tenant_id": tenant_id,
            "project_id": project_id,
            "run_id": run_id,
            "agent_id": agent_id,
            "path": path,
            "size_bytes": size,
            "content_type": content_type,
            "bucket": self.bucket,
            "object_key": object_key,
            "sha256": hashlib.sha256(content).hexdigest(),
            "est_tokens": size // 4,
            "source": {"kind": "file_artifact", "path": path, "agent_id": agent_id},
            "representations": {
                "raw": {"object_key": object_key, "content_type": content_type, "size_bytes": size},
            },
            "created_at": datetime.now(timezone.utc),
        }
        if not _looks_binary(content[:_PREVIEW_HEAD_BYTES]):
            record["preview"] = _head_tail_preview(content, size)
        persisted = await self._write_ref(record)
        if not persisted:
            # Blob is in the bucket but unaddressable; a lifecycle rule reclaims
            # it. Report failure so the caller inlines rather than storing a
            # reference nothing can resolve.
            logger.warning("[ARCHIVE] file spill locator not persisted for %s (path=%s)", ref_id, path)
            return None
        logger.info("[ARCHIVE] spilled %d B file artifact path=%s → ref=%s", size, path, ref_id)
        return {
            "ref_id": ref_id,
            "object_key": object_key,
            "size_bytes": size,
            "sha256": record["sha256"],
            "content_type": content_type,
        }

    async def _write_ref(self, record: Dict[str, Any]) -> bool:
        """Persist the small locator record. A failed record must not undo a
        successful blob PUT — the run survives; we log and move on. Returns True
        when the record landed. On False the blob is unaddressable (retrieval
        resolves through this record), so the caller must not promise the
        archive_* tools for it — they would 404."""
        if self.refs is None:
            logger.warning("[ARCHIVE] no db — archive_refs record %s not persisted", record.get("_id"))
            return False
        try:
            await self.refs.insert_one(dict(record))
            return True
        except Exception as exc:
            logger.warning("[ARCHIVE] archive_refs insert failed for %s: %s", record.get("_id"), exc)
            return False

    async def delete_prefix(self, prefix: str) -> Tuple[int, int]:
        """Best-effort delete of every archived blob under ``prefix`` — e.g. a
        tenant's ``"<tenant_id>/"`` key-space when the tenant is deleted.

        Returns ``(deleted, failed)``. NEVER raises: a tenant delete must not
        fail because the bucket is unreachable. By the time this runs the Mongo
        locators are already gone (nothing can address the bytes), and the
        bucket lifecycle rule is the backstop for anything left behind.
        """
        if not prefix:
            return (0, 0)
        if not self.is_configured():
            logger.warning(
                "[ARCHIVE] blobs under %r not deleted — object storage not configured; "
                "relying on bucket lifecycle policy",
                prefix,
            )
            return (0, 0)
        try:
            return await self._delete_prefix(prefix)
        except Exception as exc:
            logger.error("[ARCHIVE] prefix delete failed (%r): %s", prefix, exc)
            return (0, 0)

    async def _delete_prefix(self, prefix: str) -> Tuple[int, int]:
        """List and delete all objects under ``prefix``. Overridable seam for
        tests; lazy-imports aioboto3. Paginates the listing and deletes in
        ≤1000-key batches (the S3 DeleteObjects limit)."""
        import aioboto3
        from botocore.config import Config

        deleted = 0
        failed = 0
        session = aioboto3.Session()
        async with session.client(
            "s3",
            endpoint_url=self.endpoint,
            region_name=self.region,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
            config=Config(s3={"addressing_style": self.addressing_style}),
        ) as s3:
            continuation: Optional[str] = None
            while True:
                kwargs: Dict[str, Any] = {"Bucket": self.bucket, "Prefix": prefix, "MaxKeys": 1000}
                if continuation:
                    kwargs["ContinuationToken"] = continuation
                listing = await s3.list_objects_v2(**kwargs)
                keys = [{"Key": obj["Key"]} for obj in listing.get("Contents", [])]
                if keys:
                    resp = await s3.delete_objects(
                        Bucket=self.bucket, Delete={"Objects": keys, "Quiet": True}
                    )
                    errors = len(resp.get("Errors", []))
                    failed += errors
                    deleted += len(keys) - errors
                if not listing.get("IsTruncated"):
                    break
                continuation = listing.get("NextContinuationToken")
        return (deleted, failed)

    async def _put_blob(self, key: str, data: bytes, content_type: str) -> None:
        """PUT bytes to the bucket. Overridable seam for tests; lazy-imports aioboto3."""
        import aioboto3
        from botocore.config import Config

        session = aioboto3.Session()
        async with session.client(
            "s3",
            endpoint_url=self.endpoint,
            region_name=self.region,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
            config=Config(s3={"addressing_style": self.addressing_style}),
        ) as s3:
            await s3.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType=content_type)

    async def _put_blob_checked(self, key: str, data: bytes, content_type: str) -> None:
        """When the connection drops after the body is sent, aiohttp re-sends the PUT with the
        drained body: storage keeps the first copy and answers the re-send with 400 InvalidArgument.
        A HEAD that fails or times out is taken as storage being down, so nothing is re-uploaded."""
        for attempt in (1, 2):
            started = time.monotonic()
            try:
                await self._put_blob(key, data, content_type)
            except Exception as exc:
                put_error = exc
            else:
                if attempt > 1:
                    logger.info("[ARCHIVE] put succeeded on retry (key=%s)", key)
                return
            logger.warning(
                "[ARCHIVE] put error on attempt %d after %.1fs (key=%s, %d B): %s",
                attempt, time.monotonic() - started, key, len(data), _describe_s3_error(put_error),
            )
            try:
                head = await asyncio.wait_for(self.head_blob(key), _HEAD_AFTER_PUT_ERROR_TIMEOUT_SECONDS)
            except Exception as exc:
                logger.warning(
                    "[ARCHIVE] HEAD after put error failed (key=%s): %s — not retrying",
                    key, _describe_s3_error(exc),
                )
                raise put_error
            if head is not None and _is_same_blob(head, data):
                logger.warning(
                    "[ARCHIVE] put error, but the object is stored intact (key=%s) — treating as stored", key
                )
                return
            logger.warning(
                "[ARCHIVE] key=%s attempt=%d head_len=%s head_etag=%s want_len=%d want_md5=%s"
                " — object %s after put error, %s",
                key, attempt, (head or {}).get("ContentLength"), (head or {}).get("ETag"),
                len(data), hashlib.md5(data, usedforsecurity=False).hexdigest(),
                "missing" if head is None else "differs",
                "uploading again" if attempt == 1 else "giving up",
            )
        raise put_error

    # ------------------------------------------------------------------
    # Read side (AppFactory-148). Every entry point takes a ref/key that the
    # CALLER has already authorized — ownership checks live at the tool and
    # route layers, before anything here runs (presigned URLs are bearer
    # capabilities: a check after minting is no check at all).
    # ------------------------------------------------------------------

    async def get_ref(self, ref_id: str) -> Optional[Dict[str, Any]]:
        """The locator record for a ref, or None. Mongo-only — never touches S3."""
        if self.refs is None or not ref_id:
            return None
        return await self.refs.find_one({"_id": ref_id})

    async def head_blob(self, object_key: str) -> Optional[Dict[str, Any]]:
        """HEAD an object: metadata dict, or None when the object is gone.

        archive_refs is an advisory index, not a 1:1 mirror of the bucket —
        the object can be missing (retention expired, failed PUT half) while
        the record survives. Callers turn None into an explicit agent-facing
        error; any non-404 failure propagates as a real storage error.
        """
        try:
            return await self._head_blob(object_key)
        except Exception as exc:
            if _is_missing_key(exc):
                return None
            raise

    async def open_blob_stream(self, object_key: str) -> Optional[AsyncIterator[bytes]]:
        """The object's bytes in chunks, or None when the object is gone.

        The GET is made and its first chunk read before this returns, so a
        missing object or a storage error surfaces while an HTTP caller can
        still choose the status; after that chunks pass through one at a time."""
        chunks = self._get_blob_stream(object_key)
        try:
            first = await anext(chunks)
        except StopAsyncIteration:
            first = b""
        except Exception as exc:
            if _is_missing_key(exc):
                return None
            raise
        return _prepend(first, chunks)

    async def presign_get(
        self, object_key: str, ttl_seconds: int, *, content_disposition: Optional[str] = None
    ) -> str:
        """Mint a short-TTL presigned GET for one object. Local SigV4 — no bytes move.

        ``content_disposition`` is signed in as ``response-content-disposition``:
        the storage answers with it instead of the object's own headers (objects
        are written without one), so a browser saves the file under that name."""
        return await self._presign(object_key, int(ttl_seconds), content_disposition)

    async def presign_put(
        self, object_key: str, ttl_seconds: int, *, content_type: str, metadata: Dict[str, str]
    ) -> str:
        """Mint a scope-bound PUT URL; caller must finalize metadata afterwards."""
        return await self._presign_put(object_key, int(ttl_seconds), content_type, metadata)

    async def scan_lines(
        self,
        object_key: str,
        matcher: Callable[[str], bool],
        *,
        max_matches: int,
        max_scan_bytes: int,
        line_max_bytes: int = _SCAN_LINE_MAX_BYTES,
    ) -> Dict[str, Any]:
        """Stream an object line by line and collect matching lines.

        Constant memory by construction: only one chunk and one bounded line
        buffer are ever held — a line longer than ``line_max_bytes`` is matched
        against its first slice, flagged ``line_truncated``, and skipped to the
        next newline. ``max_scan_bytes`` bounds total S3 reads; when it cuts
        the scan short, ``scan_complete`` is False and the half-line left
        dangling is dropped rather than matched — it may continue past the cut,
        so counting it would invent a line. The one exception is a dangling
        piece already longer than ``line_max_bytes``: the cap fires first and
        emits its first slice as a truncated line, which is a real match on
        real bytes, so it counts toward ``lines_scanned`` and the matches.
        """
        matches: list[Dict[str, Any]] = []
        total = 0
        scanned = 0
        line_no = 0
        truncated_lines = 0
        buf = bytearray()
        skipping = False
        complete = True

        def flush(data: bytes, truncated: bool) -> None:
            nonlocal total, line_no, truncated_lines
            line_no += 1
            if truncated:
                # Counted whether or not the slice matches: a needle PAST the
                # cap yields no match and would otherwise leave zero trace —
                # the aggregate count is the caller's only honesty signal.
                truncated_lines += 1
            text = data.decode("utf-8", errors="replace")
            # Strip CR only on a WHOLE line (CRLF, so `$` anchors see the end).
            # On a truncated slice the last byte is arbitrary payload that just
            # happens to be 0x0D — stripping it would corrupt the matched text.
            if not truncated and text.endswith("\r"):
                text = text[:-1]
            if matcher(text):
                total += 1
                if len(matches) < max_matches:
                    m: Dict[str, Any] = {"line": line_no, "text": text[:_MATCH_TEXT_MAX_CHARS]}
                    if truncated:
                        m["line_truncated"] = True
                    matches.append(m)

        last_yield = time.monotonic()
        async for chunk in self._get_blob_stream(object_key):
            budget = max_scan_bytes - scanned
            if budget <= 0:
                complete = False
                break
            if len(chunk) > budget:
                chunk = chunk[:budget]
                complete = False
            scanned += len(chunk)
            start = 0
            while True:
                if time.monotonic() - last_yield >= _SCAN_YIELD_INTERVAL_SECONDS:
                    # sleep(0) is NOT enough: it grants one loop pass, but due
                    # timers are collected before this task's next sync burn,
                    # so waiters (heartbeats, wait_for deadlines) starve
                    # anyway (measured: 5 yields, 1 heartbeat tick). A real
                    # 1ms sleep parks this task BEHIND everything already due.
                    await asyncio.sleep(0.001)
                    last_yield = time.monotonic()
                nl = chunk.find(b"\n", start)
                if nl == -1:
                    if not skipping:
                        buf.extend(chunk[start:])
                        if len(buf) > line_max_bytes:
                            flush(bytes(buf[:line_max_bytes]), True)
                            buf.clear()
                            skipping = True
                    break
                if skipping:
                    skipping = False  # the long line ends here; it was already flushed truncated
                else:
                    buf.extend(chunk[start:nl])
                    if len(buf) > line_max_bytes:
                        # The line finished inside this chunk but already blew
                        # the cap. Same truncated flush as the cross-chunk path
                        # — otherwise cap enforcement (and the truncation flag)
                        # would depend on where chunk boundaries happened to
                        # fall, not on the line's actual size.
                        flush(bytes(buf[:line_max_bytes]), True)
                    else:
                        flush(bytes(buf), False)
                    buf.clear()
                start = nl + 1
            if not complete:
                break

        if buf and complete and not skipping:
            flush(bytes(buf), False)  # trailing line without a final newline

        return {
            "matches": matches,
            "total_matches": total,
            "scanned_bytes": scanned,
            "scan_complete": complete,
            "lines_scanned": line_no,
            "lines_truncated": truncated_lines,
        }

    # ---- S3 read seams (overridable in tests; lazy-import aioboto3) ----

    def _s3_client(self):
        import aioboto3
        from botocore.config import Config

        session = aioboto3.Session()
        return session.client(
            "s3",
            endpoint_url=self.endpoint,
            region_name=self.region,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
            config=Config(s3={"addressing_style": self.addressing_style}),
        )

    async def _head_blob(self, object_key: str) -> Dict[str, Any]:
        async with self._s3_client() as s3:
            return await s3.head_object(Bucket=self.bucket, Key=object_key)

    async def _presign(
        self, object_key: str, ttl_seconds: int, content_disposition: Optional[str] = None
    ) -> str:
        params: Dict[str, Any] = {"Bucket": self.bucket, "Key": object_key}
        if content_disposition:
            params["ResponseContentDisposition"] = content_disposition
        async with self._s3_client() as s3:
            return await s3.generate_presigned_url("get_object", Params=params, ExpiresIn=ttl_seconds)

    async def _presign_put(
        self, object_key: str, ttl_seconds: int, content_type: str, metadata: Dict[str, str]
    ) -> str:
        async with self._s3_client() as s3:
            return await s3.generate_presigned_url(
                "put_object",
                Params={
                    "Bucket": self.bucket,
                    "Key": object_key,
                    "ContentType": content_type,
                    "Metadata": metadata,
                },
                ExpiresIn=ttl_seconds,
            )

    async def _get_blob_stream(self, object_key: str):
        """Yield the object's bytes in chunks without ever holding the whole body."""
        async with self._s3_client() as s3:
            resp = await s3.get_object(Bucket=self.bucket, Key=object_key)
            body = resp["Body"]
            iter_chunks = getattr(body, "iter_chunks", None)
            if iter_chunks is not None:
                async for chunk in iter_chunks(65536):
                    yield chunk
            else:  # older aiobotocore: fall back to bounded read()s
                while True:
                    chunk = await body.read(65536)
                    if not chunk:
                        break
                    yield chunk
