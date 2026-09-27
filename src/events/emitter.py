"""
Event Emitter for Real-time Updates

Emits events to UI via Server-Sent Events (SSE).
Allows UI to see:
- Agent actions
- Task progress
- Approval requests
- Errors
"""

from typing import Dict, Any, List, Callable, Optional
import asyncio
import logging
import os
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID
import json

from uuid_utils import uuid7  # UUID v7: time-ordered, lexicographically sortable
                              # Move to stdlib `from uuid import uuid7` on Python 3.13+

logger = logging.getLogger(__name__)


# Persist agent.streaming.*.delta events to MongoDB. Off by default — deltas are
# high-volume and ephemeral, so the SSE channel is sufficient for normal use.
# Flip to "true" temporarily to capture per-delta timing for post-mortem
# investigation (e.g. "did the critic LLM keep streaming after the auction
# wait_for cancelled?"). Read once at import — restart the process to toggle.
_PERSIST_STREAM_DELTAS = os.getenv("PERSIST_STREAM_DELTAS", "false").lower() == "true"


# Ceiling on a single event field before it is replaced with a bounded preview.
# Tool `result`/`params` are the only unbounded fields the system emits; a
# pathological one (the ~17 MiB MCP result from 2026-06-10) must not reach the
# in-memory history, the SSE queue, or the Mongo insert (16 MB BSON cap)
# verbatim — the insert crashes and the UI feed chokes. 256 KiB is generous for
# legitimate params/results yet far under the BSON ceiling. ArchiveStore is the
# parallel guard on the LLM-facing copy; this one protects the event sinks.
_EVENT_FIELD_MAX_BYTES = 256 * 1024
_EVENT_PREVIEW_HEAD_BYTES = 1024
_EVENT_PREVIEW_TAIL_BYTES = 2048
_BOUNDED_EVENT_FIELDS = ("result", "params")


def _bound_event_payload(data: Dict[str, Any]) -> Dict[str, Any]:
    """Swap an oversized tool ``result``/``params`` for a bounded preview.

    Returns ``data`` unchanged when nothing is oversized; otherwise a shallow
    copy with the offending field(s) replaced — the caller's dict, and the live
    tool result it references, are never mutated. One canonical guard in front
    of every sink (history, SSE, Mongo), so a single fat payload can neither
    crash the event insert nor bloat the feed.
    """
    if not isinstance(data, dict):
        return data
    bounded = None
    for key in _BOUNDED_EVENT_FIELDS:
        if key not in data:
            continue
        raw = json.dumps(data[key], ensure_ascii=False, default=str).encode(
            "utf-8", errors="replace"
        )
        if len(raw) <= _EVENT_FIELD_MAX_BYTES:
            continue
        if bounded is None:
            bounded = dict(data)
        head = raw[:_EVENT_PREVIEW_HEAD_BYTES].decode("utf-8", errors="replace")
        tail = raw[-_EVENT_PREVIEW_TAIL_BYTES:].decode("utf-8", errors="replace")
        bounded[key] = {
            "_truncated": True,
            "_original_bytes": len(raw),
            "preview": f"{head}\n...[elided — {len(raw)} bytes]...\n{tail}",
        }
    return bounded if bounded is not None else data


def _sse_json_default(o):
    """Fallback encoder for `json.dumps(default=...)` used by the SSE stream.

    Event payloads carry data that's already passed through Mongo / Pydantic
    layers elsewhere, so by the time it reaches the SSE generator we expect
    JSON-native types. But some paths slip non-native objects through:

    - artifact_store rows: `updated_at` is a `datetime` (BSON-native, never
      coerced to ISO unless the FastAPI response model handles it — the SSE
      path is hand-rolled `json.dumps` and does not).
    - tenant/run/approval IDs occasionally arrive as `uuid.UUID` instead of
      `str` when emitters pass raw objects.
    - LLM cost rows can carry `Decimal`.

    Anything not in the table below still raises `TypeError`, which the SSE
    generator catches per-event and logs — better to drop one event than to
    bury an unknown serialization bug under `default=str`.
    """
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    if isinstance(o, UUID):
        return str(o)
    if isinstance(o, Decimal):
        return str(o)
    raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")


# ─────────────────────────────────────────────────────────────────────────────
# run_id CONTRACT (sister to the project_id strict check in emit() below)
# ─────────────────────────────────────────────────────────────────────────────
# Events are run-scoped by default. run_id is what lets the UI filter by run
# (RunsPanel selection, "show events from this run only"), and what lets the
# DB cleanly partition rows by run for revert / fork operations.
#
# The events table on project 1e2602ac-... had 242 rows. 223 of them had
# run_id=null. That's not a few stragglers — that's a structural plumbing
# gap in the auction/streaming/task/tool layer. Examples of the gap:
#   - phase_runner.run_phase passes run_id=None to auction.run_auction
#     (see the TODO at phase_runner.py around the run_auction call).
#   - agents/base.py streaming emits use shared_context.run_id, but
#     shared_context.run_id is itself None for most non-phase entry paths.
#   - task_executor's task_attempt I added in commit X reads
#     shared_context.run_id, which is None for the same reason.
#
# Fixing every gap is a real refactor across orchestrator → workflow_engine
# → phase_runner → auction → agent. Until that's done, raising on every
# auction event would prevent any project from running. So this check is
# TWO-MODE:
#
#   STRICT_RUN_ID=true   → missing run_id raises ValueError, hard fail
#   STRICT_RUN_ID=false  → missing run_id logs a WARN (default — keeps
#                          the system running while making the gap loud)
#
# Use the strict mode in a clean test environment to see exactly which
# events need plumbing. In normal dev, leave it off and grep backend.log
# for "[EMIT.run_id]" to find violations.
#
# The allowlist below names events that legitimately have NO run_id by
# design — they predate any specific run (project_started fires before
# the first run is committed; container_created is system-level; etc.).
# These never log or raise even in strict mode. Adding to this list is a
# design decision that should be defended in the comment that touches it.
# ─────────────────────────────────────────────────────────────────────────────
_STRICT_RUN_ID = os.getenv("STRICT_RUN_ID", "false").lower() == "true"

_RUN_ID_OPTIONAL_EVENTS = frozenset({
    # Project lifecycle — fire at run boundaries; the run itself is the
    # subject, not a member.
    "project_started",
    "project_completed",
    "project_failed",
    "project_stopping",
    "project_reverted",
    # Container is system-level (one container can outlive multiple runs in
    # principle).
    "container_created",
    # Snapshots are addressable independently of runs (revert from any run
    # to any snapshot); they carry run_id inside data when relevant but
    # don't require it at the emitter level.
    "snapshot_created",
})

# Per-event-type one-shot warning dedup. Without this every auction emit
# floods the log with the same warning. We want ONE line per type per
# process so the user sees the surface area without noise.
_RUN_ID_WARNED: set = set()


class EventEmitter:
    """
    Event system for broadcasting updates to UI.
    
    Supports:
    - SSE (Server-Sent Events) for web UI
    - WebSocket connections
    - Local callbacks for testing
    - MongoDB persistence (optional)
    """
    
    def __init__(self, storage_backend=None):
        """
        Initialize event emitter
        
        Args:
            storage_backend: Optional MongoDB backend for event persistence
        """
        # Subscribers (SSE connections)
        self.subscribers: Dict[str, List[asyncio.Queue]] = {}
        
        # Event history (for replay/debugging)
        self.event_history: List[Dict] = []
        self.max_history = 1000
        
        # Callbacks (for testing/local usage)
        self.callbacks: Dict[str, List[Callable]] = {}
        
        # Shutdown flag
        self._shutting_down = False
        
        # Storage backend for persistence
        self.storage = storage_backend
    
    async def emit(self, event_type: str, run_id: Optional[str], data: Dict[str, Any]):
        """
        Emit an event.

        Args:
            event_type: Event type (e.g., "agent.started", "task.completed")
            run_id:     Run this event belongs to. Pass None for project-level
                        lifecycle events (project_started/completed/failed,
                        container_created). Run-scoped events MUST pass the
                        owning run_id so the UI can filter when viewing
                        previous runs.
            data:       Event payload. MUST include "project_id" (a real
                        project UUID). run_id is injected into data
                        automatically — callers should not duplicate it.

        Raises:
            ValueError: if data is missing "project_id", or it is None / empty
                        / the literal string "global". This is a STRICT
                        contract — see the comment above the validation
                        below before considering an exemption.
        """
        # ─────────────────────────────────────────────────────────────────────
        # STRICT project_id CONTRACT — DO NOT BYPASS
        # ─────────────────────────────────────────────────────────────────────
        # SSE subscribers are bucketed by data["project_id"]. The previous
        # default (`enriched_data.get("project_id", "global")`) silently
        # routed events with no project_id into the "global" bucket — the
        # per-project UI subscriber never received them.
        #
        # This was a real bug, not a theoretical one: the new task_attempt
        # emit in phase_runner / task_executor (commit X) omitted
        # project_id, so on project e7b5e041-... zero task_attempt events
        # reached the UI even though the emit completed without error. The
        # symptom was a "Coding Agent is processing… waiting for first
        # response chunk" ghost panel that never cleared because the agent
        # signal never arrived. Diagnosis required tracing through the
        # emitter; the failure was completely silent.
        #
        # NEVER replace this raise with a default fallback. NEVER add a
        # `if event_type in {...}: skip_check` exemption — every event in
        # the system today already has a project context (verified across
        # base.py, phase_runner.py, task_executor.py, project_manager.py,
        # auction.py, deploy/*.py, api/routes/*.py at the time of writing).
        # If you find an emit site that doesn't have project_id available,
        # the right fix is to plumb it through to that call site, not to
        # weaken this check.
        #
        # If you genuinely need a system-wide event with no project, add a
        # separate emit_system_event() method on this class — do not pollute
        # the project channel with the literal string "global".
        # ─────────────────────────────────────────────────────────────────────
        project_id_in = data.get("project_id") if isinstance(data, dict) else None
        if not project_id_in or project_id_in == "global":
            raise ValueError(
                f"emit({event_type!r}): data['project_id'] is required and must be a real "
                f"project id (got {project_id_in!r}). See the STRICT contract comment in "
                f"events/emitter.py:emit() before changing this check."
            )

        # ─────────────────────────────────────────────────────────────────────
        # run_id contract — see the long comment block at module top for the
        # full rationale, the allowlist, and the strict-vs-warn modes.
        # ─────────────────────────────────────────────────────────────────────
        if run_id is None and event_type not in _RUN_ID_OPTIONAL_EVENTS:
            if _STRICT_RUN_ID:
                raise ValueError(
                    f"emit({event_type!r}): run_id is required for run-scoped events "
                    f"and must not be None. Either plumb run_id through to this emit "
                    f"site, or add {event_type!r} to _RUN_ID_OPTIONAL_EVENTS in "
                    f"events/emitter.py with a justification comment. See the "
                    f"run_id CONTRACT block at the top of this file."
                )
            if event_type not in _RUN_ID_WARNED:
                _RUN_ID_WARNED.add(event_type)
                print(
                    f"⚠️  [EMIT.run_id] {event_type!r} emitted with run_id=None — "
                    f"this event is run-scoped and should carry a run_id. Logged once "
                    f"per process. Set STRICT_RUN_ID=true to make this raise."
                )

        event_id = str(uuid7())
        enriched_data = {**data, "run_id": run_id} if run_id is not None else data
        # Bound an oversized tool result/params before ANY sink sees it — history,
        # SSE, and the Mongo insert all read enriched_data below.
        enriched_data = _bound_event_payload(enriched_data)
        event = {
            "event_id": event_id,
            "type": event_type,
            "data": enriched_data,
            "timestamp": datetime.utcnow().isoformat(),
        }

        # Store in history (in-memory)
        self.event_history.append(event)
        if len(self.event_history) > self.max_history:
            self.event_history.pop(0)

        # project_id is guaranteed non-empty and not "global" by the strict
        # validation at the top of this method. The previous `.get(..., "global")`
        # fallbacks are intentionally gone — see the contract comment above.
        project_id = enriched_data["project_id"]

        # Notify SSE subscribers BEFORE awaiting storage.save_event. The
        # mint→put_nowait path contains no `await`, so two concurrent emits
        # on the same project_id cannot interleave between mint and queue
        # delivery — queue order matches event_id order, which the FE
        # watermark (id <= lastSeen → drop, no second chance) requires.
        # Previously the await on save_event was the yield point, and Motor
        # connection-pool reordering could land event B's queue.put_nowait
        # before event A's, causing the FE to drop A and Mongo's replay
        # query ({$gt: B}) to never re-deliver it.
        # NOTE: Use non-blocking queue writes to avoid backpressure stalls.
        # If any subscriber queue is full or stuck, we drop the event for
        # that subscriber instead of blocking the whole system.
        if project_id in self.subscribers:
            for queue in self.subscribers[project_id]:
                try:
                    queue.put_nowait(event)
                except Exception:
                    pass  # Queue might be closed or full

        # Notify global subscribers
        if "global" in self.subscribers:
            for queue in self.subscribers["global"]:
                try:
                    queue.put_nowait(event)
                except Exception:
                    pass

        # Persist to MongoDB (if storage backend available).
        # Delta events are skipped by default (high-volume, ephemeral) — set
        # PERSIST_STREAM_DELTAS=true to capture them for investigation.
        # Runs AFTER subscriber notification (see comment above). The
        # durability window (event delivered to FE but not yet in Mongo) is
        # bounded by this single await; if save fails, subscribers still
        # received the event and a subsequent reconnect's replay won't see
        # it — same loss profile as the pre-existing print-and-continue
        # except handler, so net durability is unchanged.
        is_delta = event_type.endswith('.delta')
        if self.storage and (not is_delta or _PERSIST_STREAM_DELTAS):
            try:
                await self.storage.save_event(
                    project_id, event_type, enriched_data,
                    event_id=event_id, run_id=run_id,
                )
            except Exception as e:
                print(f"⚠️  Failed to persist event to MongoDB: {e}")

        # Call registered callbacks
        if event_type in self.callbacks:
            for callback in self.callbacks[event_type]:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(event)
                    else:
                        callback(event)
                except Exception as e:
                    print(f"Callback error: {e}")
    
    def subscribe(self, project_id: str = "global") -> asyncio.Queue:
        """
        Subscribe to events for a project.
        
        Args:
            project_id: Project to subscribe to (or "global" for all)
        
        Returns:
            Queue that will receive events
        """
        queue = asyncio.Queue(maxsize=100)
        
        if project_id not in self.subscribers:
            self.subscribers[project_id] = []
        
        self.subscribers[project_id].append(queue)
        
        return queue
    
    def unsubscribe(self, project_id: str, queue: asyncio.Queue):
        """Unsubscribe from events"""
        if project_id in self.subscribers:
            try:
                self.subscribers[project_id].remove(queue)
            except ValueError:
                pass
    
    def register_callback(self, event_type: str, callback: Callable):
        """
        Register callback for event type.
        
        Useful for testing or local integrations.
        """
        if event_type not in self.callbacks:
            self.callbacks[event_type] = []
        
        self.callbacks[event_type].append(callback)
    
    def get_history(
        self,
        event_type: Optional[str] = None,
        project_id: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict]:
        """
        Get event history.
        
        Args:
            event_type: Filter by type
            project_id: Filter by project
            limit: Max events to return
        
        Returns:
            List of events (newest first)
        """
        events = self.event_history.copy()
        
        # Filter by type
        if event_type:
            events = [e for e in events if e["type"] == event_type]
        
        # Filter by project
        if project_id:
            events = [
                e for e in events
                if e["data"].get("project_id") == project_id
            ]
        
        # Return newest first
        events.reverse()
        return events[:limit]
    
    async def stream_events(self, queue: asyncio.Queue):
        """
        Generator for SSE streaming.
        
        Yields:
            SSE-formatted event strings
        """
        try:
            while not self._shutting_down:
                try:
                    # Use timeout to allow checking shutdown flag
                    event = await asyncio.wait_for(queue.get(), timeout=1.0)
                    
                    # Format as SSE. `id:` is the canonical resume pointer per
                    # SSE spec — clients auto-send it back as Last-Event-ID
                    # on reconnect. Use event_id (UUID v7, time-ordered) so
                    # the backend can resume via `event_id > last_event_id`
                    # in Mongo and both live and replay paths emit the same ID.
                    #
                    # `default=_sse_json_default` handles datetime (e.g. the
                    # `updated_at` field on artifact_store rows that ride
                    # along inside approval `data.context_snapshot.artifacts`).
                    # Without it, the stdlib encoder raises `TypeError: Object
                    # of type datetime is not JSON serializable`, kills the
                    # SSE response mid-stream, and the FE never sees the
                    # output approval event — verified on project 21b8c9e1
                    # where the artifact landed in Mongo (seq 17) but the
                    # chat only showed it after a full page refresh because
                    # the live channel had died. Errors here detach the
                    # current event and continue (logged once per process)
                    # so one bad payload doesn't blackhole the rest of the
                    # stream and force users to refresh.
                    try:
                        data_json = json.dumps(event['data'], default=_sse_json_default)
                    except (TypeError, ValueError) as e:
                        logger.exception(
                            "[SSE] failed to serialize event data type=%s event_id=%s err=%s",
                            event.get('type'), event.get('event_id'), e,
                        )
                        continue
                    yield f"event: {event['type']}\n"
                    yield f"data: {data_json}\n"
                    yield f"id: {event['event_id']}\n\n"
                except asyncio.TimeoutError:
                    # Send keepalive comment
                    yield ": keepalive\n\n"
                    continue
                
        except asyncio.CancelledError:
            pass
    
    async def shutdown(self):
        """Shutdown event emitter and close all connections"""
        self._shutting_down = True
        
        # Close all subscriber queues
        for project_id, queues in self.subscribers.items():
            for queue in queues:
                try:
                    # Put a sentinel value to unblock any waiting gets
                    if not queue.full():
                        await queue.put(None)
                except Exception:
                    pass
        
        # Clear subscribers
        self.subscribers.clear()
    
    def clear_history(self):
        """Clear event history"""
        self.event_history.clear()
    
    def get_stats(self) -> Dict:
        """Get emitter statistics"""
        return {
            "total_events": len(self.event_history),
            "subscribers": {
                project_id: len(queues)
                for project_id, queues in self.subscribers.items()
            },
            "callbacks_registered": sum(len(cbs) for cbs in self.callbacks.values())
        }

