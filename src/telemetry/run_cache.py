"""Bounded Run identities belong to storage, not the Project."""

import asyncio
from collections import OrderedDict
from dataclasses import dataclass, field
from weakref import WeakValueDictionary

from telemetry.run_context import RunTraceContext

_MAX_CACHED_RUNS = 1024


@dataclass
class RunTraceSlot:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    context: RunTraceContext | None = None
    reported: set[tuple[str, str]] = field(default_factory=set)


class _RunTraceCache:
    def __init__(self):
        self.recent: OrderedDict[str | None, RunTraceSlot] = OrderedDict()
        self.live: WeakValueDictionary[str | None, RunTraceSlot] = WeakValueDictionary()

    def get(self, run_id: str | None) -> RunTraceSlot:
        # An evicted slot may still be held by a reader, lock waiter or reporter.
        # Reuse it until those callers finish, without retaining it indefinitely.
        slot = self.live.get(run_id)
        if slot is None:
            slot = RunTraceSlot()
            self.live[run_id] = slot
        self.recent[run_id] = slot
        self.recent.move_to_end(run_id)
        while len(self.recent) > _MAX_CACHED_RUNS:
            self.recent.popitem(last=False)
        return slot


def get_run_slot(storage, run_id: str | None) -> RunTraceSlot:
    # Slot-only test doubles have no lifetime on which to retain a cache.
    namespace = getattr(storage, "__dict__", None)
    if namespace is None:
        return RunTraceSlot()
    cache = namespace.get("_run_trace_cache")
    if cache is None:
        cache = namespace["_run_trace_cache"] = _RunTraceCache()
    return cache.get(run_id)


def remember_run_context(storage, project_id: str, run_id: str, traceparent: str):
    slot = get_run_slot(storage, run_id)
    context = RunTraceContext(project_id, run_id, traceparent)
    if slot.context is not None and slot.context != context:
        raise ValueError("Run trace identity is immutable")
    slot.context = context
    return context
