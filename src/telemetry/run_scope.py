"""Reconnect operations to their durable Run identity without hiding spans."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from storage.run_trace_store import RunTraceStore
from telemetry.run_cache import get_run_slot, remember_run_context
from telemetry.run_context import (
    RunTraceContext,
    _active_context,
    _bind_context,
    _current_context,
    _extract_span_context,
    _reset_context,
)
from telemetry.run_diagnostics import report_trace_issue
from telemetry.run_identity import generate_traceparent

if TYPE_CHECKING:
    from telemetry.tracer import AppFactoryTracer

logger = logging.getLogger(__name__)


async def _prepare_run_context(storage, tracer, project_id: str, run_id: str):
    del tracer
    slot = get_run_slot(storage, run_id)
    async with slot.lock:
        if slot.context is not None:
            if slot.context.project_id != project_id:
                raise ValueError("Run belongs to another project")
            saved = slot.context.traceparent
        else:
            store = RunTraceStore(storage)
            saved = await store.read(project_id, run_id)
            if saved is None:
                # Legacy candidates never record spans, including a lost CAS race.
                saved = await store.claim(project_id, run_id, generate_traceparent())
                await report_trace_issue(
                    storage, project_id, run_id, "legacy_trace_initialized"
                )
            elif getattr(store, "telemetry_status", None) == "degraded":
                await report_trace_issue(
                    storage,
                    project_id,
                    run_id,
                    store.telemetry_error or "persisted_trace_failure",
                )
            _extract_span_context(saved)
            remember_run_context(storage, project_id, run_id, saved)
        return saved, _extract_span_context(saved)


def _same_run(project_id: str, run_id: str | None) -> bool:
    active = _current_context()
    if not (
        active
        and active.project_id == project_id
        and active.run_id == run_id
        and active.traceparent
    ):
        return False
    try:
        return _active_context() is not None
    except Exception:
        return False


@asynccontextmanager
async def run_trace_scope(
    storage, tracer: AppFactoryTracer, project_id: str, run_id: str | None
) -> AsyncIterator[None]:
    if not tracer.enabled or not tracer.tracer or _same_run(project_id, run_id):
        yield
        return

    from opentelemetry import context as otel_context, trace

    identity_token = _bind_context(RunTraceContext(project_id, run_id, None))
    attached_token = None
    context_token = None
    try:
        prepared = None
        try:
            if run_id is None:
                raise ValueError("missing_run_id")
            prepared = await _prepare_run_context(storage, tracer, project_id, run_id)
        except Exception as error:
            await report_trace_issue(
                storage,
                project_id,
                run_id,
                "missing_run_id" if run_id is None else type(error).__name__,
            )

        try:
            parent = otel_context.Context()
            if prepared is not None:
                traceparent, span_context = prepared
                parent = trace.set_span_in_context(
                    trace.NonRecordingSpan(span_context), parent
                )
            # A failed lookup must not attach work to an ambient different Run.
            attached_token = otel_context.attach(parent)
            if prepared is not None:
                context_token = _bind_context(
                    RunTraceContext(project_id, run_id, traceparent)
                )
        except Exception as error:
            await report_trace_issue(storage, project_id, run_id, type(error).__name__)
        yield
    finally:
        if context_token is not None:
            _reset_context(context_token)
        if attached_token is not None:
            try:
                otel_context.detach(attached_token)
            except Exception:
                logger.exception(
                    "[OTEL] project_id=%s run_id=%s — context cleanup failed",
                    project_id,
                    run_id,
                )
        _reset_context(identity_token)
