"""Prepare a Run's durable trace identity before its first database insert."""

from __future__ import annotations

import logging
import secrets
from typing import Any


logger = logging.getLogger(__name__)


def generate_traceparent() -> str:
    """Generate nonzero W3C identifiers without a recording tracer or I/O."""
    trace_id = secrets.randbelow((1 << 128) - 1) + 1
    span_id = secrets.randbelow((1 << 64) - 1) + 1
    return f"00-{trace_id:032x}-{span_id:016x}-01"


def prepare_run_trace(project_id: str, run_id: str) -> tuple[dict[str, str], Any]:
    """Return insert-ready telemetry and a root span to finish after the insert."""
    span = None
    try:
        from telemetry.tracer import get_tracer

        tracer = get_tracer()
        if not tracer.enabled or tracer.tracer is None:
            logger.info(
                "[OTEL] action=prepare_run_trace project_id=%s run_id=%s "
                "status=prepared mode=disabled",
                project_id,
                run_id,
            )
            return {"traceparent": generate_traceparent()}, None

        from opentelemetry.context import Context
        from telemetry.run_context import RunTraceContext, _bind_context, _reset_context

        token = _bind_context(RunTraceContext(project_id, run_id, None))
        try:
            span = tracer.tracer.start_span(
                "run",
                context=Context(),
                attributes={"project.id": project_id, "run_id": run_id},
            )
        finally:
            _reset_context(token)
        context = span.get_span_context()
        if not context.is_valid:
            raise ValueError("run root has no valid W3C identity")
        traceparent = (
            f"00-{context.trace_id:032x}-{context.span_id:016x}"
            f"-{int(context.trace_flags):02x}"
        )
        logger.info(
            "[OTEL] action=prepare_run_trace project_id=%s run_id=%s "
            "status=prepared mode=enabled",
            project_id,
            run_id,
        )
        return {"traceparent": traceparent}, span
    except Exception as error:
        logger.error(
            "[OTEL] action=prepare_run_trace project_id=%s run_id=%s "
            "status=degraded reason=root_span_failed error=%s",
            project_id,
            run_id,
            type(error).__name__,
        )
        return {
            "traceparent": generate_traceparent(),
            "status": "degraded",
            "error": "root_span_failed",
        }, span


def finish_run_trace(span, *, project_id: str, run_id: str) -> None:
    """Report root export failures without replacing a database insert error."""
    if span is None:
        return
    try:
        span.end()
    except Exception as error:
        logger.error(
            "[OTEL] action=end_run_root project_id=%s run_id=%s "
            "status=degraded reason=cleanup_failed error=%s",
            project_id,
            run_id,
            type(error).__name__,
        )
