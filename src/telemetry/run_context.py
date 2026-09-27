"""Async-local identity and propagation helpers for a traced Run."""

from __future__ import annotations

import logging
from contextvars import ContextVar, Token
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from telemetry.tracer import AppFactoryTracer


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RunTraceContext:
    project_id: str
    run_id: str | None
    traceparent: str | None


_RUN_TRACE_CONTEXT: ContextVar[RunTraceContext | None] = ContextVar(
    "AppFactory_run_trace_context", default=None
)


def _current_context() -> RunTraceContext | None:
    return _RUN_TRACE_CONTEXT.get()


def _bind_context(context: RunTraceContext) -> Token:
    return _RUN_TRACE_CONTEXT.set(context)


def _reset_context(token: Token) -> None:
    _RUN_TRACE_CONTEXT.reset(token)


def _extract_span_context(traceparent: str):
    from opentelemetry import trace
    from opentelemetry.context import Context
    from opentelemetry.trace.propagation.tracecontext import (
        TraceContextTextMapPropagator,
    )

    extracted = TraceContextTextMapPropagator().extract(
        {"traceparent": traceparent}, context=Context()
    )
    span_context = trace.get_current_span(extracted).get_span_context()
    if not span_context.is_valid:
        raise ValueError("invalid W3C traceparent")
    return span_context


def _active_context() -> tuple[RunTraceContext, Any] | None:
    from opentelemetry import trace

    context = _current_context()
    if context is None or context.run_id is None or context.traceparent is None:
        return None
    root_context = _extract_span_context(context.traceparent)
    active_context = trace.get_current_span().get_span_context()
    if not active_context.is_valid or active_context.trace_id != root_context.trace_id:
        return None
    return context, active_context


def current_run_traceparent(tracer: AppFactoryTracer) -> str | None:
    if not tracer.enabled or not tracer.tracer:
        return None
    try:
        active = _active_context()
        return active[0].traceparent if active else None
    except Exception as error:
        logger.warning(
            "[OTEL] action=current_run_context reason=validation_failed error=%s",
            type(error).__name__,
        )
        return None


def outgoing_trace_headers(tracer: AppFactoryTracer) -> dict[str, str]:
    if not tracer.enabled or not tracer.tracer:
        return {}
    try:
        if _active_context() is None:
            return {}
        from opentelemetry.trace.propagation.tracecontext import (
            TraceContextTextMapPropagator,
        )

        carrier: dict[str, str] = {}
        TraceContextTextMapPropagator().inject(carrier)
        return carrier
    except Exception as error:
        logger.warning(
            "[OTEL] action=inject_run_context reason=injection_failed error=%s",
            type(error).__name__,
        )
        return {}


def _label_active_run_span(span) -> None:
    try:
        context = _current_context()
        if context is None or context.run_id is None:
            return
        span.set_attribute("run_id", context.run_id)
    except Exception as error:
        logger.warning(
            "[OTEL] action=label_run_span reason=processor_failed error=%s",
            type(error).__name__,
        )


def make_run_span_processor() -> Any:
    from opentelemetry.sdk.trace import SpanProcessor

    class RunSpanProcessor(SpanProcessor):
        def on_start(self, span, parent_context=None) -> None:
            del parent_context
            _label_active_run_span(span)

        def on_end(self, span) -> None:
            del span

        def shutdown(self) -> None:
            return None

        def force_flush(self, timeout_millis=30000) -> bool:
            del timeout_millis
            return True

    return RunSpanProcessor()
