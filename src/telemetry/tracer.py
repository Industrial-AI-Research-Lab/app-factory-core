"""
OpenTelemetry Tracer for AppFactory (lazy import friendly)

Centralized tracing helpers with runtime-only OpenTelemetry imports so
static analysis doesn't require OTEL packages.
"""

from __future__ import annotations

import importlib
import logging
import os
import sys
from contextlib import contextmanager
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class AppFactoryTracer:
    """Centralized tracer with lazy/dynamic imports (no top-level OTEL deps)."""

    def __init__(self) -> None:
        self.enabled = os.getenv("OTEL_ENABLED", "false").strip().lower() == "true"
        self.endpoint = (
            os.getenv("OTEL_ENDPOINT", "").strip() or "http://localhost:4318/v1/traces"
        )
        self.service_name = (
            os.getenv("OTEL_SERVICE_NAME", "").strip() or "AppFactory-backend"
        )
        self.tracer = None
        if self.enabled:
            self._setup_tracer()
        else:
            logger.info("[OTEL] enabled=false — tracing is disabled")

    def _lazy_import(self, module: str):
        return importlib.import_module(module)

    def _setup_tracer(self) -> None:
        try:
            trace = self._lazy_import("opentelemetry.trace")
            sdk_trace = self._lazy_import("opentelemetry.sdk.trace")
            exporter_mod = self._lazy_import(
                "opentelemetry.exporter.otlp.proto.http.trace_exporter"
            )
            resources = self._lazy_import("opentelemetry.sdk.resources")
            sdk_export = self._lazy_import("opentelemetry.sdk.trace.export")

            resource = resources.Resource(
                attributes={
                    "service.name": self.service_name,
                    "service.version": os.getenv("APP_VERSION", "0.1.0"),
                    "deployment.environment": os.getenv("ENVIRONMENT", "production"),
                    "telemetry.sdk.name": "AppFactory-tracer",
                    "telemetry.sdk.version": "0.1.0",
                    "telemetry.sdk.language": "python",
                }
            )
            provider = sdk_trace.TracerProvider(resource=resource)
            exporter = exporter_mod.OTLPSpanExporter(endpoint=self.endpoint)
            processor = sdk_export.BatchSpanProcessor(exporter)
            from telemetry.run_context import make_run_span_processor

            provider.add_span_processor(make_run_span_processor())
            provider.add_span_processor(processor)
            trace.set_tracer_provider(provider)
            self.tracer = trace.get_tracer(__name__)
            logger.info(
                "[OTEL] status=initialized service=%s endpoint=%s",
                self.service_name,
                self.endpoint,
            )
        except Exception as e:
            logger.warning(
                "[OTEL] status=disabled service=%s endpoint=%s — initialization failed: %s",
                self.service_name,
                self.endpoint,
                e,
            )
            self.enabled = False

    @contextmanager
    def start_span(
        self,
        name: str,
        attributes: Optional[Dict[str, Any]] = None,
        kind: Optional[Any] = None,
    ):
        if not self.enabled or not self.tracer:
            # Tracing disabled – behave as a no-op context manager.
            yield None
            return

        # Resolve OpenTelemetry dependencies up-front so that failures here
        # degrade gracefully without affecting the body of the span.
        try:
            trace = self._lazy_import("opentelemetry.trace")
            span_kind = kind or trace.SpanKind.INTERNAL
        except Exception as e:
            logger.warning(
                "[OTEL] action=start_span span=%s status=noop — dependency lookup failed: %s",
                name,
                e,
            )
            yield None
            return

        # Enter the OpenTelemetry context separately from the application body. Instrumentation
        # failures must degrade to a no-op, while exceptions raised by the body must still escape.
        try:
            span_manager = self.tracer.start_as_current_span(name, kind=span_kind)
            span = span_manager.__enter__()
        except Exception as e:
            logger.warning(
                "[OTEL] action=start_span span=%s status=noop — tracer failed: %s",
                name,
                e,
            )
            yield None
            return

        body_exception = (None, None, None)
        try:
            try:
                attribute_items = (attributes or {}).items()
            except Exception as e:
                logger.warning(
                    "[OTEL] action=set_attributes span=%s status=dropped — invalid attributes: %s",
                    name,
                    e,
                )
                attribute_items = ()

            for key, value in attribute_items:
                try:
                    from telemetry.run_context import _current_context

                    active_run = _current_context()
                    if key == "run_id" and active_run and active_run.run_id is not None:
                        # The span processor already set the canonical Run identity.
                        continue
                    if isinstance(value, (dict, list)):
                        try:
                            import json

                            value = json.dumps(value)
                        except Exception:
                            value = str(value)
                    if value is None:
                        continue
                    if isinstance(value, str) and len(value) > 4096:
                        value = value[:4096] + "... (truncated)"
                    span.set_attribute(key, value)
                except Exception as e:
                    logger.warning(
                        "[OTEL] action=set_attribute span=%s key=%s status=dropped — %s",
                        name,
                        key,
                        e,
                    )

            try:
                yield span
            except BaseException:
                body_exception = sys.exc_info()
                raise
        finally:
            try:
                span_manager.__exit__(*body_exception)
            except Exception as e:
                # If the application body is already failing, never replace its exception with a
                # telemetry cleanup error. With a successful body, exporter/context cleanup is
                # still best-effort and therefore cannot fail the request or workflow.
                logger.warning(
                    "[OTEL] action=finish_span span=%s status=dropped — cleanup failed: %s",
                    name,
                    e,
                )

    def add_event(
        self, span, name: str, attributes: Optional[Dict[str, Any]] = None
    ) -> None:
        if not self.enabled or not span:
            return
        try:
            span.add_event(name, attributes=attributes or {})
        except Exception as e:
            logger.warning(
                "[OTEL] action=add_event event=%s status=dropped — %s", name, e
            )

    def set_error(self, span, error: Exception) -> None:
        if not self.enabled or not span:
            return
        try:
            trace = self._lazy_import("opentelemetry.trace")
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(error)))
            span.record_exception(error)
        except Exception as e:
            logger.warning("[OTEL] action=set_error status=dropped — %s", e)

    def set_success(self, span) -> None:
        if not self.enabled or not span:
            return
        try:
            trace = self._lazy_import("opentelemetry.trace")
            span.set_status(trace.Status(trace.StatusCode.OK))
        except Exception as e:
            logger.warning("[OTEL] action=set_success status=dropped — %s", e)


_TRACER: Optional[AppFactoryTracer] = None


def get_tracer() -> AppFactoryTracer:
    global _TRACER
    if _TRACER is None:
        _TRACER = AppFactoryTracer()
    return _TRACER


def create_task_with_context(coro):  # coroutine
    """
    Create an asyncio task that preserves OpenTelemetry trace context.

    Without this, asyncio.create_task() starts a new trace context,
    breaking the parent-child span hierarchy.
    """
    import asyncio

    try:
        from opentelemetry import context as otel_context
    except ImportError:
        # OpenTelemetry not installed, fall back to regular create_task
        return asyncio.create_task(coro)

    # Capture current context before creating task
    ctx = otel_context.get_current()

    async def wrapped():
        # Attach the captured context in the new task
        token = otel_context.attach(ctx)
        try:
            return await coro
        finally:
            try:
                otel_context.detach(token)
            except ValueError:
                # Event loop shutdown / GC: attach token may be invalid here (pytest noise).
                pass

    wrapped_coro = wrapped()
    try:
        return asyncio.create_task(wrapped_coro)
    except BaseException:
        wrapped_coro.close()
        raise
