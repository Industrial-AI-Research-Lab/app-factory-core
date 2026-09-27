"""
Telemetry module for AppFactory

Provides OpenTelemetry tracing for the entire system.
"""

from .tracer import get_tracer, AppFactoryTracer

__all__ = ["get_tracer", "AppFactoryTracer"]

