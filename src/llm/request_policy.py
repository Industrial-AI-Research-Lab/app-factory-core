"""Typed request-value policies shared by LLM call boundaries."""

from __future__ import annotations

from typing import Final, TypeAlias


class UnsetType:
    """Marker for an optional argument that the caller did not provide."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "UNSET"


UNSET: Final = UnsetType()
TemperatureInput: TypeAlias = float | None | UnsetType


def resolve_temperature_input(
    temperature: TemperatureInput,
    *,
    default_temperature: float,
) -> float | None:
    """Apply the client default only when temperature was not provided."""
    if temperature is UNSET:
        return default_temperature
    return temperature
