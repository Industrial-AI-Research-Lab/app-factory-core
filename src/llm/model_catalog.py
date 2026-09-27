"""Normalization, filtering, sorting, and pagination for the LLM catalog."""

from __future__ import annotations

import math
from typing import Any, Iterable, Optional


REASONING_PARAMETERS = frozenset({"reasoning", "reasoning_effort"})


def _provider_from_id(model_id: str) -> str:
    return model_id.split("/", 1)[0] if "/" in model_id else "unknown"


def _optional_non_negative_number(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or number < 0:
        return None
    return number


def _optional_context_length(value: Any) -> Optional[int]:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    if number <= 0 or str(value).strip() not in {str(number), f"+{number}"}:
        return None
    return number


def _normalize_reasoning_metadata(value: Any) -> Optional[dict[str, Any]]:
    """Normalize model-specific reasoning metadata without changing semantics."""
    if not isinstance(value, dict):
        return None

    normalized: dict[str, Any] = {}
    if "supported_efforts" in value:
        raw_efforts = value["supported_efforts"]
        if raw_efforts is None:
            normalized["supported_efforts"] = None
        elif isinstance(raw_efforts, (list, tuple)):
            normalized["supported_efforts"] = list(
                dict.fromkeys(
                    effort.strip()
                    for effort in raw_efforts
                    if isinstance(effort, str) and effort.strip()
                )
            )

    default_effort = value.get("default_effort")
    if isinstance(default_effort, str) and default_effort.strip():
        normalized["default_effort"] = default_effort.strip()

    for field_name in (
        "default_enabled",
        "mandatory",
        "supports_max_tokens",
    ):
        field_value = value.get(field_name)
        if isinstance(field_value, bool):
            normalized[field_name] = field_value

    return normalized


def normalize_model(model: dict[str, Any]) -> dict[str, Any]:
    """Return a catalog model with stable capability and pricing fields."""
    normalized = dict(model)
    model_id = str(normalized.get("id") or "")
    raw_parameters = normalized.get("supported_parameters")
    supported_parameters = (
        [value for value in raw_parameters if isinstance(value, str)]
        if isinstance(raw_parameters, (list, tuple))
        else []
    )
    derived_reasoning = bool(
        REASONING_PARAMETERS.intersection(supported_parameters)
    ) or isinstance(normalized.get("reasoning"), dict)
    explicit_reasoning = normalized.get("is_reasoning")

    provider = normalized.get("provider")
    if not isinstance(provider, str) or not provider.strip():
        provider = _provider_from_id(model_id)

    input_price = _optional_non_negative_number(normalized.get("input_price"))
    output_price = _optional_non_negative_number(normalized.get("output_price"))

    normalized.update(
        {
            "id": model_id,
            "provider": provider,
            "context_length": _optional_context_length(
                normalized.get("context_length")
            ),
            "input_price": input_price,
            "output_price": output_price,
            "is_free": (
                input_price is not None
                and output_price is not None
                and input_price == 0
                and output_price == 0
            ),
            "is_reasoning": (explicit_reasoning is True) or derived_reasoning,
            "supported_parameters": supported_parameters,
        }
    )
    reasoning = _normalize_reasoning_metadata(normalized.get("reasoning"))
    if reasoning is None:
        normalized.pop("reasoning", None)
    else:
        normalized["reasoning"] = reasoning
    return normalized


def normalize_models(models: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return [normalize_model(model) for model in models if isinstance(model, dict)]


def normalize_openrouter_model(model: dict[str, Any]) -> dict[str, Any]:
    """Convert an OpenRouter model payload into catalog price units."""
    pricing = model.get("pricing") if isinstance(model.get("pricing"), dict) else {}
    prompt_price = _optional_non_negative_number(pricing.get("prompt"))
    completion_price = _optional_non_negative_number(pricing.get("completion"))
    raw_id = str(model.get("id") or "")
    description = model.get("description")

    normalized = {
        "id": raw_id,
        "name": model.get("name"),
        "provider": _provider_from_id(raw_id),
        "description": description[:200] if isinstance(description, str) else "",
        "context_length": model.get("context_length"),
        "input_price": (
            round(prompt_price * 1_000_000, 4) if prompt_price is not None else None
        ),
        "output_price": (
            round(completion_price * 1_000_000, 4)
            if completion_price is not None
            else None
        ),
        "architecture": (
            model.get("architecture")
            if isinstance(model.get("architecture"), dict)
            else {}
        ),
        "supported_parameters": model.get("supported_parameters"),
        "created_at": model.get("created") or 0,
    }
    if "reasoning" in model:
        normalized["reasoning"] = model.get("reasoning")
    return normalize_model(normalized)


def apply_system_price_limit(
    models: Iterable[dict[str, Any]], max_price_per_million: float
) -> list[dict[str, Any]]:
    normalized = normalize_models(models)
    if max_price_per_million <= 0 or max_price_per_million >= 100:
        return normalized

    visible = []
    for model in normalized:
        input_price = model["input_price"]
        output_price = model["output_price"]
        if input_price is None or output_price is None:
            continue
        if (
            model["is_free"]
            or (input_price + output_price) / 2 <= max_price_per_million
        ):
            visible.append(model)
    return visible


def build_catalog_page(
    models: Iterable[dict[str, Any]],
    *,
    max_price_per_million: float,
    q: Optional[str] = None,
    provider: Optional[list[str]] = None,
    reasoning: Optional[bool] = None,
    free: Optional[bool] = None,
    min_input_price: Optional[float] = None,
    max_input_price: Optional[float] = None,
    min_output_price: Optional[float] = None,
    max_output_price: Optional[float] = None,
    min_context_length: Optional[int] = None,
    sort_by: Optional[str] = None,
    sort_dir: str = "asc",
    offset: int = 0,
    limit: Optional[int] = None,
) -> dict[str, Any]:
    """Build the public page contract from cached or freshly fetched models."""
    visible = apply_system_price_limit(models, max_price_per_million)
    result = visible

    if q:
        needle = q.casefold()
        result = [
            model
            for model in result
            if any(
                needle in str(model.get(field) or "").casefold()
                for field in ("id", "name", "provider", "description")
            )
        ]

    if provider:
        selected = {value.casefold() for value in provider}
        result = [
            model
            for model in result
            if str(model.get("provider") or "").casefold() in selected
        ]

    if reasoning is not None:
        result = [model for model in result if model["is_reasoning"] is reasoning]
    if free is True:
        result = [model for model in result if model["is_free"]]
    elif free is False:
        result = [
            model
            for model in result
            if any(
                price is not None and price > 0
                for price in (model["input_price"], model["output_price"])
            )
        ]

    price_filters = (
        ("input_price", min_input_price, max_input_price),
        ("output_price", min_output_price, max_output_price),
    )
    for field, minimum, maximum in price_filters:
        if minimum is not None:
            result = [
                model
                for model in result
                if model[field] is not None and model[field] >= minimum
            ]
        if maximum is not None:
            result = [
                model
                for model in result
                if model[field] is not None and model[field] <= maximum
            ]

    if min_context_length is not None:
        result = [
            model
            for model in result
            if model["context_length"] is not None
            and model["context_length"] >= min_context_length
        ]

    total = len(result)
    reverse = sort_dir == "desc"
    if sort_by == "avg_price":
        known = [
            model
            for model in result
            if model["input_price"] is not None and model["output_price"] is not None
        ]
        unknown = [
            model
            for model in result
            if model["input_price"] is None or model["output_price"] is None
        ]
        result = sorted(
            known,
            key=lambda model: (
                (model["input_price"] + model["output_price"]) / 2,
                model["id"],
            ),
            reverse=reverse,
        ) + sorted(unknown, key=lambda model: model["id"])
    elif sort_by in {"input_price", "output_price"}:
        known = [model for model in result if model[sort_by] is not None]
        unknown = [model for model in result if model[sort_by] is None]
        result = sorted(
            known,
            key=lambda model: (model[sort_by], model["id"]),
            reverse=reverse,
        ) + sorted(unknown, key=lambda model: model["id"])
    elif sort_by == "name":
        result = sorted(
            result,
            key=lambda model: (
                str(model.get("name") or model["id"]).casefold(),
                model["id"],
            ),
            reverse=reverse,
        )
    elif sort_by == "provider":
        result = sorted(
            result,
            key=lambda model: (
                model["provider"].casefold(),
                str(model.get("name") or "").casefold(),
                model["id"],
            ),
            reverse=reverse,
        )

    page = result[offset:]
    if limit is not None:
        page = page[:limit]

    return {
        "models": page,
        "total": total,
        "total_count": len(visible),
        "filtered_count": total,
        "returned_count": len(page),
        "offset": offset,
        "limit": limit,
        "has_more": offset + len(page) < total,
        "available_providers": sorted(
            {model.get("provider") or "unknown" for model in visible}
        ),
    }
