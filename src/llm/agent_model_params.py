"""Resolution and save-time validation for flat agent model parameters."""

from __future__ import annotations

import logging
import math
from collections.abc import Collection
from dataclasses import dataclass
from typing import Any, Optional

from llm.model_config import (
    ALL_REASONING_EFFORTS,
    ModelCapability,
    ModelConfig,
    get_model_registry,
    normalize_model_id,
)

logger = logging.getLogger(__name__)

MODEL_PARAM_FIELDS = frozenset({"model", "temperature", "reasoning_effort"})
DEFAULT_AGENT_MODEL = "gpt-5-mini"
DEFAULT_AGENT_TEMPERATURE = 1.0


@dataclass(frozen=True)
class EffectiveAgentModelParams:
    """Complete runtime model parameters plus their winning sources."""

    model: str
    temperature: Any
    reasoning_effort: Any
    sources: dict[str, str]


class ModelConfigResolutionError(RuntimeError):
    """Raised when model capabilities cannot be resolved reliably."""


class AgentModelParamsValidationError(ValueError):
    """Stable validation error returned by agent configuration APIs."""

    def __init__(
        self,
        *,
        code: str,
        message: str,
        field: str,
        model: Optional[str] = None,
        allowed: Optional[Any] = None,
        context: Optional[dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.field = field
        self.model = model
        self.allowed = allowed
        self.context = dict(context or {})

    def to_detail(self) -> dict[str, Any]:
        detail: dict[str, Any] = {
            "code": self.code,
            "message": self.message,
            "field": self.field,
        }
        if self.model is not None:
            detail["model"] = self.model
        if self.allowed is not None:
            detail["allowed"] = self.allowed
        detail.update(self.context)
        return detail


def materialize_agent_model_params(doc: dict[str, Any]) -> dict[str, Any]:
    """Return an agent document with the complete flat model contract."""
    normalized = dict(doc)
    model = normalized.get("model", DEFAULT_AGENT_MODEL)
    normalized["model"] = model.strip() if isinstance(model, str) else model
    normalized.setdefault("temperature", DEFAULT_AGENT_TEMPERATURE)
    effort = normalized.get("reasoning_effort")
    if isinstance(effort, str):
        normalized["reasoning_effort"] = effort.strip() or None
    else:
        normalized.setdefault("reasoning_effort", None)
    return normalized


def resolve_effective_agent_model_params(
    *,
    agent_id: str,
    subsystem: str,
    agent_model: Optional[str],
    agent_temperature: Any,
    agent_reasoning_effort: Any,
    run_config: Optional[dict[str, Any]] = None,
    project_model: Optional[str] = None,
    force_project_model: bool = False,
    project_reasoning_effort: Any = None,
    project_temperature: Any = None,
) -> EffectiveAgentModelParams:
    """Resolve the same flat model contract consumed by ``SharedContext``."""
    config = dict(run_config or {})
    bare_agent_id = str(agent_id or "").split("@", 1)[0]
    agent_configs = config.get("agent_configs")
    if not isinstance(agent_configs, dict):
        agent_configs = {}
    override = agent_configs.get(bare_agent_id)
    if not isinstance(override, dict):
        override = {}
    models = config.get("models")
    if not isinstance(models, dict):
        models = {}

    if force_project_model and project_model:
        model = project_model
        model_source = "project.model"
    elif isinstance(override.get("model"), str) and override["model"].strip():
        model = override["model"].strip()
        model_source = f"run_config.agent_configs.{bare_agent_id}.model"
    elif isinstance(models.get(subsystem), str) and models[subsystem].strip():
        model = models[subsystem].strip()
        model_source = f"run_config.models.{subsystem}"
    elif isinstance(models.get("default"), str) and models["default"].strip():
        model = models["default"].strip()
        model_source = "run_config.models.default"
    elif isinstance(project_model, str) and project_model.strip():
        model = project_model.strip()
        model_source = "project.model"
    else:
        model = agent_model or DEFAULT_AGENT_MODEL
        model_source = "agent.model"

    if isinstance(override.get("temperature"), (int, float)) and not isinstance(
        override.get("temperature"), bool
    ):
        temperature = float(override["temperature"])
        temperature_source = (
            f"run_config.agent_configs.{bare_agent_id}.temperature"
        )
    elif isinstance(project_temperature, (int, float)) and not isinstance(
        project_temperature, bool
    ):
        temperature = float(project_temperature)
        temperature_source = "project.temperature"
    else:
        temperature = agent_temperature
        temperature_source = "agent.temperature"

    run_effort = override.get("reasoning_effort")
    if isinstance(run_effort, str) and run_effort.strip():
        reasoning_effort = run_effort.strip()
        reasoning_source = (
            f"run_config.agent_configs.{bare_agent_id}.reasoning_effort"
        )
    elif isinstance(project_reasoning_effort, str) and project_reasoning_effort.strip():
        reasoning_effort = project_reasoning_effort.strip()
        reasoning_source = "project.reasoning_effort"
    else:
        reasoning_effort = agent_reasoning_effort
        reasoning_source = "agent.reasoning_effort"

    return EffectiveAgentModelParams(
        model=str(model).strip(),
        temperature=temperature,
        reasoning_effort=reasoning_effort,
        sources={
            "model": model_source,
            "temperature": temperature_source,
            "reasoning_effort": reasoning_source,
        },
    )


async def resolve_tenant_default_model(storage, tenant_id: Optional[str]) -> tuple[str, str]:
    """Admin-configured default model for a tenant, with its winning source.

    Chain: tenant_settings.default_model -> system_info.default_model ->
    DEFAULT_AGENT_MODEL. A value without a '/' is skipped as not gateway-routable
    (the gateway needs 'provider/model'), so a bare name never masks the real
    default. Any storage error degrades to the hardcoded default rather than
    raising — callers use this on hot paths (intent classification) where a DB
    hiccup must not break the request.

    Unlike ``resolve_effective_agent_model_params`` (which reads only a
    run_config), this reads the persisted tenant/global admin defaults, so it is
    the right fallback when no SharedContext is loaded.
    """
    try:
        if storage is not None and tenant_id:
            settings = await storage.get_tenant_settings(tenant_id)
            raw = (settings or {}).get("default_model")
            if isinstance(raw, str) and "/" in raw and raw.strip():
                return raw.strip(), "tenant_settings"
        if storage is not None:
            doc = await storage.db.system_info.find_one({"_id": "default_model"})
            global_id = (doc or {}).get("model_id")
            if isinstance(global_id, str) and "/" in global_id and global_id.strip():
                return global_id.strip(), "system_info"
    except Exception as exc:
        logger.warning(
            "[TENANT_DEFAULT_MODEL] tenant_id=%s — resolution failed, using default: %s",
            tenant_id,
            exc,
        )
    return DEFAULT_AGENT_MODEL, "hardcoded_fallback"


async def get_model_metadata_from_cache(
    model_id: str,
    storage=None,
    *,
    strict: bool = False,
) -> Optional[dict[str, Any]]:
    """Resolve normalized catalog metadata by exact or unique legacy ID."""
    if not storage:
        from api import deps

        storage = deps.get_storage()
    if not storage:
        return None
    try:
        cache_doc = await storage.db.system_info.find_one({"_id": "models_cache"})
        if not cache_doc:
            return None
        models = [m for m in cache_doc.get("models", []) if isinstance(m, dict)]
        normalized_model_id = normalize_model_id(model_id)
        matches = [
            model
            for model in models
            if normalize_model_id(str(model.get("id") or "")) == normalized_model_id
        ]
        if not matches and "/" not in normalized_model_id:
            matches = [
                model
                for model in models
                if normalize_model_id(str(model.get("id") or "")).endswith(
                    f"/{normalized_model_id}"
                )
            ]
        if len(matches) == 1:
            from llm.model_catalog import normalize_model

            return normalize_model(matches[0])
        if len(matches) > 1:
            logger.warning(
                "[MODEL_CONFIG] model=%s match_count=%d — ambiguous legacy model id",
                model_id,
                len(matches),
            )
            if strict:
                raise ModelConfigResolutionError(
                    f"Ambiguous model metadata for '{model_id}'"
                )
    except ModelConfigResolutionError:
        raise
    except Exception as exc:
        logger.warning(
            "[MODEL_CONFIG] model=%s — failed to read model metadata: %s",
            model_id,
            exc,
        )
        if strict:
            raise ModelConfigResolutionError(
                f"Unable to resolve model metadata for '{model_id}'"
            ) from exc
    return None


async def resolve_model_config(
    model_id: str,
    storage=None,
    *,
    strict: bool = False,
    require_known: bool = False,
) -> ModelConfig:
    """Resolve registry configuration enriched with catalog capabilities."""
    metadata = await get_model_metadata_from_cache(
        model_id,
        storage=storage,
        strict=strict,
    )
    supported_parameters = None
    catalog_reasoning = None
    if metadata is not None:
        supported_parameters = list(metadata.get("supported_parameters") or [])
        if metadata.get("is_reasoning") and not {
            "reasoning",
            "reasoning_effort",
        }.intersection(supported_parameters):
            supported_parameters.append("reasoning")
        if isinstance(metadata.get("reasoning"), dict):
            catalog_reasoning = metadata["reasoning"]
    canonical_model_id = (
        normalize_model_id(str(metadata.get("id") or model_id))
        if metadata is not None
        else normalize_model_id(model_id)
    )
    registry = get_model_registry()
    if require_known and metadata is None and not registry.has_config(model_id):
        raise ModelConfigResolutionError(
            f"Unknown model '{model_id}' in registry and model catalog"
        )
    return registry.get_config(
        canonical_model_id,
        supported_parameters=supported_parameters,
        catalog_reasoning=catalog_reasoning,
        has_catalog_metadata=metadata is not None,
    )


def _validation_error(
    code: str,
    message: str,
    field: str,
    model: Optional[str] = None,
    allowed: Optional[Any] = None,
    context: Optional[dict[str, Any]] = None,
) -> AgentModelParamsValidationError:
    return AgentModelParamsValidationError(
        code=code,
        message=message,
        field=field,
        model=model,
        allowed=allowed,
        context=context,
    )


def validate_model_params_against_config(
    *,
    model: str,
    temperature: Any,
    reasoning_effort: Any,
    config: ModelConfig,
    fields_to_validate: Optional[Collection[str]] = None,
) -> ModelConfig:
    """Validate flat parameters against an already-resolved model contract."""
    validation_fields = (
        MODEL_PARAM_FIELDS
        if fields_to_validate is None
        else frozenset(fields_to_validate)
    )
    unknown_fields = validation_fields - MODEL_PARAM_FIELDS
    if unknown_fields:
        raise ValueError(
            f"Unknown agent model fields: {', '.join(sorted(unknown_fields))}"
        )
    if not validation_fields:
        raise ValueError("At least one agent model field must be validated")
    if "model" in validation_fields:
        validation_fields = MODEL_PARAM_FIELDS

    normalized_model = model.strip() if isinstance(model, str) else ""
    if not normalized_model:
        raise _validation_error(
            "invalid_model", "Model must be a non-empty string", "model"
        )
    if "temperature" in validation_fields and temperature is not None:
        if isinstance(temperature, bool) or not isinstance(temperature, (int, float)):
            raise _validation_error(
                "invalid_temperature",
                "Temperature must be a finite number or null",
                "temperature",
                normalized_model,
            )
        if not math.isfinite(float(temperature)):
            raise _validation_error(
                "invalid_temperature",
                "Temperature must be a finite number or null",
                "temperature",
                normalized_model,
            )

    if (
        "reasoning_effort" in validation_fields
        and reasoning_effort is not None
        and not config.supports(ModelCapability.REASONING)
    ):
        raise _validation_error(
            "reasoning_not_supported",
            f"Model '{normalized_model}' does not support reasoning effort",
            "reasoning_effort",
            normalized_model,
        )
    if "reasoning_effort" in validation_fields and reasoning_effort is not None:
        if not isinstance(reasoning_effort, str) or not reasoning_effort:
            raise _validation_error(
                "invalid_reasoning_effort",
                "Reasoning effort must be a non-empty string or null",
                "reasoning_effort",
                normalized_model,
            )

        reasoning = config.reasoning
        if reasoning.mandatory and reasoning_effort == "none":
            raise _validation_error(
                "reasoning_effort_required",
                f"Model '{normalized_model}' requires reasoning",
                "reasoning_effort",
                normalized_model,
                list(reasoning.allowed_efforts()),
                {
                    "default_effort": reasoning.default_effort,
                    "mandatory": True,
                },
            )

        # Catalog metadata for effort_configurable/supported_efforts is
        # frequently incomplete (deepseek-r1, most Qwen3-thinking models,
        # gpt-5.*, o1, etc. lack it despite genuinely supporting effort
        # selection) — no longer hard-blocked here. The real provider call
        # is the source of truth; llm/client.py retries without
        # reasoning_effort if the provider actually rejects it.
        allowed_efforts = reasoning.allowed_efforts()
        if (
            reasoning_effort not in ALL_REASONING_EFFORTS
            and reasoning_effort not in allowed_efforts
        ):
            raise _validation_error(
                "invalid_reasoning_effort",
                f"Unsupported reasoning effort '{reasoning_effort}'",
                "reasoning_effort",
                normalized_model,
                list(allowed_efforts),
            )

    # Temperature support/forcing/range are also catalog-derived and can be
    # just as incomplete — no longer hard-blocked at save time either.
    # resolve_temperature() no longer pre-emptively drops it at call time
    # for catalog-unsupported models either — it's sent as requested, and
    # client.py's BadRequestError retry handles an actual provider rejection.
    return config


async def validate_agent_model_params(
    *,
    model: str,
    temperature: Any,
    reasoning_effort: Any,
    storage=None,
    fields_to_validate: Optional[Collection[str]] = None,
) -> ModelConfig:
    """Resolve and validate the flat agent model contract."""
    normalized_model = model.strip() if isinstance(model, str) else ""
    if not normalized_model:
        raise _validation_error(
            "invalid_model", "Model must be a non-empty string", "model"
        )
    config = await resolve_model_config(
        normalized_model,
        storage=storage,
        strict=True,
    )
    return validate_model_params_against_config(
        model=normalized_model,
        temperature=temperature,
        reasoning_effort=reasoning_effort,
        config=config,
        fields_to_validate=fields_to_validate,
    )
