"""Shared validation for fully resolved agent model parameters."""

from __future__ import annotations

from dataclasses import dataclass, replace
import logging
from typing import Any, Iterable, Optional

from llm.agent_model_params import (
    AgentModelParamsValidationError,
    EffectiveAgentModelParams,
    ModelConfigResolutionError,
    resolve_effective_agent_model_params,
    resolve_model_config,
    validate_agent_model_params,
)

from utils.run_config import normalize_run_config


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AgentModelParamsTarget:
    """Base model parameters and runtime identity for one agent."""

    agent_id: str
    model: Optional[str]
    temperature: Any
    reasoning_effort: Any
    subsystem: str = "agent_default"


async def validate_effective_agent_model_params(
    targets: Iterable[AgentModelParamsTarget],
    *,
    run_config: Optional[Any],
    storage,
    project_model: Optional[str] = None,
    force_project_model: bool = False,
    project_reasoning_effort: Any = None,
    project_temperature: Any = None,
    field_prefix: str = "agents",
    allow_legacy_reasoning_fallback: bool = False,
) -> dict[str, EffectiveAgentModelParams]:
    """Resolve and validate the effective model triple for every target."""
    normalized_run_config = normalize_run_config(run_config)
    effective_by_agent: dict[str, EffectiveAgentModelParams] = {}

    for target in targets:
        effective = resolve_effective_agent_model_params(
            agent_id=target.agent_id,
            subsystem=target.subsystem,
            agent_model=target.model,
            agent_temperature=target.temperature,
            agent_reasoning_effort=target.reasoning_effort,
            run_config=normalized_run_config,
            project_model=project_model,
            force_project_model=force_project_model,
            project_reasoning_effort=project_reasoning_effort,
            project_temperature=project_temperature,
        )

        # "Use default" (reasoning_effort не выбран нигде в цепочке) не должен
        # молча означать "ничего не отправлять" для модели, которой reasoning
        # обязателен. Подставляем дефолтный effort из каталога (или "medium",
        # если каталог его не знает). Явный "none" — другой, осознанный выбор,
        # не трогаем — он всё равно упадёт на валидации ниже.
        if not effective.reasoning_effort:
            try:
                probe_config = await resolve_model_config(
                    effective.model, storage=storage, strict=True
                )
            except ModelConfigResolutionError:
                probe_config = None
            if probe_config is not None and probe_config.reasoning.mandatory:
                default_effort = probe_config.reasoning.default_effort or "medium"
                logger.info(
                    "[MODEL_PARAMS] agent=%s model=%s — mandatory reasoning "
                    "with no effort selected, defaulting to '%s'",
                    target.agent_id,
                    effective.model,
                    default_effort,
                )
                effective = replace(effective, reasoning_effort=default_effort)

        try:
            await validate_agent_model_params(
                model=effective.model,
                temperature=effective.temperature,
                reasoning_effort=effective.reasoning_effort,
                storage=storage,
            )
        except AgentModelParamsValidationError as exc:
            source = effective.sources.get(exc.field)
            if (
                exc.field == "reasoning_effort"
                and source != "project.reasoning_effort"
                and allow_legacy_reasoning_fallback
            ):
                logger.warning(
                    "[MODEL_PARAMS] agent=%s model=%s source=%s requested=%s "
                    "allowed=%s action=omit_reasoning_effort "
                    "— incompatible inherited reasoning effort",
                    target.agent_id,
                    effective.model,
                    source,
                    effective.reasoning_effort,
                    exc.allowed,
                )
                effective_by_agent[target.agent_id] = replace(
                    effective,
                    reasoning_effort=None,
                )
                continue
            model_source = effective.sources.get("model")
            source_context = {"source": source} if source else {}
            if model_source and model_source.startswith("run_config.models."):
                source_context["model_source"] = model_source
            raise AgentModelParamsValidationError(
                code=exc.code,
                message=exc.message,
                field=f"{field_prefix}.{target.agent_id}.{exc.field}",
                model=exc.model,
                allowed=exc.allowed,
                context={
                    **exc.context,
                    "agent_id": target.agent_id,
                    **source_context,
                },
            ) from exc

        effective_by_agent[target.agent_id] = effective

    return effective_by_agent
