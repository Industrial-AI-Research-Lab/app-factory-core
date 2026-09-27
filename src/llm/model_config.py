"""
Model Configuration System

Decouples agents from model-specific behavior. Agents request capabilities,
the config system handles provider/model-specific implementation details.

Design principles:
1. Agents specify WHAT they need (reasoning, json_mode, tool_calling)
2. Config system handles HOW (extra_body, temperature overrides, etc.)
3. Model configs are centralized - change once, affects all agents
4. Supports runtime config updates without code changes
"""

from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, field, replace
from enum import Enum
import logging

logger = logging.getLogger(__name__)

OPENROUTER_MODEL_PREFIX = "openrouter/"


def normalize_model_id(model_id: str) -> str:
    """Remove transport-only prefixes from a model identifier."""
    if model_id.startswith(OPENROUTER_MODEL_PREFIX):
        return model_id[len(OPENROUTER_MODEL_PREFIX) :]
    return model_id


class ModelCapability(str, Enum):
    """Capabilities that models may support"""

    REASONING = "reasoning"  # Extended thinking/chain-of-thought
    TOOL_CALLING = "tool_calling"  # Function/tool calling
    JSON_MODE = "json_mode"  # Structured JSON output
    VISION = "vision"  # Image understanding
    STREAMING = "streaming"  # Streaming responses
    RESPONSES_API = "responses_api"  # OpenAI Responses API support


ALL_REASONING_EFFORTS: Tuple[str, ...] = (
    "max",
    "xhigh",
    "high",
    "medium",
    "low",
    "minimal",
    "none",
)


@dataclass
class ReasoningConfig:
    """Configuration for reasoning/thinking behavior"""

    enabled: bool = False
    effort: str = "medium"  # low, medium, high
    summary: str = "auto"  # auto, none, verbose
    max_thinking_tokens: Optional[int] = None
    effort_configurable: bool = False
    supported_efforts: Optional[Tuple[str, ...]] = None
    default_effort: Optional[str] = None
    default_enabled: Optional[bool] = None
    mandatory: bool = False
    supports_max_tokens: bool = False

    def allowed_efforts(self) -> Tuple[str, ...]:
        """Return exact selectable efforts for the resolved model contract."""
        if not self.effort_configurable:
            return ()
        efforts = (
            ALL_REASONING_EFFORTS
            if self.supported_efforts is None
            else self.supported_efforts
        )
        if self.mandatory:
            return tuple(effort for effort in efforts if effort != "none")
        return efforts


@dataclass
class ModelConfig:
    """Configuration for a specific model"""

    model_id: str
    display_name: str
    provider: str  # openai, anthropic, google, openrouter, etc.

    # Capabilities
    capabilities: List[ModelCapability] = field(default_factory=list)

    # Reasoning settings
    reasoning: ReasoningConfig = field(default_factory=ReasoningConfig)

    # Temperature constraints
    supports_temperature: Optional[bool] = None
    min_temperature: float = 0.0
    max_temperature: float = 2.0
    force_temperature: Optional[float] = None  # Override requested temperature

    # Provider-specific extra_body parameters
    extra_body: Dict[str, Any] = field(default_factory=dict)

    # Context limits
    max_context_tokens: int = 128000
    max_output_tokens: int = 16384

    def supports(self, capability: ModelCapability) -> bool:
        """Check if model supports a capability"""
        return capability in self.capabilities

    def resolve_temperature(
        self,
        requested_temperature: Optional[float],
    ) -> Optional[float]:
        """Apply model temperature constraints independently of capabilities.

        A catalog-reported `supports_temperature=False` is no longer a local
        hard drop — like reasoning_effort, catalog completeness for this
        field is unreliable, so the value is sent as requested and the real
        provider call decides; client.py retries without it on
        BadRequestError instead of us pre-emptively omitting it here.
        """
        if self.force_temperature is not None:
            logger.debug(
                "Model %s: forcing temperature=%s",
                self.model_id,
                self.force_temperature,
            )
            return self.force_temperature
        if requested_temperature is None:
            return None
        return max(
            self.min_temperature,
            min(self.max_temperature, requested_temperature),
        )

    def get_request_params(
        self,
        requested_reasoning: Optional[bool] = None,
        requested_temperature: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Get provider-specific request parameters.

        Agents call this to get the right params without knowing provider details.
        """
        params = {}

        # Temperature handling
        temp = self.resolve_temperature(requested_temperature)
        if temp is not None:
            params["temperature"] = temp

        # Reasoning handling
        use_reasoning = (
            requested_reasoning
            if requested_reasoning is not None
            else self.reasoning.enabled
        )
        if use_reasoning and self.supports(ModelCapability.REASONING):
            # Build reasoning extra_body based on provider
            reasoning_body = self._build_reasoning_body()
            if reasoning_body:
                params.setdefault("extra_body", {}).update(reasoning_body)

        # Add any model-specific extra_body params
        if self.extra_body:
            params.setdefault("extra_body", {}).update(self.extra_body)

        return params

    def _build_reasoning_body(self) -> Dict[str, Any]:
        """Build provider-specific reasoning parameters"""
        # OpenAI o1/o3 style
        if self.provider == "openai" and any(
            x in self.model_id for x in ["/o1", "/o3", "o1-", "o3-"]
        ):
            return {
                "reasoning_effort": self.reasoning.effort,
            }

        # Moonshot/Kimi style (e.g., kimi-k2.5)
        if "kimi" in self.model_id.lower() or "moonshot" in self.model_id.lower():
            return {"reasoning": {"enabled": True}}

        # DeepSeek R1 style
        if (
            "deepseek-r1" in self.model_id.lower()
            or "deepseek/r1" in self.model_id.lower()
        ):
            return {"reasoning": {"enabled": True}}

        # Qwen QwQ style
        if "qwq" in self.model_id.lower():
            return {"reasoning": {"enabled": True}}

        # Generic fallback - try OpenAI-style
        return {"reasoning": {"enabled": True}}


# Default model configurations
# These can be overridden at runtime or via database
DEFAULT_MODEL_CONFIGS: Dict[str, ModelConfig] = {
    # OpenAI GPT-5 family
    "openai/gpt-5-mini": ModelConfig(
        model_id="openai/gpt-5-mini",
        display_name="GPT-5 Mini",
        provider="openai",
        capabilities=[
            ModelCapability.TOOL_CALLING,
            ModelCapability.JSON_MODE,
            ModelCapability.STREAMING,
        ],
        force_temperature=1.0,  # GPT-5 requires temp=1
    ),
    "openai/gpt-5": ModelConfig(
        model_id="openai/gpt-5",
        display_name="GPT-5",
        provider="openai",
        capabilities=[
            ModelCapability.TOOL_CALLING,
            ModelCapability.JSON_MODE,
            ModelCapability.STREAMING,
            ModelCapability.REASONING,
        ],
        reasoning=ReasoningConfig(
            enabled=False,
            effort_configurable=True,
            supported_efforts=("high", "medium", "low"),
            default_effort="medium",
            default_enabled=False,
        ),
        force_temperature=1.0,
    ),
    "openai/gpt-5-chat-latest": ModelConfig(
        model_id="openai/gpt-5-chat-latest",
        display_name="GPT-5 Chat",
        provider="openai",
        capabilities=[
            ModelCapability.TOOL_CALLING,
            ModelCapability.JSON_MODE,
            ModelCapability.STREAMING,
        ],
    ),
    # OpenAI o1/o3 reasoning models
    "openai/o1": ModelConfig(
        model_id="openai/o1",
        display_name="o1",
        provider="openai",
        capabilities=[
            ModelCapability.REASONING,
            ModelCapability.TOOL_CALLING,
            ModelCapability.JSON_MODE,
        ],
        reasoning=ReasoningConfig(
            enabled=True,
            effort="medium",
            effort_configurable=True,
            supported_efforts=("high", "medium", "low"),
            default_effort="medium",
            default_enabled=True,
        ),
        force_temperature=1.0,
    ),
    "openai/o3-mini": ModelConfig(
        model_id="openai/o3-mini",
        display_name="o3 Mini",
        provider="openai",
        capabilities=[
            ModelCapability.REASONING,
            ModelCapability.TOOL_CALLING,
            ModelCapability.JSON_MODE,
        ],
        reasoning=ReasoningConfig(
            enabled=True,
            effort="medium",
            effort_configurable=True,
            supported_efforts=("high", "medium", "low"),
            default_effort="medium",
            default_enabled=True,
        ),
        force_temperature=1.0,
    ),
    # Moonshot Kimi
    "moonshotai/kimi-k2.5": ModelConfig(
        model_id="moonshotai/kimi-k2.5",
        display_name="Kimi K2.5",
        provider="moonshot",
        capabilities=[
            ModelCapability.REASONING,
            ModelCapability.TOOL_CALLING,
            ModelCapability.JSON_MODE,
            ModelCapability.STREAMING,
        ],
        reasoning=ReasoningConfig(enabled=False),  # Has reasoning but off by default
    ),
    # DeepSeek
    "deepseek/deepseek-r1": ModelConfig(
        model_id="deepseek/deepseek-r1",
        display_name="DeepSeek R1",
        provider="deepseek",
        capabilities=[
            ModelCapability.REASONING,
            ModelCapability.TOOL_CALLING,
            ModelCapability.JSON_MODE,
        ],
        reasoning=ReasoningConfig(enabled=True),
    ),
    # Anthropic Claude
    "anthropic/claude-sonnet-4": ModelConfig(
        model_id="anthropic/claude-sonnet-4",
        display_name="Claude Sonnet 4",
        provider="anthropic",
        capabilities=[
            ModelCapability.TOOL_CALLING,
            ModelCapability.JSON_MODE,
            ModelCapability.STREAMING,
            ModelCapability.REASONING,
        ],
        reasoning=ReasoningConfig(enabled=False),
    ),
}


class ModelConfigRegistry:
    """
    Central registry for model configurations.

    Provides a single point to manage model-specific behavior.
    Can be extended to load configs from database for runtime updates.
    """

    def __init__(self):
        self._configs: Dict[str, ModelConfig] = DEFAULT_MODEL_CONFIGS.copy()
        self._runtime_overrides: Dict[str, Dict[str, Any]] = {}

    def get_config(
        self,
        model_id: str,
        supported_parameters: Optional[List[str]] = None,
        *,
        catalog_reasoning: Optional[Dict[str, Any]] = None,
        has_catalog_metadata: bool = False,
    ) -> ModelConfig:
        """
        Get configuration for a model.

        Falls back to a generic config if model not explicitly configured.
        """
        # Normalize model ID (handle provider prefixes)
        normalized = self._normalize_model_id(model_id)

        if normalized in self._configs:
            config = self._configs[normalized]
            # Apply runtime overrides if any
            if normalized in self._runtime_overrides:
                config = self._apply_overrides(
                    config, self._runtime_overrides[normalized]
                )
        else:
            # Create generic config for unknown models
            config = self._create_generic_config(normalized)

        return self._apply_catalog_capabilities(
            config,
            supported_parameters,
            catalog_reasoning=catalog_reasoning,
            has_catalog_metadata=has_catalog_metadata,
        )

    def has_config(self, model_id: str) -> bool:
        """Return whether a model resolves to an explicitly registered config."""
        return self._normalize_model_id(model_id) in self._configs

    def set_reasoning_enabled(self, model_id: str, enabled: bool):
        """Enable/disable reasoning for a model at runtime"""
        normalized = self._normalize_model_id(model_id)
        self._runtime_overrides.setdefault(normalized, {})[
            "reasoning_enabled"
        ] = enabled
        logger.info(
            f"Model {model_id}: reasoning {'enabled' if enabled else 'disabled'}"
        )

    def register_config(self, config: ModelConfig):
        """Register or update a model configuration"""
        self._configs[config.model_id] = config

    def _normalize_model_id(self, model_id: str) -> str:
        """Normalize model ID for lookup"""
        normalized = normalize_model_id(model_id)
        if normalized in self._configs or "/" in normalized:
            return normalized

        matches = [
            configured_id
            for configured_id in self._configs
            if configured_id.endswith(f"/{normalized}")
        ]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            logger.warning(
                "[MODEL_CONFIG] model=%s match_count=%d — ambiguous registered model id",
                model_id,
                len(matches),
            )
        return normalized

    def _apply_overrides(
        self, config: ModelConfig, overrides: Dict[str, Any]
    ) -> ModelConfig:
        """Apply runtime overrides to a config"""
        # Create a copy to avoid mutating the original
        new_config = replace(config)

        if "reasoning_enabled" in overrides:
            new_config.reasoning = replace(
                config.reasoning,
                enabled=overrides["reasoning_enabled"],
            )

        return new_config

    def _apply_catalog_capabilities(
        self,
        config: ModelConfig,
        supported_parameters: Optional[List[str]],
        *,
        catalog_reasoning: Optional[Dict[str, Any]],
        has_catalog_metadata: bool,
    ) -> ModelConfig:
        """Return a config enriched with exact catalog parameter contracts."""
        if has_catalog_metadata:
            supports_temperature = "temperature" in (supported_parameters or [])
            # force_temperature reflects a deliberate, locally-known provider
            # constraint (e.g. "OpenAI requires temperature=1 for gpt-5-*")
            # — it must survive the catalog merge regardless of whether this
            # particular sync happens to list "temperature" in
            # supported_parameters. That field is catalog-derived and
            # unreliable/incomplete for plenty of models (see the comment on
            # resolve_temperature below) — it should inform `supports_temperature`
            # (surfaced read-only in the API), never silently erase a real force.
            config = replace(
                config,
                supports_temperature=supports_temperature,
            )

        supports_reasoning = bool(catalog_reasoning is not None) or bool(
            supported_parameters
            and {"reasoning", "reasoning_effort"}.intersection(supported_parameters)
        )
        if not supports_reasoning:
            return config

        capabilities = list(config.capabilities)
        if ModelCapability.REASONING not in capabilities:
            capabilities.append(ModelCapability.REASONING)

        if not has_catalog_metadata:
            return replace(config, capabilities=capabilities)

        metadata = catalog_reasoning or {}
        effort_configurable = "supported_efforts" in metadata
        raw_efforts = metadata.get("supported_efforts")
        supported_efforts = None
        if effort_configurable and raw_efforts is not None:
            supported_efforts = tuple(raw_efforts)

        default_effort = metadata.get("default_effort")
        default_enabled = metadata.get("default_enabled")
        mandatory = metadata.get("mandatory") is True
        enabled = (
            True
            if mandatory
            else (
                default_enabled
                if isinstance(default_enabled, bool)
                else config.reasoning.enabled
            )
        )
        effort = (
            default_effort
            if isinstance(default_effort, str) and default_effort != "none"
            else config.reasoning.effort
        )

        return replace(
            config,
            capabilities=capabilities,
            reasoning=replace(
                config.reasoning,
                enabled=enabled,
                effort=effort,
                effort_configurable=effort_configurable,
                supported_efforts=supported_efforts,
                default_effort=(
                    default_effort if isinstance(default_effort, str) else None
                ),
                default_enabled=(
                    default_enabled if isinstance(default_enabled, bool) else None
                ),
                mandatory=mandatory,
                supports_max_tokens=metadata.get("supports_max_tokens") is True,
            ),
        )

    def _create_generic_config(self, model_id: str) -> ModelConfig:
        """Create a generic config for unknown models"""
        # Try to infer provider from model ID
        provider = "unknown"
        capabilities = [ModelCapability.STREAMING]
        reasoning = ReasoningConfig(enabled=False)
        force_temp = None

        model_lower = model_id.lower()

        # Infer provider
        if "openai" in model_lower or model_lower.startswith("gpt"):
            provider = "openai"
            capabilities.append(ModelCapability.TOOL_CALLING)
            capabilities.append(ModelCapability.JSON_MODE)
        elif "anthropic" in model_lower or "claude" in model_lower:
            provider = "anthropic"
            capabilities.append(ModelCapability.TOOL_CALLING)
        elif "google" in model_lower or "gemini" in model_lower:
            provider = "google"
            capabilities.append(ModelCapability.TOOL_CALLING)
        elif "deepseek" in model_lower:
            provider = "deepseek"
            capabilities.append(ModelCapability.TOOL_CALLING)
        elif "moonshot" in model_lower or "kimi" in model_lower:
            provider = "moonshot"
            capabilities.append(ModelCapability.TOOL_CALLING)
        elif "qwen" in model_lower:
            provider = "qwen"
            capabilities.append(ModelCapability.TOOL_CALLING)

        # add temperature by name-match, because we doint have this parameter in supported_parameters
        if any(x in model_lower for x in ["/o1", "/o3", "o1-", "o3-"]):
            force_temp = 1.0
        # GPT-5 temperature gate
        if "gpt-5" in model_lower:
            force_temp = 1.0

        return ModelConfig(
            model_id=model_id,
            display_name=model_id.split("/")[-1],
            provider=provider,
            capabilities=capabilities,
            reasoning=reasoning,
            force_temperature=force_temp,
        )


# Global registry instance
_registry: Optional[ModelConfigRegistry] = None


def get_model_registry() -> ModelConfigRegistry:
    """Get the global model config registry"""
    global _registry
    if _registry is None:
        _registry = ModelConfigRegistry()
    return _registry


async def get_supported_parameters_from_cache(
    model_id: str, storage=None
) -> Optional[List[str]]:
    """Read normalized supported_parameters from system_info.models_cache."""
    from llm.agent_model_params import get_model_metadata_from_cache

    metadata = await get_model_metadata_from_cache(model_id, storage=storage)
    if metadata is None:
        return None
    return metadata["supported_parameters"]


async def get_context_length_from_cache(model_id: str, storage=None) -> Optional[int]:
    """Read the model's real context window from system_info.models_cache.

    Agents usually hold the bare model name ("gpt-5-mini") while the gateway
    lists "provider/model" ids — an exact-id match wins, otherwise the first
    "*/model_id" suffix match. None on a miss so callers fall back to
    ModelConfig.max_context_tokens.
    """
    if not storage:
        from api import deps

        storage = deps.get_storage()
    if not storage:
        return None
    try:
        cache_doc = await storage.db.system_info.find_one({"_id": "models_cache"})
        if not cache_doc:
            return None
        suffix_match: Optional[int] = None
        for m in cache_doc.get("models", []):
            mid = m.get("id")
            length = m.get("context_length")
            if not mid or not length:
                continue
            if mid == model_id:
                return int(length)
            if suffix_match is None and mid.endswith("/" + model_id):
                suffix_match = int(length)
        return suffix_match
    except Exception:
        pass
    return None
