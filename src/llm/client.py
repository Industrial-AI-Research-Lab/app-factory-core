"""
LLM Client Wrapper

Abstracts LLM provider (OpenAI, etc.) for easy swapping.
Handles:
- Chat completions
- Streaming
- Error handling
- Rate limiting
- Model-specific configuration gates
- Reasoning support via ModelConfigRegistry
"""

import logging
import os
import re
import time
from typing import Any, Callable, Dict, List, Optional

import httpx
from openai import AsyncOpenAI, BadRequestError
from telemetry.tracer import get_tracer

from llm.agent_model_params import (
    AgentModelParamsValidationError,
    DEFAULT_AGENT_MODEL,
    resolve_model_config,
    validate_model_params_against_config,
)
from llm.errors import ContextOverflowError, context_overflow_from_bad_request
from llm.model_config import get_model_registry
from llm.request_policy import (
    UNSET,
    TemperatureInput,
    resolve_temperature_input,
)
from utils.llm_json import parse_llm_json_object
try:
    # Optional import for typing/enum; at runtime tracer handles kind lazily
    from opentelemetry.trace import SpanKind as _SpanKind
except Exception:  # pragma: no cover - optional dep
    class _SpanKind:  # type: ignore
        CLIENT = object()

logger = logging.getLogger(__name__)

# Per-chunk LLM stream logging. Off by default — chunks are very high volume
# (one per ~10ms during text streaming). Set LOG_LLM_CHUNKS=true in the env to
# enable when debugging "no UI updates for N seconds" reports — the log then
# shows the actual upstream chunk timing so we can prove whether the silence
# is upstream (e.g. a model that buffers reasoning server-side and dumps at
# the end like nvidia/nemotron-3-super-120b-a12b:free) or in our pipeline.
_LOG_LLM_CHUNKS = os.getenv("LOG_LLM_CHUNKS", "").lower() == "true"


class LLMClient:
    """
    Wrapper for LLM API calls.
    
    Makes it easy to swap providers (OpenAI → Anthropic → Local, etc.)
    """
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        default_model: str = DEFAULT_AGENT_MODEL,
        default_temperature: float = 0.2,
        use_bifrost: bool = False,
        fallback_models: Optional[List[str]] = None,
        model_config_storage: Any = None,
    ):
        """
        Args:
            api_key: OpenAI API key (or get from env)
            default_model: Default model to use
            default_temperature: Default temperature
            use_bifrost: If True, route all LLM calls through Bifrost gateway
            fallback_models: List of fallback models (used with Bifrost)
            model_config_storage: Optional storage backend for catalog capabilities
        """
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.default_model = default_model
        self.default_temperature = default_temperature
        self.use_bifrost = use_bifrost or os.getenv("USE_BIFROST", "false").lower() == "true"
        self.model_config_storage = model_config_storage
        
        # Bifrost virtual key for x-bf-vk header
        self.bifrost_vk = os.getenv("BIFROST_VK")
        self.bifrost_url = os.getenv("BIFROST_URL", "http://localhost:8080/v1")
        self.bifrost_provider = os.getenv("BIFROST_PROVIDER", "openrouter")
        
        # Fallback models from env or defaults. Distinguish "the env var is
        # not set at all" (use the hardcoded convenience default) from "it's
        # set to an empty string" (fallback explicitly disabled) — the old
        # `elif os.getenv(...)` truthiness check treated both the same way,
        # so BIFROST_FALLBACK_MODELS="" silently re-enabled the hardcoded
        # gpt-5-mini→gpt-4o fallback instead of disabling it. That masked
        # real provider failures behind a silently substituted model with no
        # trace anywhere in our own logs (Bifrost itself still reports it via
        # x-bifrost-resolved-model / routing_info, but nothing here reads it).
        if fallback_models:
            self.fallback_models = fallback_models
        elif "BIFROST_FALLBACK_MODELS" in os.environ:
            raw = os.environ["BIFROST_FALLBACK_MODELS"].strip()
            self.fallback_models = (
                [m.strip() for m in raw.split(",") if m.strip()] if raw else []
            )
        else:
            # Default fallback models: use valid OpenRouter model IDs
            self.fallback_models = ["openrouter/openai/gpt-5-mini", "openrouter/openai/gpt-4o"]
        
        # Cache for override clients to avoid resource leaks
        self._override_clients: Dict[str, AsyncOpenAI] = {}
        
        # Configure default client
        self.client = self._create_client()
        if self.use_bifrost:
            vk_status = f"[SET:{self.bifrost_vk[:8]}...]" if self.bifrost_vk and len(self.bifrost_vk) > 8 else ("[EMPTY]" if self.bifrost_vk == "" else "[MISSING]")
            if self.client:
                logger.info(f"✅ Bifrost enabled at {self.bifrost_url} | VK={vk_status} | fallbacks={self.fallback_models}")
            else:
                logger.error(f"❌ Bifrost enabled but client is None - BIFROST_VK={vk_status}")
        # Tracing
        self.tracer = get_tracer()
    
    def _get_or_create_client(self, api_key_override: Optional[str] = None) -> Optional[AsyncOpenAI]:
        """
        Get or create an OpenAI client. Caches override clients to avoid resource leaks.
        """
        if not api_key_override:
            return self.client
        
        # Check cache first
        if api_key_override in self._override_clients:
            return self._override_clients[api_key_override]
        
        # Create and cache new client
        client = self._create_client(api_key_override)
        if client:
            self._override_clients[api_key_override] = client
        return client
    
    def _create_client(self, api_key_override: Optional[str] = None) -> Optional[AsyncOpenAI]:
        """
        Create an OpenAI client, configured for Bifrost if enabled.
        
        For Bifrost, uses x-bf-vk header with virtual key.
        """
        if self.use_bifrost:
            # Use Bifrost with virtual key header
            vk = api_key_override or self.bifrost_vk
            if not vk:
                logger.warning("Bifrost enabled but BIFROST_VK not set (override=%s, env=%s)", 
                              '[SET]' if api_key_override else '[NONE]',
                              '[SET]' if self.bifrost_vk else '[MISSING]')
                return None
            logger.debug(f"Creating Bifrost client with vk={'[SET:' + vk[:8] + '...]' if vk and len(vk) > 8 else '[SHORT/EMPTY]'}")
            # Use custom httpx client with headers explicitly set.
            # Also strip Authorization header to match the working curl/Postman requests
            # (Bifrost auth should be via x-bf-vk).
            async def preprocess_request(request: httpx.Request) -> None:
                request.headers.pop("authorization", None)
                has_vk = "x-bf-vk" in request.headers
                has_auth = "authorization" in request.headers
                logger.info(
                    "[HTTPX] %s %s | x-bf-vk=%s auth=%s",
                    request.method,
                    request.url,
                    "[SET]" if has_vk else "[MISSING]",
                    "[PRESENT]" if has_auth else "[STRIPPED]",
                )

            http_client = httpx.AsyncClient(
                headers={"x-bf-vk": vk},
                event_hooks={"request": [preprocess_request]},
            )
            return AsyncOpenAI(
                api_key="bifrost",  # Required by SDK, actual auth via x-bf-vk header
                base_url=self.bifrost_url,
                http_client=http_client
            )
        else:
            # Direct OpenAI
            key = api_key_override or self.api_key
            if not key:
                return None
            return AsyncOpenAI(api_key=key)
    
    def _apply_model_gates(
        self,
        model: str,
        temperature: Optional[float],
        **kwargs,
    ) -> tuple:
        """
        Apply temperature constraints from the shared model registry.
        
        Args:
            model: Model name (without provider prefix)
            temperature: Requested temperature
            **kwargs: Other parameters
        
        Returns:
            (modified_temperature, modified_kwargs)
        """
        config = get_model_registry().get_config(model)
        effective_temperature = config.resolve_temperature(temperature)
        if temperature != effective_temperature:
            logger.info(
                "[MODEL_CONFIG] model=%s requested_temperature=%s "
                "effective_temperature=%s — applying temperature policy",
                model,
                temperature,
                effective_temperature,
            )

        return effective_temperature, kwargs

    async def _resolve_model_config(self, model: str):
        """Resolve catalog capabilities when storage is explicitly available."""
        if self.model_config_storage is None:
            return get_model_registry().get_config(model)
        return await resolve_model_config(
            model,
            storage=self.model_config_storage,
        )

    async def _resolve_temperature_policy(
        self,
        model: str,
        requested_temperature: Optional[float],
    ):
        """Resolve one catalog-aware temperature policy for every transport."""
        config = await self._resolve_model_config(model)
        effective_temperature = config.resolve_temperature(requested_temperature)
        if requested_temperature != effective_temperature:
            logger.info(
                "[MODEL_CONFIG] model=%s requested_temperature=%s "
                "effective_temperature=%s supported=%s — applying temperature policy",
                model,
                requested_temperature,
                effective_temperature,
                config.supports_temperature,
            )
        return config, effective_temperature

    @staticmethod
    def _validate_runtime_reasoning_effort(
        *,
        model: str,
        model_config,
        reasoning_effort: Optional[str],
    ) -> Optional[str]:
        """Log (but no longer block) when runtime input violates catalog efforts.

        Catalog metadata is often incomplete/wrong (see agent_model_params.py),
        so this never fails closed anymore — the real provider call is the
        source of truth, and the caller retries without reasoning_effort if
        the provider actually rejects it (see client.py's BadRequestError
        retry handling).
        """
        from llm.model_config import ModelCapability

        if not reasoning_effort:
            # A caller that skipped the agent-level resolver (validate_
            # effective_agent_model_params) — e.g. /api/settings/test-model,
            # title generation, any future direct LLMClient use — can still
            # reach this with an empty effort on a model that can't run
            # without reasoning. Apply the same catalog-default substitution
            # here as a last line of defense, so every caller of chat_
            # completion/stream_responses/stream_completion_with_callback is
            # covered, not just the ones that went through the resolver.
            if model_config.reasoning.mandatory:
                default_effort = model_config.reasoning.default_effort or "medium"
                logger.info(
                    "[MODEL_PARAMS] model=%s — mandatory reasoning with no "
                    "effort provided at call time, defaulting to '%s'",
                    model,
                    default_effort,
                )
                return default_effort
            return None
        if not model_config.supports(ModelCapability.REASONING):
            return None
        try:
            validate_model_params_against_config(
                model=model,
                temperature=None,
                reasoning_effort=reasoning_effort,
                config=model_config,
                fields_to_validate={"reasoning_effort"},
            )
        except AgentModelParamsValidationError as exc:
            logger.warning(
                "[MODEL_CONFIG] model=%s field=%s value=%s allowed=%s "
                "— catalog flags this reasoning effort, letting the provider decide",
                model,
                exc.field,
                reasoning_effort,
                exc.allowed,
            )
        return reasoning_effort

    def _supports_json_mode(self, model: str) -> bool:
        """Return True if `model` accepts OpenAI-style response_format=json_object.

        Falls back to the registry's generic-config inference for unknown
        model IDs — only OpenAI-family models default to JSON_MODE there.
        """
        try:
            from llm.model_config import ModelCapability
            registry = get_model_registry()
            cfg = registry.get_config(model)
            return cfg.supports(ModelCapability.JSON_MODE)
        except Exception as e:
            # If the registry is broken for some reason, fail open so we don't
            # break callers that previously worked. The provider call will
            # error out cleanly if the model truly doesn't support it.
            logger.debug(f"_supports_json_mode lookup failed for {model}: {e}")
            return True

    def _format_model_name(self, model: str) -> str:
        """
        Format model name for Bifrost compatibility.
        
        Bifrost requires 'provider/model' format (e.g., 'openrouter/gpt-5-mini').
        Provider prefix is set via BIFROST_PROVIDER env var (default: openrouter).
        If already in correct format or not using Bifrost, returns as-is.
        """
        if not self.use_bifrost:
            return model

        provider_prefix = f"{self.bifrost_provider}/"

        # When using Bifrost we always want the top-level provider prefix (default: openrouter)
        # even if the underlying model id itself contains slashes (e.g. openai/gpt-4o).
        if model.startswith(provider_prefix):
            return model

        return f"{provider_prefix}{model}"

    @staticmethod
    def _merge_extra_body(
        request_params: Dict[str, Any],
        extra_body: Optional[Dict[str, Any]],
    ) -> None:
        """Merge caller/model extras with fields already prepared for the request."""
        if extra_body is None:
            return

        merged_extra_body = dict(request_params.get("extra_body") or {})
        merged_extra_body.update(extra_body)
        if merged_extra_body:
            request_params["extra_body"] = merged_extra_body

    def _apply_chat_reasoning_effort(
        self,
        request_params: Dict[str, Any],
        reasoning_effort: Optional[str],
    ) -> None:
        """Add reasoning effort using the active Chat Completions transport contract."""
        if not reasoning_effort:
            return

        extra_body = dict(request_params.get("extra_body") or {})
        extra_body.pop("reasoning", None)
        extra_body.pop("reasoning_effort", None)
        if extra_body:
            request_params["extra_body"] = extra_body
        else:
            request_params.pop("extra_body", None)

        request_params.pop("reasoning_effort", None)
        if not self.use_bifrost:
            request_params["reasoning_effort"] = reasoning_effort
            return

        extra_body["reasoning"] = {"effort": reasoning_effort}
        request_params["extra_body"] = extra_body

    @staticmethod
    def _bad_request_named_sampling_params(exc: BadRequestError) -> set:
        """Which of {'temperature', 'reasoning'} this 400's body actually
        names as the offending field — never both just because one was named.

        Bifrost proxies upstream failures (rate limits, capacity) through a
        generic HTTP 400 envelope too — those carry a nested `error.code`
        that is itself a non-4xx upstream status (e.g. `'code': '502'`) and
        have nothing to do with our request parameters. Only param/message
        evidence — not "it was an HTTP 400" — earns a strip-and-retry;
        anything else re-raises so the caller's existing backoff/retry
        handles it instead.

        Returning the *specific* named param(s) (not just a bool) matters:
        a 400 that names only temperature must not also cost the caller its
        reasoning_effort, and vice versa — see _strip_named_sampling_params.
        """
        body = exc.body if isinstance(exc.body, dict) else {}
        nested_error = body.get("error") if isinstance(body.get("error"), dict) else {}

        param = (exc.param or body.get("param") or nested_error.get("param") or "").lower()
        named = {key for key in ("temperature", "reasoning") if key in param}
        if named:
            return named

        for raw_code in (nested_error.get("code"), body.get("code"), exc.code):
            code = str(raw_code or "")
            if code.isdigit() and not code.startswith("4"):
                # A numeric, non-4xx code (nested or top-level) means the
                # gateway is relaying an upstream failure — not rejecting
                # our request parameters.
                return set()

        message = f"{exc.message or ''} {nested_error.get('message') or ''}".lower()
        return {key for key in ("temperature", "reasoning") if key in message}

    @staticmethod
    def _bad_request_blames_sampling_params(exc: BadRequestError) -> bool:
        """True if the 400 names temperature and/or reasoning at all."""
        return bool(LLMClient._bad_request_named_sampling_params(exc))

    @staticmethod
    def _strip_named_sampling_params(
        request_params: Dict[str, Any],
        named: set,
        *,
        reasoning_mandatory: bool = False,
    ) -> Dict[str, Any]:
        """Remove only the sampling knobs the provider actually named, in place.

        Never strips the other, unnamed param — a 400 about temperature must
        not also cost the caller its reasoning_effort (or vice versa).

        For a model where reasoning is mandatory (reasoning.mandatory=True),
        'reasoning' is deliberately NOT stripped even if named: an omitted
        reasoning param would itself violate that model's own contract and
        likely 400 again on the retry, with no backoff for a third attempt.
        Leaving it untouched here means the caller's `if not dropped: raise`
        fires instead — a clean failure instead of a guaranteed second one.

        Handles both wire shapes used across this client: flat
        temperature/reasoning_effort/reasoning (Chat Completions without
        Bifrost, and the Responses API) and extra_body-nested
        reasoning/reasoning_effort (Chat Completions via Bifrost).
        """
        dropped: Dict[str, Any] = {}
        if "temperature" in named and "temperature" in request_params:
            dropped["temperature"] = request_params.pop("temperature")

        if "reasoning" in named and not reasoning_mandatory:
            for key in ("reasoning_effort", "reasoning"):
                if key in request_params:
                    dropped[key] = request_params.pop(key)
            extra_body = request_params.get("extra_body")
            if extra_body:
                for key in ("reasoning", "reasoning_effort"):
                    if key in extra_body:
                        dropped[f"extra_body.{key}"] = extra_body.pop(key)
                if not extra_body:
                    request_params.pop("extra_body", None)

        return dropped
    
    async def chat_completion(
        self,
        messages: List[Dict[str, str]],
        model: Optional[str] = None,
        temperature: TemperatureInput = UNSET,
        max_tokens: Optional[int] = None,
        tools: Optional[List[Dict]] = None,
        tool_choice: Optional[str] = None,
        api_key_override: Optional[str] = None,
        fallback_models_override: Optional[List[str]] = None,
        reasoning: Optional[bool] = None,  # Enable/disable reasoning for this call
        **kwargs
    ) -> Dict[str, Any]:
        """
        Get chat completion from LLM.
        
        Args:
            messages: List of {"role": "user/assistant/system", "content": "..."}
            model: Model to use (defaults to default_model)
            temperature: Temperature (0-2); explicit None uses the model default
            max_tokens: Max response tokens
            tools: List of tool definitions for function calling
            tool_choice: "auto", "none", or {"type": "function", "function": {"name": "..."}}
            reasoning: Enable/disable reasoning for models that support it (None = use model default)
        
        Returns:
            Dict with 'content' and optional 'tool_calls'
        """
        try:
            # Determine model and temperature
            selected_model = model or self.default_model
            selected_temperature = resolve_temperature_input(
                temperature,
                default_temperature=self.default_temperature,
            )
            
            # Get model config and apply model-specific parameters
            model_config = await self._resolve_model_config(selected_model)
            config_params = model_config.get_request_params(
                requested_reasoning=reasoning,
                requested_temperature=selected_temperature,
            )
            
            # Absence now only means the caller didn't request a temperature
            # at all (requested_temperature was None) — catalog-unsupported
            # models still get their value sent; see resolve_temperature().
            selected_temperature = config_params.get("temperature")
            
            # Merge extra_body from config into kwargs
            if "extra_body" in config_params:
                existing_extra = kwargs.get("extra_body", {})
                existing_extra.update(config_params["extra_body"])
                kwargs["extra_body"] = existing_extra
            
            # Format model name for provider (Bifrost)
            model_name = self._format_model_name(selected_model)
            
            # Prepare request parameters
            request_params = {
                "model": model_name,
                "messages": messages,
            }
            if selected_temperature is not None:
                request_params["temperature"] = selected_temperature
            
            if max_tokens:
                request_params["max_tokens"] = max_tokens
            
            # Add tools for function calling
            if tools:
                request_params["tools"] = tools
                if tool_choice:
                    request_params["tool_choice"] = tool_choice
            
            # Add Bifrost fallbacks if enabled. None = caller didn't override,
            # use the instance default; [] = caller explicitly disabled fallback.
            effective_fallback_models = (
                fallback_models_override if fallback_models_override is not None else self.fallback_models
            )
            if self.use_bifrost and effective_fallback_models:
                request_params['extra_body'] = {'fallbacks': effective_fallback_models}
            
            # Merge any additional kwargs without losing Bifrost fallbacks.
            additional_extra_body = kwargs.pop("extra_body", None)
            request_params.update(kwargs)
            self._merge_extra_body(request_params, additional_extra_body)
            
            reasoning_effort = request_params.pop("reasoning_effort", None)
            effective_reasoning_effort = self._validate_runtime_reasoning_effort(
                model=selected_model,
                model_config=model_config,
                reasoning_effort=reasoning_effort,
            )
            self._apply_chat_reasoning_effort(
                request_params,
                effective_reasoning_effort,
            )

            # Choose client (override or default) - uses cache to avoid resource leaks
            client = self._get_or_create_client(api_key_override)
            if not client:
                raise ValueError("OpenAI API key or BIFROST_VK not provided")
            # Trace the LLM request using GenAI semantic conventions
            # Extract short model name for span (e.g., "gpt-4" from "openai/gpt-4-turbo")
            short_model = model_name.split("/")[-1].split("-")[0] if "/" in model_name else model_name.split("-")[0]
            with self.tracer.start_span(
                f"llm.chat_completion.{short_model}",
                attributes={
                    "gen_ai.system": "openai",
                    "gen_ai.request.model": model_name,
                    "gen_ai.request.temperature": selected_temperature,
                    "gen_ai.request.max_tokens": max_tokens or 0,
                    "bifrost.used": self.use_bifrost,
                    "AppFactory.llm.provider": "openai",
                    "AppFactory.llm.gateway": "bifrost" if self.use_bifrost else "direct",
                    "AppFactory.llm.request_type": "chat_completion",
                    "AppFactory.llm.model_family": model_name.split("-")[0] if "-" in model_name else model_name
                },
                kind=_SpanKind.CLIENT,
            ) as span:
                # Add request preview as a Jaeger event (no truncation; last 10 messages for sanity)
                try:
                    msgs_preview = []
                    for m in messages[-10:]:  # last 10 msgs
                        role = m.get("role", "")
                        content = m.get("content", "")
                        # Include full content as requested
                        msgs_preview.append({"role": role, "content": content})

                    self.tracer.add_event(span, "llm.request", {
                        "model": model_name,
                        "temperature": selected_temperature,
                        "messages": str(msgs_preview)
                    })
                except Exception:
                    pass

                try:
                    response = await client.chat.completions.create(**request_params)
                    param_downgrade = None
                except BadRequestError as exc:
                    named = self._bad_request_named_sampling_params(exc)
                    if not named:
                        raise
                    dropped = self._strip_named_sampling_params(
                        request_params, named,
                        reasoning_mandatory=model_config.reasoning.mandatory,
                    )
                    if not dropped:
                        raise
                    logger.warning(
                        "[MODEL_PARAMS] model=%s dropped=%s retrying without them — %s",
                        model_name, dropped, exc,
                    )
                    response = await client.chat.completions.create(**request_params)
                    param_downgrade = {"dropped": dropped, "reason": str(exc)}

                served_model = getattr(response, "model", None)
                if served_model is None and isinstance(response, dict):
                    served_model = response.get("model")
                self._warn_if_model_substituted(model_name, served_model)

                # Handle both Pydantic objects and raw dicts (some providers return dicts)
                reasoning_content = None
                if isinstance(response, dict):
                    choices = response.get("choices", [])
                    first_choice = choices[0] if choices else {}
                    message = first_choice.get("message", {}) if isinstance(first_choice, dict) else getattr(first_choice, "message", {})
                    if isinstance(message, dict):
                        content = message.get("content", "")
                        tool_calls = message.get("tool_calls")
                        # Extract reasoning content (OpenRouter returns it in message.reasoning or message.reasoning_content)
                        reasoning_content = message.get("reasoning") or message.get("reasoning_content")
                    else:
                        content = getattr(message, "content", "")
                        tool_calls = getattr(message, "tool_calls", None)
                        reasoning_content = getattr(message, "reasoning", None) or getattr(message, "reasoning_content", None)
                    finish_reason = first_choice.get("finish_reason", "") if isinstance(first_choice, dict) else getattr(first_choice, "finish_reason", "")
                    usage = response.get("usage", {})
                    resp_model = response.get("model", "")
                    response_id = response.get("id") or None
                else:
                    message = response.choices[0].message
                    content = getattr(message, "content", "")
                    tool_calls = getattr(message, "tool_calls", None)
                    reasoning_content = getattr(message, "reasoning", None) or getattr(message, "reasoning_content", None)
                    finish_reason = response.choices[0].finish_reason
                    usage = getattr(response, "usage", None)
                    resp_model = getattr(response, "model", "")
                    response_id = getattr(response, "id", None) or None
                
                # Add response preview as a Jaeger event (no truncation)
                try:
                    content_preview = content or ""
                    self.tracer.add_event(span, "llm.response", {
                        "model": resp_model,
                        "finish_reason": finish_reason,
                        "content": content_preview,
                    })
                except Exception:
                    pass
                try:
                    if usage and span:
                        if isinstance(usage, dict):
                            span.set_attribute("gen_ai.usage.input_tokens", usage.get("prompt_tokens"))
                            span.set_attribute("gen_ai.usage.output_tokens", usage.get("completion_tokens"))
                            span.set_attribute("gen_ai.usage.total_tokens", usage.get("total_tokens"))
                        else:
                            span.set_attribute("gen_ai.usage.input_tokens", getattr(usage, "prompt_tokens", None))
                            span.set_attribute("gen_ai.usage.output_tokens", getattr(usage, "completion_tokens", None))
                            span.set_attribute("gen_ai.usage.total_tokens", getattr(usage, "total_tokens", None))
                    if span:
                        span.set_attribute("gen_ai.response.model", resp_model)
                        span.set_attribute("gen_ai.response.finish_reason", finish_reason)
                        self.tracer.set_success(span)
                except Exception:
                    pass
            
            # Return content, tool calls, and reasoning
            return {
                "content": content,
                "tool_calls": tool_calls,
                "finish_reason": finish_reason,
                "usage": usage,
                "reasoning": reasoning_content,  # Reasoning/thinking content from models that support it
                "param_downgrade": param_downgrade,  # Set if temperature/reasoning_effort got stripped and retried
                "response_id": response_id,
            }

            
        except AgentModelParamsValidationError:
            raise
        except Exception as e:
            # record error if there was an active span
            try:
                self.tracer.set_error(span, e)  # type: ignore[name-defined]
            except Exception:
                pass
            raise RuntimeError(f"LLM call failed: {str(e)}")
    
    async def chat_completion_with_json(
        self,
        messages: List[Dict[str, str]],
        model: Optional[str] = None,
        temperature: TemperatureInput = UNSET,
        api_key_override: Optional[str] = None,
        return_reasoning: bool = False,
        **kwargs
    ) -> Dict:
        """
        Get JSON response from LLM.
        
        Uses structured output mode if available.
        
        Args:
            return_reasoning: If True, returns {"result": parsed_json, "reasoning": str|None}
        """
        try:
            # Determine model and temperature
            selected_model = model or self.default_model
            selected_temperature = resolve_temperature_input(
                temperature,
                default_temperature=self.default_temperature,
            )
            
            _model_config, selected_temperature = await self._resolve_temperature_policy(
                selected_model,
                selected_temperature,
            )
            
            # Format model name for provider (Bifrost)
            model_name = self._format_model_name(selected_model)

            # Prepare request parameters
            request_params = {
                "model": model_name,
                "messages": messages,
            }
            if selected_temperature is not None:
                request_params["temperature"] = selected_temperature
            # Only request OpenAI-style JSON mode when the model supports it.
            # Many OpenRouter-routed providers (e.g. Tencent Hy3) reject the
            # response_format param and return a generic "Provider returned error".
            # The system prompt is expected to mandate JSON; json.loads below still parses it.
            if self._supports_json_mode(selected_model):
                request_params["response_format"] = {"type": "json_object"}
            else:
                logger.debug(
                    f"chat_completion_with_json: skipping response_format for {selected_model} "
                    f"(model not flagged JSON_MODE-capable)"
                )

            # NOTE: Do NOT add extra_body with response_format - causes API format errors
            # Bifrost fallbacks are disabled for JSON mode to avoid conflicts
            # TODO: Find a way to support both simultaneously

            # Merge any additional kwargs
            request_params.update(kwargs)

            client = self._get_or_create_client(api_key_override)
            if not client:
                logger.error(f"chat_completion_with_json: No client - api_key_override={'[SET]' if api_key_override else '[NONE]'}, use_bifrost={self.use_bifrost}, bifrost_vk={'[SET]' if self.bifrost_vk else '[MISSING]'}")
                raise ValueError("OpenAI API key or BIFROST_VK not provided")

            try:
                response = await client.chat.completions.create(**request_params)
                param_downgrade = None
            except BadRequestError as exc:
                named = self._bad_request_named_sampling_params(exc)
                if not named:
                    raise
                dropped = self._strip_named_sampling_params(
                    request_params, named,
                    reasoning_mandatory=_model_config.reasoning.mandatory,
                )
                if not dropped:
                    raise
                logger.warning(
                    "[MODEL_PARAMS] model=%s dropped=%s retrying without them — %s",
                    model_name, dropped, exc,
                )
                response = await client.chat.completions.create(**request_params)
                param_downgrade = {"dropped": dropped, "reason": str(exc)}

            served_model = getattr(response, "model", None)
            if served_model is None and isinstance(response, dict):
                served_model = response.get("model")
            self._warn_if_model_substituted(model_name, served_model)

            # Handle both Pydantic objects and raw dicts
            reasoning_content = None
            if isinstance(response, dict):
                choices = response.get("choices", [])
                first_choice = choices[0] if choices else {}
                message = first_choice.get("message", {}) if isinstance(first_choice, dict) else getattr(first_choice, "message", {})
                if isinstance(message, dict):
                    content = message.get("content", "")
                    reasoning_content = message.get("reasoning") or message.get("reasoning_content")
                else:
                    content = getattr(message, "content", "")
                    reasoning_content = getattr(message, "reasoning", None) or getattr(message, "reasoning_content", None)
            else:
                message = response.choices[0].message
                content = getattr(message, "content", "")
                reasoning_content = getattr(message, "reasoning", None) or getattr(message, "reasoning_content", None)

            parsed = parse_llm_json_object(content)
            
            if return_reasoning:
                return {"result": parsed, "reasoning": reasoning_content, "param_downgrade": param_downgrade}
            return parsed
            
        except Exception as e:
            raise RuntimeError(f"LLM JSON call failed: {str(e)}")
    
    async def stream_completion_with_callback(
        self,
        messages: List[Dict[str, str]],
        model: Optional[str] = None,
        temperature: TemperatureInput = UNSET,
        api_key_override: Optional[str] = None,
        fallback_models_override: Optional[List[str]] = None,
        event_callback: Optional[Callable] = None,
        **kwargs
    ) -> Dict:
        """
        Stream chat completion with callback for reasoning events.
        Returns dict with 'content', 'reasoning', and optionally 'tool_calls'.
        """
        import time
        
        try:
            selected_model = model or self.default_model
            selected_temperature = resolve_temperature_input(
                temperature,
                default_temperature=self.default_temperature,
            )
            
            model_cfg, selected_temperature = await self._resolve_temperature_policy(
                selected_model,
                selected_temperature,
            )
            
            model_name = self._format_model_name(selected_model)
            
            request_params = {
                "model": model_name,
                "messages": messages,
                "stream": True,
                "stream_options": {"include_usage": True}
            }
            if selected_temperature is not None:
                request_params["temperature"] = selected_temperature

            effective_fallback_models = (
                fallback_models_override if fallback_models_override is not None else self.fallback_models
            )
            if self.use_bifrost and effective_fallback_models:
                request_params['extra_body'] = {'fallbacks': effective_fallback_models}

            reasoning_effort = kwargs.pop("reasoning_effort", None)
            additional_extra_body = kwargs.pop("extra_body", None)
            request_params.update(kwargs)
            self._merge_extra_body(request_params, additional_extra_body)
            effective_reasoning_effort = self._validate_runtime_reasoning_effort(
                model=selected_model,
                model_config=model_cfg,
                reasoning_effort=reasoning_effort,
            )
            self._apply_chat_reasoning_effort(
                request_params,
                effective_reasoning_effort,
            )
            
            client = self._get_or_create_client(api_key_override)
            if not client:
                raise ValueError("OpenAI API key or BIFROST_VK not provided")

            try:
                stream = await client.chat.completions.create(**request_params)
                param_downgrade = None
            except BadRequestError as exc:
                named = self._bad_request_named_sampling_params(exc)
                if not named:
                    raise
                dropped = self._strip_named_sampling_params(
                    request_params, named,
                    reasoning_mandatory=model_cfg.reasoning.mandatory,
                )
                if not dropped:
                    raise
                logger.warning(
                    "[MODEL_PARAMS] model=%s dropped=%s retrying without them — %s",
                    model_name, dropped, exc,
                )
                stream = await client.chat.completions.create(**request_params)
                param_downgrade = {"dropped": dropped, "reason": str(exc)}
                if event_callback:
                    try:
                        await event_callback({"type": "param_downgrade", "dropped": dropped})
                    except Exception:
                        pass

            thinking_start = None
            thinking_content = ""
            text_content = ""
            tool_calls_data = []
            usage_data = None
            substitution_checked = False
            final_finish_reason = None
            response_id = None
            
            async for chunk in stream:
                if not substitution_checked:
                    served_model = getattr(chunk, "model", None)
                    if served_model:
                        substitution_checked = True
                    self._warn_if_model_substituted(model_name, served_model)
                if response_id is None and getattr(chunk, "id", None):
                    response_id = chunk.id

                # Capture usage from final chunk (OpenAI/OpenRouter include it there)
                if hasattr(chunk, 'usage') and chunk.usage:
                    usage_data = {
                        "prompt_tokens": getattr(chunk.usage, 'prompt_tokens', 0),
                        "completion_tokens": getattr(chunk.usage, 'completion_tokens', 0),
                        "total_tokens": getattr(chunk.usage, 'total_tokens', 0),
                    }
                
                if not chunk.choices:
                    continue
                
                delta = chunk.choices[0].delta
                
                # Handle reasoning
                reasoning = getattr(delta, 'reasoning', None) or getattr(delta, 'reasoning_content', None)
                if reasoning is None and hasattr(delta, '__dict__'):
                    reasoning = delta.__dict__.get('reasoning') or delta.__dict__.get('reasoning_content')
                
                if reasoning and event_callback:
                    if thinking_start is None:
                        thinking_start = time.time()
                    thinking_content += reasoning
                    try:
                        await event_callback({"type": "thinking.delta", "content": reasoning})
                    except Exception:
                        pass
                
                # Handle text content
                content = getattr(delta, 'content', None)
                if content:
                    if thinking_content and thinking_start and event_callback:
                        thinking_time = time.time() - thinking_start
                        # Diagnostic: track which emission site fired and the
                        # content length so we can detect duplicate emissions
                        # in backend.log when investigating "double thought
                        # block" reports. Two sites in this function emit
                        # thinking.done — this one (text-content-arrived) and
                        # the finish_reason one below — and they should be
                        # mutually exclusive per call. Mismatch in the log
                        # means thinking_content/start weren't cleared.
                        logger.info(
                            "[THINKING.DONE.EMIT] site=stream_completion#text-arrived "
                            "model=%s elapsed=%.3fs chars=%d",
                            model_name, thinking_time, len(thinking_content),
                        )
                        try:
                            await event_callback({
                                "type": "thinking.done",
                                "content": thinking_content,
                                "thinking_time": thinking_time,
                                "usage": usage_data
                            })
                        except Exception:
                            pass
                        thinking_content = ""
                        thinking_start = None
                    text_content += content
                    # Emit per-chunk text.delta so the UI can render the
                    # streaming content live. Without this, the entire
                    # output-streaming portion of the call is invisible to
                    # users (see the 18s "Ready" gap diagnosed against
                    # project 0a711fa7-c0ab-4e56-b458-e1efb007fb2e).
                    if event_callback:
                        try:
                            await event_callback({"type": "text.delta", "content": content})
                        except Exception:
                            pass

                # Handle tool calls
                if hasattr(delta, 'tool_calls') and delta.tool_calls:
                    for tc in delta.tool_calls:
                        idx = tc.index if hasattr(tc, 'index') else 0
                        while len(tool_calls_data) <= idx:
                            tool_calls_data.append({"id": "", "type": "function", "function": {"name": "", "arguments": ""}})
                        if hasattr(tc, 'id') and tc.id:
                            tool_calls_data[idx]["id"] = tc.id
                        if hasattr(tc, 'function') and tc.function:
                            if hasattr(tc.function, 'name') and tc.function.name:
                                tool_calls_data[idx]["function"]["name"] += tc.function.name
                            if hasattr(tc.function, 'arguments') and tc.function.arguments:
                                tool_calls_data[idx]["function"]["arguments"] += tc.function.arguments
                
                # Check finish
                if chunk.choices[0].finish_reason:
                    final_finish_reason = chunk.choices[0].finish_reason
                    if thinking_content and thinking_start and event_callback:
                        thinking_time = time.time() - thinking_start
                        # See companion log at site=stream_completion#text-arrived.
                        # If both sites fire for the same call, thinking_content
                        # wasn't cleared after the first emit — that's a bug.
                        logger.info(
                            "[THINKING.DONE.EMIT] site=stream_completion#finish_reason "
                            "model=%s elapsed=%.3fs chars=%d finish=%s",
                            model_name, thinking_time, len(thinking_content),
                            chunk.choices[0].finish_reason,
                        )
                        try:
                            await event_callback({
                                "type": "thinking.done",
                                "content": thinking_content,
                                "thinking_time": thinking_time,
                                "usage": usage_data
                            })
                        except Exception:
                            pass

            # Tell the UI the streaming output finished — pairs with the
            # text.delta events emitted per chunk above.
            if event_callback and text_content:
                try:
                    await event_callback({
                        "type": "text.done",
                        "content": text_content,
                        "usage": usage_data,
                    })
                except Exception:
                    pass

            result = {"content": text_content}
            if final_finish_reason is not None:
                result["finish_reason"] = final_finish_reason
            if thinking_content:
                result["reasoning"] = thinking_content
            if tool_calls_data:
                result["tool_calls"] = tool_calls_data
            if usage_data:
                result["usage"] = usage_data
            if param_downgrade:
                result["param_downgrade"] = param_downgrade
            if response_id:
                result["response_id"] = response_id
            return result
            
        except AgentModelParamsValidationError:
            raise
        except Exception as e:
            raise RuntimeError(f"LLM streaming call failed: {str(e)}")
    
    async def stream_completion_with_json(
        self,
        messages: List[Dict[str, str]],
        model: Optional[str] = None,
        temperature: TemperatureInput = UNSET,
        api_key_override: Optional[str] = None,
        event_callback: Optional[Callable] = None,
        **kwargs
    ) -> Dict:
        """
        Stream chat completion with JSON response, emitting reasoning events during streaming.

        Args:
            event_callback: Async callback(event_dict) called for each thinking event

        Returns:
            Parsed JSON response
        """
        import time
        
        try:
            selected_model = model or self.default_model
            selected_temperature = resolve_temperature_input(
                temperature,
                default_temperature=self.default_temperature,
            )
            
            _model_config, selected_temperature = await self._resolve_temperature_policy(
                selected_model,
                selected_temperature,
            )
            
            model_name = self._format_model_name(selected_model)
            
            request_params = {
                "model": model_name,
                "messages": messages,
                "stream": True,
                "stream_options": {"include_usage": True},
            }
            if selected_temperature is not None:
                request_params["temperature"] = selected_temperature
            if self._supports_json_mode(selected_model):
                request_params["response_format"] = {"type": "json_object"}
            else:
                logger.debug(
                    f"stream_completion_with_json: skipping response_format for {selected_model} "
                    f"(model not flagged JSON_MODE-capable)"
                )

            # NOTE: Do NOT add extra_body with response_format - causes API format errors.
            # Bifrost fallbacks are disabled for JSON mode to avoid conflicts, same as
            # chat_completion_with_json — no tenant/force override to plumb here since
            # fallback is never sent regardless.
            # TODO: Find a way to support both simultaneously

            request_params.update(kwargs)

            client = self._get_or_create_client(api_key_override)
            if not client:
                raise ValueError("OpenAI API key or BIFROST_VK not provided")

            try:
                stream = await client.chat.completions.create(**request_params)
            except BadRequestError as exc:
                named = self._bad_request_named_sampling_params(exc)
                if not named:
                    raise
                dropped = self._strip_named_sampling_params(
                    request_params, named,
                    reasoning_mandatory=_model_config.reasoning.mandatory,
                )
                if not dropped:
                    raise
                logger.warning(
                    "[MODEL_PARAMS] model=%s dropped=%s retrying without them — %s",
                    model_name, dropped, exc,
                )
                stream = await client.chat.completions.create(**request_params)
                if event_callback:
                    try:
                        await event_callback({"type": "param_downgrade", "dropped": dropped})
                    except Exception:
                        pass

            thinking_start = None
            thinking_content = ""
            text_content = ""
            usage_data = None
            substitution_checked = False

            async for chunk in stream:
                if not substitution_checked:
                    served_model = getattr(chunk, "model", None)
                    if served_model:
                        substitution_checked = True
                    self._warn_if_model_substituted(model_name, served_model)
                # Capture usage from final chunk
                if hasattr(chunk, 'usage') and chunk.usage:
                    usage_data = {
                        "prompt_tokens": getattr(chunk.usage, 'prompt_tokens', 0),
                        "completion_tokens": getattr(chunk.usage, 'completion_tokens', 0),
                        "total_tokens": getattr(chunk.usage, 'total_tokens', 0),
                    }
                
                if not chunk.choices:
                    continue
                
                delta = chunk.choices[0].delta
                
                # Handle reasoning (OpenRouter returns in delta.reasoning)
                reasoning = getattr(delta, 'reasoning', None) or getattr(delta, 'reasoning_content', None)
                if reasoning is None and hasattr(delta, '__dict__'):
                    reasoning = delta.__dict__.get('reasoning') or delta.__dict__.get('reasoning_content')
                
                if reasoning and event_callback:
                    if thinking_start is None:
                        thinking_start = time.time()
                    thinking_content += reasoning
                    try:
                        await event_callback({"type": "thinking.delta", "content": reasoning})
                    except Exception:
                        pass
                
                # Handle text content
                content = getattr(delta, 'content', None)
                if content:
                    # Emit thinking.done when transitioning from thinking to content
                    if thinking_content and thinking_start and event_callback:
                        thinking_time = time.time() - thinking_start
                        logger.info(
                            "[THINKING.DONE.EMIT] site=stream_completion_with_json#text-arrived "
                            "model=%s elapsed=%.3fs chars=%d",
                            model_name, thinking_time, len(thinking_content),
                        )
                        try:
                            await event_callback({
                                "type": "thinking.done",
                                "content": thinking_content,
                                "thinking_time": thinking_time,
                                "usage": usage_data
                            })
                        except Exception:
                            pass
                        thinking_content = ""
                        thinking_start = None

                    text_content += content
                    # Emit per-chunk text.delta. JSON-mode streams previously
                    # went silent for the whole content phase; that's the 18s
                    # gap reproduced on project 0a711fa7-c0ab-4e56-b458-e1efb007fb2e
                    # (thinking.done at 16:41:49.692, message_appended at
                    # 16:42:07.564, [STREAM.end] elapsed=26.91s). Streaming
                    # the raw JSON chunks lets the UI show what the model is
                    # actually producing instead of a generic "doing
                    # something" placeholder.
                    if event_callback:
                        try:
                            await event_callback({"type": "text.delta", "content": content})
                        except Exception:
                            pass

                # Check finish
                if chunk.choices[0].finish_reason:
                    # Emit any remaining thinking
                    if thinking_content and thinking_start and event_callback:
                        thinking_time = time.time() - thinking_start
                        logger.info(
                            "[THINKING.DONE.EMIT] site=stream_completion_with_json#finish_reason "
                            "model=%s elapsed=%.3fs chars=%d finish=%s",
                            model_name, thinking_time, len(thinking_content),
                            chunk.choices[0].finish_reason,
                        )
                        try:
                            await event_callback({
                                "type": "thinking.done",
                                "content": thinking_content,
                                "thinking_time": thinking_time,
                                "usage": usage_data
                            })
                        except Exception:
                            pass

            # Notify the UI that the streaming output is finalised. This is
            # the signal LiveActivity uses to drop the live "writing" box.
            if event_callback and text_content:
                try:
                    await event_callback({
                        "type": "text.done",
                        "content": text_content,
                        "usage": usage_data,
                    })
                except Exception:
                    pass

            # Parse JSON from accumulated text
            return parse_llm_json_object(text_content)
            
        except Exception as e:
            raise RuntimeError(f"LLM streaming JSON call failed: {str(e)}")
    
    async def stream_completion(
        self,
        messages: List[Dict[str, str]],
        model: Optional[str] = None,
        temperature: TemperatureInput = UNSET,
        api_key_override: Optional[str] = None,
        fallback_models_override: Optional[List[str]] = None,
        **kwargs
    ):
        """
        Stream chat completion with reasoning support.
        
        Yields:
            Dict events with type 'text.delta', 'thinking.delta', 'thinking.done', or 'text.done'
        """
        import time
        
        try:
            # Determine model and temperature
            selected_model = model or self.default_model
            selected_temperature = resolve_temperature_input(
                temperature,
                default_temperature=self.default_temperature,
            )
            
            _model_config, selected_temperature = await self._resolve_temperature_policy(
                selected_model,
                selected_temperature,
            )
            
            # Format model name for provider (Bifrost)
            model_name = self._format_model_name(selected_model)
            
            # Prepare request parameters
            request_params = {
                "model": model_name,
                "messages": messages,
                "stream": True,
                "stream_options": {"include_usage": True}
            }
            if selected_temperature is not None:
                request_params["temperature"] = selected_temperature
            
            # Add Bifrost fallbacks if enabled. None = caller didn't override,
            # use the instance default; [] = caller explicitly disabled fallback.
            effective_fallback_models = (
                fallback_models_override if fallback_models_override is not None else self.fallback_models
            )
            if self.use_bifrost and effective_fallback_models:
                request_params['extra_body'] = {'fallbacks': effective_fallback_models}
            
            # Merge any additional kwargs
            request_params.update(kwargs)
            
            client = self._get_or_create_client(api_key_override)
            if not client:
                raise ValueError("OpenAI API key or BIFROST_VK not provided")
            try:
                stream = await client.chat.completions.create(**request_params)
            except BadRequestError as exc:
                named = self._bad_request_named_sampling_params(exc)
                if not named:
                    raise
                dropped = self._strip_named_sampling_params(
                    request_params, named,
                    reasoning_mandatory=_model_config.reasoning.mandatory,
                )
                if not dropped:
                    raise
                logger.warning(
                    "[MODEL_PARAMS] model=%s dropped=%s retrying without them — %s",
                    model_name, dropped, exc,
                )
                stream = await client.chat.completions.create(**request_params)
                yield {"type": "param_downgrade", "dropped": dropped}

            thinking_start = None

            thinking_content = ""
            text_content = ""
            # Once thinking.done has been emitted for this stream, we must not
            # emit it again — even if the upstream sends another chunk that
            # carries finish_reason. Verified on project 21ee263f-...: two
            # finish_reason chunks arrived 3ms apart, both triggering
            # thinking.done with the same content (chars=3630, elapsed=0.239s
            # vs 0.242s). Before this guard, the duplicate was emitted to the
            # UI as two collapsed-thought bubbles for the same agent.
            thinking_done_emitted = False

            stream_started_at = time.time()
            chunk_count = 0
            last_chunk_at = stream_started_at
            substitution_checked = False

            async for chunk in stream:
                chunk_count += 1
                if not substitution_checked:
                    served_model = getattr(chunk, "model", None)
                    if served_model:
                        substitution_checked = True
                    self._warn_if_model_substituted(model_name, served_model)
                # ----- D: per-chunk diagnostic logging -----
                # Off by default. Enable with LOG_LLM_CHUNKS=true to capture the
                # upstream chunk timing — proves whether a "47s of silence"
                # report is genuine upstream buffering or a pipeline bug.
                if _LOG_LLM_CHUNKS:
                    now = time.time()
                    gap_ms = (now - last_chunk_at) * 1000
                    last_chunk_at = now
                    if chunk.choices:
                        _d = chunk.choices[0].delta
                        _reason = getattr(_d, 'reasoning', None) or getattr(_d, 'reasoning_content', None)
                        if _reason is None and hasattr(_d, '__dict__'):
                            _reason = _d.__dict__.get('reasoning') or _d.__dict__.get('reasoning_content')
                        _content = getattr(_d, 'content', None)
                        _finish = chunk.choices[0].finish_reason
                        logger.info(
                            "[LLM_CHUNK] model=%s idx=%d gap=%.1fms reasoning=%d content=%d finish=%s",
                            model_name, chunk_count, gap_ms,
                            len(_reason) if _reason else 0,
                            len(_content) if _content else 0,
                            _finish,
                        )
                    else:
                        logger.info(
                            "[LLM_CHUNK] model=%s idx=%d gap=%.1fms no_choices=1 has_usage=%s",
                            model_name, chunk_count, gap_ms,
                            getattr(chunk, 'usage', None) is not None,
                        )

                if not chunk.choices:
                    continue

                delta = chunk.choices[0].delta

                # Handle reasoning/thinking content (OpenRouter returns in delta.reasoning)
                reasoning = getattr(delta, 'reasoning', None) or getattr(delta, 'reasoning_content', None)
                if reasoning is None and hasattr(delta, '__dict__'):
                    reasoning = delta.__dict__.get('reasoning') or delta.__dict__.get('reasoning_content')

                if reasoning:
                    if thinking_start is None:
                        thinking_start = time.time()
                    thinking_content += reasoning
                    yield {"type": "thinking.delta", "content": reasoning}

                # Handle regular text content
                content = getattr(delta, 'content', None)
                if content:
                    # If we were thinking and now getting content, emit thinking.done
                    if thinking_content and thinking_start and not thinking_done_emitted:
                        thinking_time = time.time() - thinking_start
                        logger.info(
                            "[THINKING.DONE.EMIT] site=stream_completion#text-arrived "
                            "model=%s elapsed=%.3fs chars=%d",
                            model_name, thinking_time, len(thinking_content),
                        )
                        yield {"type": "thinking.done", "content": thinking_content, "thinking_time": thinking_time}
                        thinking_done_emitted = True
                        thinking_content = ""
                        thinking_start = None

                    text_content += content
                    yield {"type": "text.delta", "content": content}

                # Check finish reason
                finish_reason = chunk.choices[0].finish_reason
                if finish_reason:
                    # Emit any remaining thinking content (only once per stream).
                    if thinking_content and thinking_start and not thinking_done_emitted:
                        thinking_time = time.time() - thinking_start
                        logger.info(
                            "[THINKING.DONE.EMIT] site=stream_completion#finish_reason "
                            "model=%s elapsed=%.3fs chars=%d finish=%s",
                            model_name, thinking_time, len(thinking_content),
                            finish_reason,
                        )
                        yield {"type": "thinking.done", "content": thinking_content, "thinking_time": thinking_time}
                        thinking_done_emitted = True
                        thinking_content = ""
                        thinking_start = None
                    yield {"type": "text.done", "content": text_content, "finish_reason": finish_reason}

        except Exception as e:
            raise RuntimeError(f"LLM streaming failed: {str(e)}")
    
    # ------------------------------------------------------------------
    # Chat Completions helpers (used for multi-turn tool call rounds)
    # ------------------------------------------------------------------

    @staticmethod
    def _sanitize_fn_name(name: str) -> str:
        """Clamp/sanitize Chat Completions function names to [a-zA-Z0-9_-] and max 64 chars."""
        from tools.mcp_tool_ids import OPENAI_FUNCTION_NAME_MAX_LEN, clamp_openai_function_name

        sanitized = re.sub(r"[^a-zA-Z0-9_-]", "_", str(name or ""))
        return clamp_openai_function_name(sanitized, max_len=OPENAI_FUNCTION_NAME_MAX_LEN)

    def _warn_if_model_substituted(
        self, requested_model: str, served_model: Optional[str]
    ) -> None:
        """Bifrost can silently serve a different model than the one
        requested (its own fallback chain kicking in on an error) — the
        response/chunk always says which model actually answered. Without
        this, a substitution like gpt-5-mini → gpt-4o leaves zero trace
        anywhere in our own logs. Shared by both the Responses path (round 1)
        and the Chat Completions path (round 2+, tool-result rounds) so a
        fallback landing on either round is equally visible.
        """
        if not served_model:
            return
        requested_bare = requested_model
        bifrost_prefix = f"{self.bifrost_provider}/"
        if requested_bare.startswith(bifrost_prefix):
            requested_bare = requested_bare[len(bifrost_prefix):]
        if served_model == requested_bare:
            return
        # A bare model id ("gpt-5-pro") and its provider-qualified form
        # ("openai/gpt-5-pro") are the same model, just missing the provider
        # prefix on the requested side (a normal, supported input shape —
        # /api/settings/test-model accepts bare ids) — compare the trailing
        # segment too before calling it a substitution, or every bare-id
        # call falsely warns.
        if requested_bare.rsplit("/", 1)[-1] == served_model.rsplit("/", 1)[-1]:
            return
        logger.warning(
            "[MODEL_SUBSTITUTION] requested=%s served=%s "
            "— provider served a different model than "
            "requested (likely a Bifrost fallback)",
            requested_bare,
            served_model,
        )

    @staticmethod
    def _convert_input_to_chat_messages(
        input_items: List[Dict[str, Any]],
        name_fwd: Optional[Dict[str, str]] = None,
    ) -> List[Dict[str, Any]]:
        """Convert Responses API input items to Chat Completions messages format.

        Groups consecutive function_call items into a single assistant message
        with tool_calls, and maps function_call_output to role=tool messages.

        name_fwd: optional mapping {original_name → sanitized_name} used to
        sanitize function names so they conform to Chat Completions rules.
        """
        messages: List[Dict[str, Any]] = []
        pending_tool_calls: List[Dict[str, Any]] = []

        def flush_tool_calls() -> None:
            if pending_tool_calls:
                # Omit "content" entirely (not null) — some routers reject content:null
                messages.append({"role": "assistant", "tool_calls": list(pending_tool_calls)})
                pending_tool_calls.clear()

        for item in input_items:
            if not isinstance(item, dict):
                continue
            item_type = item.get("type")

            if item_type == "message":
                flush_tool_calls()
                role = item.get("role", "user")
                content = item.get("content", "")
                if isinstance(content, list):
                    # Preserve vision parts for Chat Completions; flatten text-only otherwise.
                    parts: List[Dict[str, Any]] = []
                    has_image = False
                    for block in content:
                        if not isinstance(block, dict):
                            continue
                        btype = block.get("type")
                        if btype in ("text", "input_text"):
                            parts.append({"type": "text", "text": block.get("text", "")})
                        elif btype in ("input_image", "image_url"):
                            url = block.get("image_url")
                            if isinstance(url, dict):
                                url = url.get("url")
                            if isinstance(url, str) and url:
                                has_image = True
                                parts.append(
                                    {"type": "image_url", "image_url": {"url": url}}
                                )
                    content = parts if has_image else "\n".join(
                        p.get("text", "") for p in parts if p.get("type") == "text"
                    )
                messages.append({"role": role, "content": content})

            elif item_type == "function_call":
                original_name = item.get("name", "")
                fn_name = (name_fwd or {}).get(original_name, original_name)
                pending_tool_calls.append({
                    "id": item.get("call_id") or item.get("id", ""),
                    "type": "function",
                    "function": {
                        "name": fn_name,
                        "arguments": item.get("arguments", ""),
                    },
                })

            elif item_type == "function_call_output":
                flush_tool_calls()
                messages.append({
                    "role": "tool",
                    "tool_call_id": item.get("call_id", ""),
                    "content": item.get("output", ""),
                })

        flush_tool_calls()
        return messages

    @staticmethod
    def _normalize_tools_for_chat(tools: Optional[List[Dict]]) -> Optional[List[Dict]]:
        """Convert tools to Chat Completions format (nested under 'function' key)."""
        if not tools:
            return None
        result = []
        for t in tools:
            if not isinstance(t, dict):
                continue
            if t.get("type") == "function":
                # Already Chat Completions format if has nested "function" key
                if "function" in t:
                    result.append(t)
                    continue
                # Responses API format: {type, name, parameters, description}
                fn_name = t.get("name", "")
                fn_params = t.get("parameters", {})
                fn_desc = t.get("description")
                if fn_name:
                    entry: Dict[str, Any] = {
                        "type": "function",
                        "function": {"name": fn_name, "parameters": fn_params},
                    }
                    if fn_desc:
                        entry["function"]["description"] = fn_desc
                    result.append(entry)
        return result or None

    async def _stream_via_chat_completions(
        self,
        input_items: List[Dict[str, Any]],
        model_name: str,
        cc_tools: Optional[List[Dict]],
        api_key_override: Optional[str] = None,
        fallback_models_override: Optional[List[str]] = None,
        temperature: Optional[float] = None,
        reasoning_effort: Optional[str] = None,
        reasoning_mandatory: bool = False,
    ):
        """Use Chat Completions streaming API and emit the same event format as stream_responses.

        This is used for round 2+ when there are function_call_output items in the
        input, because the Responses API stateless multi-turn with parallel tool calls
        is not reliably supported by OpenRouter.

        Tool names are sanitized to [a-zA-Z0-9_-] (Chat Completions requirement).
        MCP tools use dot-notation (e.g. 'time.get_current_time') which is invalid —
        dots are replaced with underscores.  A reverse map restores original names
        before yielding tool_call.start events so streaming_agent_runner can route them.
        """
        # Build name maps: original → sanitized, sanitized → original
        name_fwd: Dict[str, str] = {}
        name_rev: Dict[str, str] = {}
        cc_tools_sanitized: Optional[List[Dict]] = None
        if cc_tools:
            cc_tools_sanitized = []
            for tool in cc_tools:
                tool_copy = dict(tool)
                if "function" in tool_copy:
                    fn = dict(tool_copy["function"])
                    original = fn.get("name", "")
                    sanitized = self._sanitize_fn_name(original)
                    if original != sanitized:
                        name_fwd[original] = sanitized
                        name_rev[sanitized] = original
                        fn["name"] = sanitized
                        tool_copy["function"] = fn
                cc_tools_sanitized.append(tool_copy)

        messages = self._convert_input_to_chat_messages(input_items, name_fwd)
        client = self._get_or_create_client(api_key_override)
        if not client:
            raise ValueError("OpenAI API key or BIFROST_VK not provided")

        request_params: Dict[str, Any] = {
            "model": model_name,
            "messages": messages,
            "stream": True,
            "stream_options": {"include_usage": True},
        }
        if cc_tools_sanitized:
            request_params["tools"] = cc_tools_sanitized
        effective_fallback_models = (
            fallback_models_override if fallback_models_override is not None else self.fallback_models
        )
        if self.use_bifrost and effective_fallback_models:
            request_params["extra_body"] = {"fallbacks": effective_fallback_models}
        self._apply_chat_reasoning_effort(request_params, reasoning_effort)
        if temperature is not None:
            request_params["temperature"] = temperature


        msg_summary = [
            f"{m.get('role')}({'tool_calls:' + str(len(m.get('tool_calls', []))) if 'tool_calls' in m else 'tool_call_id:' + str(m.get('tool_call_id','')) if m.get('role') == 'tool' else 'len:' + str(len(str(m.get('content', ''))))})"
            for m in messages
        ]
        logger.info(
            "[CHAT_CC] Starting chat completions stream model=%s messages=%d tools=%d sanitized=%s structure=%s",
            model_name, len(messages), len(cc_tools_sanitized) if cc_tools_sanitized else 0,
            list(name_fwd.keys()), msg_summary,
        )

        try:
            stream = await client.chat.completions.create(**request_params)
        except BadRequestError as exc:
            # A context-window 400 names no sampling param, so it would fall
            # through to the re-raise below as an untyped BadRequestError and the
            # overflow hook would never see it. Convert first.
            overflow = context_overflow_from_bad_request(exc, model=model_name)
            if overflow is not None:
                raise overflow
            named = self._bad_request_named_sampling_params(exc)
            if not named:
                raise
            dropped = self._strip_named_sampling_params(
                request_params, named,
                reasoning_mandatory=reasoning_mandatory,
            )
            if not dropped:
                raise
            logger.warning(
                "[MODEL_PARAMS] model=%s dropped=%s retrying without them — %s",
                model_name, dropped, exc,
            )
            stream = await client.chat.completions.create(**request_params)
            yield {"type": "param_downgrade", "dropped": dropped}

        text_content = ""

        # index -> accumulated tool call data
        tool_calls_acc: Dict[int, Dict[str, str]] = {}
        emitted_start: set = set()
        usage_data: Optional[Dict[str, Any]] = None
        final_finish_reason: Optional[str] = None
        work_flushed = False
        response_id: Optional[str] = None

        def _work_events() -> List[Dict[str, Any]]:
            """tool_call.done / text.done for the model's completed output."""
            evs: List[Dict[str, Any]] = []
            if final_finish_reason == "tool_calls":
                for idx, tc in sorted(tool_calls_acc.items()):
                    evs.append({
                        "type": "tool_call.done",
                        "call_id": tc["call_id"],
                        "name": name_rev.get(tc["name"], tc["name"]),
                        "arguments": tc["arguments"],
                    })
            elif final_finish_reason == "stop":
                if text_content:
                    evs.append({"type": "text.done", "content": text_content})
            else:
                # No clean finish ("length", disconnect, …): flush what accumulated
                if tool_calls_acc:
                    for idx, tc in sorted(tool_calls_acc.items()):
                        if idx not in emitted_start or tc["arguments"]:
                            evs.append({
                                "type": "tool_call.done",
                                "call_id": tc["call_id"],
                                "name": name_rev.get(tc["name"], tc["name"]),
                                "arguments": tc["arguments"],
                            })
                elif text_content:
                    evs.append({"type": "text.done", "content": text_content})
            return evs

        # Manual iteration so only the transport read is guarded: once
        # finish_reason arrives the model's work is complete and flushed
        # inline, so a failure while draining the trailing usage chunk must
        # not discard it — usage is best-effort metadata, tool calls and
        # text are the payload. Pre-finish failures propagate as before.
        chunk_iter = stream.__aiter__()
        substitution_checked = False
        while True:
            try:
                chunk = await chunk_iter.__anext__()
            except StopAsyncIteration:
                break
            except Exception as exc:
                if not work_flushed:
                    raise
                logger.warning(
                    "[CHAT_CC] Stream failed after finish_reason=%s; usage lost: %s",
                    final_finish_reason, exc,
                )
                break

            if not substitution_checked:
                served_model = getattr(chunk, "model", None)
                if served_model:
                    substitution_checked = True
                self._warn_if_model_substituted(model_name, served_model)

            # On OpenRouter-served calls this is the generation id (gen-…), the handle
            # for asking OpenRouter which host served the call. Yielded at once so a
            # stream that dies before response.done still leaves it in the capture.
            if response_id is None and getattr(chunk, "id", None):
                response_id = chunk.id
                yield {"type": "response.started", "response_id": response_id}

            # Usage rides a FINAL chunk that has no choices (include_usage
            # opt-in) — read it before the empty-choices skip or it is lost.
            if getattr(chunk, "usage", None):
                usage_data = {
                    "prompt_tokens": getattr(chunk.usage, "prompt_tokens", 0),
                    "completion_tokens": getattr(chunk.usage, "completion_tokens", 0),
                    "total_tokens": getattr(chunk.usage, "total_tokens", 0),
                }
            if not chunk.choices:
                continue

            choice = chunk.choices[0]
            delta = choice.delta

            if delta.content:
                text_content += delta.content
                yield {"type": "text.delta", "content": delta.content}

            if delta.tool_calls:
                for tc_delta in delta.tool_calls:
                    idx = tc_delta.index
                    if idx not in tool_calls_acc:
                        tool_calls_acc[idx] = {"call_id": "", "name": "", "arguments": ""}

                    tc = tool_calls_acc[idx]
                    if tc_delta.id:
                        tc["call_id"] = tc_delta.id
                    if tc_delta.function:
                        if tc_delta.function.name:
                            tc["name"] = tc_delta.function.name
                        if tc_delta.function.arguments:
                            tc["arguments"] += tc_delta.function.arguments

                    # Emit start event once we have both call_id and name.
                    # Restore original name (undo sanitization) so the agent can route the call.
                    if idx not in emitted_start and tc["call_id"] and tc["name"]:
                        emitted_start.add(idx)
                        original_name = name_rev.get(tc["name"], tc["name"])
                        yield {
                            "type": "tool_call.start",
                            "call_id": tc["call_id"],
                            "item_id": tc["call_id"],
                            "name": original_name,
                        }

                    if tc_delta.function and tc_delta.function.arguments:
                        yield {
                            "type": "tool_call.delta",
                            "call_id": tc["call_id"],
                            "arguments_delta": tc_delta.function.arguments,
                        }

            # Flush AFTER this chunk's deltas: providers may attach content
            # and finish_reason to the same chunk.
            if choice.finish_reason and not work_flushed:
                final_finish_reason = choice.finish_reason
                for ev in _work_events():
                    yield ev
                work_flushed = True

        if not work_flushed:
            for ev in _work_events():
                yield ev
        # response.done is the accounting envelope — emitted last so it can
        # carry the trailing usage chunk (include_usage) when it arrives.
        yield {
            "type": "response.done",
            "response": None,
            "response_id": response_id,
            "usage": usage_data,
            "stop_reason": final_finish_reason,
        }

    # ------------------------------------------------------------------

    async def stream_responses(
        self,
        input_items: List[Dict[str, Any]],
        model: Optional[str] = None,
        tools: Optional[List[Dict]] = None,
        reasoning_effort: Optional[str] = "medium",
        reasoning_summary: Optional[str] = "auto",
        api_key_override: Optional[str] = None,
        fallback_models_override: Optional[List[str]] = None,
        **kwargs
    ):
        """
        Stream responses using the Responses API with thinking + tool call support.

        For round 2+ calls (input contains function_call_output items) automatically
        switches to Chat Completions API which has universal provider support for
        multi-turn tool calls, including parallel tool calls via OpenRouter.
        """
        # Round 2+ detection: input contains tool results → use Chat Completions API.
        # Responses API stateless multi-turn (especially with parallel tool calls) is
        # not reliably supported by OpenRouter.  Chat Completions API is universal.
        temperature: Optional[float] = kwargs.pop("temperature", None)

        has_tool_results = any(
            isinstance(item, dict) and item.get("type") == "function_call_output"
            for item in input_items
        )
        if has_tool_results:
            try:
                selected_model = model or self.default_model
                model_name = self._format_model_name(selected_model)
                cc_tools = self._normalize_tools_for_chat(tools)
                from llm.model_config import ModelCapability
                model_cfg = await self._resolve_model_config(selected_model)
                effective_temperature = model_cfg.resolve_temperature(temperature)
                effective_reasoning_effort = self._validate_runtime_reasoning_effort(
                    model=selected_model,
                    model_config=model_cfg,
                    reasoning_effort=reasoning_effort,
                )
                logger.info(
                    "[CHAT_CC] Switching to Chat Completions API for multi-turn tool round "
                    "model=%s items=%d temperature=%s reasoning_effort=%s",
                    model_name,
                    len(input_items),
                    effective_temperature,
                    effective_reasoning_effort,
                )
                async for event in self._stream_via_chat_completions(
                    input_items,
                    model_name,
                    cc_tools,
                    api_key_override,
                    fallback_models_override=fallback_models_override,
                    temperature=effective_temperature,
                    reasoning_effort=effective_reasoning_effort,
                    reasoning_mandatory=model_cfg.reasoning.mandatory,
                ):
                    yield event
            except (AgentModelParamsValidationError, ContextOverflowError):
                # Errors the runner has a specific recovery for must travel
                # up as exceptions, not down as error events (overflow hook).
                raise
            except Exception as e:
                logger.error("[CHAT_CC] Stream error: %s", e)
                yield {"type": "error", "error": str(e), "error_type": type(e).__name__}
            return


        try:
            selected_model = model or self.default_model
            model_name = self._format_model_name(selected_model)

            normalized_tools: Optional[List[Dict[str, Any]]] = None
            # MCP tools use dot-notation names (e.g. 'urbanprojects.getprojectbyid')
            # which violate the function-name rule [a-zA-Z0-9_-]+ enforced by the
            # Responses API (same requirement as Chat Completions). Sanitize for the
            # wire and restore the original on tool-call events so streaming_agent_runner
            # can still route by the real id. Mirrors _stream_via_chat_completions.
            name_rev: Dict[str, str] = {}
            if tools:
                normalized_tools = []
                for t in tools:
                    if not isinstance(t, dict):
                        continue
                    t_type = t.get("type")
                    if t_type == "function" and isinstance(t.get("function"), dict):
                        fn = t.get("function") or {}
                        name = fn.get("name")
                        params = fn.get("parameters")
                        if not name or not isinstance(name, str) or not isinstance(params, dict):
                            continue
                        sanitized = self._sanitize_fn_name(name)
                        if sanitized != name:
                            name_rev[sanitized] = name
                        normalized_tools.append(
                            {
                                "type": "function",
                                "name": sanitized,
                                "parameters": params,
                                "description": fn.get("description"),
                            }
                        )
                    elif t_type == "function":
                        name = t.get("name")
                        params = t.get("parameters")
                        if not name or not isinstance(name, str) or not isinstance(params, dict):
                            continue
                        sanitized = self._sanitize_fn_name(name)
                        if sanitized != name:
                            name_rev[sanitized] = name
                        normalized_tools.append({"type": "function", "name": sanitized, "parameters": params, "description": t.get("description")})
                    else:
                        normalized_tools.append(t)
            
            from llm.model_config import ModelCapability
            model_cfg = await self._resolve_model_config(selected_model)
            supports_reasoning = model_cfg.supports(ModelCapability.REASONING)
            effective_temperature = model_cfg.resolve_temperature(temperature)
            effective_reasoning_effort = self._validate_runtime_reasoning_effort(
                model=selected_model,
                model_config=model_cfg,
                reasoning_effort=reasoning_effort,
            )

            request_params: Dict[str, Any] = {
                "model": model_name,
                "input": input_items,
                "stream": True,
            }
            if supports_reasoning:
                reasoning_params = {}
                if effective_reasoning_effort:
                    reasoning_params["effort"] = effective_reasoning_effort
                    if reasoning_summary:
                        reasoning_params["summary"] = reasoning_summary
                if reasoning_params:
                    request_params["reasoning"] = reasoning_params
            else:
                logger.debug(
                    "[RESPONSES] model=%s does not support reasoning — skipping reasoning param",
                    model_name,
                )
            
            if normalized_tools:
                request_params["tools"] = normalized_tools

            if effective_temperature is not None:
                request_params["temperature"] = effective_temperature

            effective_fallback_models = (
                fallback_models_override if fallback_models_override is not None else self.fallback_models
            )
            if self.use_bifrost and effective_fallback_models:
                request_params['extra_body'] = {'fallbacks': effective_fallback_models}
            
            request_params.update(kwargs)
            
            client = self._get_or_create_client(api_key_override)
            if not client:
                raise ValueError("OpenAI API key or BIFROST_VK not provided")
            
            thinking_start = None
            thinking_content = ""
            tool_calls_by_item_id: Dict[str, Dict[str, str]] = {}
            item_id_to_call_id: Dict[str, str] = {}
            text_content = ""
            
            tool_count_raw = len(tools) if tools else 0
            tool_count_norm = len(normalized_tools) if normalized_tools else 0
            tool_names = [t.get("name", "?") for t in (normalized_tools or [])]
            logger.info(f"[RESPONSES] Starting stream for model={model_name} input_count={len(input_items)} tools_raw={tool_count_raw} tools_normalized={tool_count_norm} tool_names={tool_names}")
            try:
                stream = await client.responses.create(**request_params)
            except BadRequestError as exc:
                # A context-window 400 names no sampling param, so it would fall
                # through to the re-raise below as an untyped BadRequestError and
                # the overflow hook would never see it. Convert first.
                overflow = context_overflow_from_bad_request(exc, model=model_name)
                if overflow is not None:
                    raise overflow
                named = self._bad_request_named_sampling_params(exc)
                if not named:
                    raise

                # Reasoning named on a mandatory-reasoning model can't be
                # fixed by stripping it (that just guarantees a second 400)
                # — the catalog's own default_effort may be stale or the
                # request may have arrived here without going through the
                # agent-level resolver at all (e.g. a Bifrost fallback body
                # reused for a different, mandatory, model). Repair the
                # reasoning param with the catalog default and retry once
                # instead of stripping it.
                if "reasoning" in named and model_cfg.reasoning.mandatory:
                    default_effort = model_cfg.reasoning.default_effort or "medium"
                    request_params["reasoning"] = {"effort": default_effort}
                    remaining = named - {"reasoning"}
                    dropped: Dict[str, Any] = {}
                    if remaining:
                        dropped = self._strip_named_sampling_params(
                            request_params, remaining,
                            reasoning_mandatory=model_cfg.reasoning.mandatory,
                        )
                    logger.warning(
                        "[MODEL_PARAMS] model=%s reasoning named in 400, "
                        "retrying with catalog default_effort=%s dropped=%s — %s",
                        model_name, default_effort, dropped, exc,
                    )
                    stream = await client.responses.create(**request_params)
                    yield {
                        "type": "param_downgrade",
                        "dropped": dropped,
                        "reasoning_effort_repaired": default_effort,
                    }
                else:
                    dropped = self._strip_named_sampling_params(
                        request_params, named,
                        reasoning_mandatory=model_cfg.reasoning.mandatory,
                    )
                    if not dropped:
                        raise
                    logger.warning(
                        "[MODEL_PARAMS] model=%s dropped=%s retrying without them — %s",
                        model_name, dropped, exc,
                    )
                    stream = await client.responses.create(**request_params)
                    yield {"type": "param_downgrade", "dropped": dropped}
            current_response_id: Optional[str] = None
            
            async for event in stream:
                event_type = getattr(event, 'type', None)
                maybe_response = getattr(event, 'response', None)
                if maybe_response is not None and current_response_id is None:
                    rid = getattr(maybe_response, 'id', None)
                    if rid is None and isinstance(maybe_response, dict):
                        rid = maybe_response.get('id')
                    if rid and isinstance(rid, str):
                        current_response_id = rid
                        yield {"type": "response.started", "response_id": current_response_id}
                
                if event_type == 'response.reasoning_summary_text.delta':
                    if thinking_start is None:
                        thinking_start = time.time()
                    delta = getattr(event, 'delta', '')
                    thinking_content += delta
                    yield {"type": "thinking.delta", "content": delta}
                
                elif event_type == 'response.reasoning_summary_text.done':
                    thinking_time = time.time() - thinking_start if thinking_start else 0
                    logger.info(
                        "[THINKING.DONE.EMIT] site=stream_responses#reasoning_summary_text.done "
                        "model=%s elapsed=%.3fs chars=%d",
                        model_name, thinking_time, len(thinking_content),
                    )
                    yield {"type": "thinking.done", "content": thinking_content, "thinking_time": thinking_time}
                    thinking_content = ""
                    thinking_start = None
                
                elif event_type == 'response.output_item.added':
                    item = getattr(event, 'item', {})
                    if getattr(item, 'type', None) == 'function_call':
                        item_id = getattr(item, 'id', '')
                        call_id = getattr(item, 'call_id', '')
                        name = getattr(item, 'name', '')
                        if not item_id and isinstance(item, dict):
                            item_id = item.get('id', '')
                        if not call_id and isinstance(item, dict):
                            call_id = item.get('call_id', '')
                        if not name and isinstance(item, dict):
                            name = item.get('name', '')
                        # Undo wire-sanitization so downstream routing uses the real id.
                        name = name_rev.get(name, name)
                        if not item_id:
                            item_id = call_id
                        if not call_id:
                            call_id = item_id
                        tool_calls_by_item_id[item_id] = {"call_id": call_id, "name": name, "arguments": ""}
                        item_id_to_call_id[item_id] = call_id
                        yield {"type": "tool_call.start", "call_id": call_id, "item_id": item_id, "name": name}
                
                elif event_type == 'response.function_call_arguments.delta':
                    item_id = getattr(event, 'item_id', '')
                    delta = getattr(event, 'delta', '')
                    call_id = item_id_to_call_id.get(item_id, item_id)
                    if item_id in tool_calls_by_item_id:
                        tool_calls_by_item_id[item_id]["arguments"] += delta
                    yield {"type": "tool_call.delta", "call_id": call_id, "arguments_delta": delta}
                
                elif event_type == 'response.function_call_arguments.done':
                    item_id = getattr(event, 'item_id', '')
                    args = getattr(event, 'arguments', '')
                    call_id = item_id_to_call_id.get(item_id, item_id)
                    tc = tool_calls_by_item_id.get(item_id, {})
                    name = tc.get("name", "")
                    yield {"type": "tool_call.done", "call_id": call_id, "name": name, "arguments": args}
                
                elif event_type == 'response.output_text.delta':
                    delta = getattr(event, 'delta', '')
                    text_content += delta
                    yield {"type": "text.delta", "content": delta}
                
                elif event_type == 'response.output_text.done':
                    yield {"type": "text.done", "content": text_content}
                
                elif event_type in ('response.completed', 'response.incomplete', 'response.failed'):
                    # Extract usage data from response
                    response = getattr(event, 'response', None)
                    usage_data = None
                    if response:
                        usage = getattr(response, 'usage', None)
                        if usage is None and isinstance(response, dict):
                            usage = response.get('usage')
                        if usage:
                            usage_data = {
                                "prompt_tokens": getattr(usage, 'input_tokens', 0) or (usage.get('input_tokens', 0) if isinstance(usage, dict) else 0),
                                "completion_tokens": getattr(usage, 'output_tokens', 0) or (usage.get('output_tokens', 0) if isinstance(usage, dict) else 0),
                                "total_tokens": (getattr(usage, 'input_tokens', 0) or 0) + (getattr(usage, 'output_tokens', 0) or 0) if hasattr(usage, 'input_tokens') else (usage.get('input_tokens', 0) + usage.get('output_tokens', 0) if isinstance(usage, dict) else 0),
                            }
                    
                    # Emit thinking.done for any pending thinking content not yet emitted
                    # (some API providers don't send reasoning_summary_text.done explicitly)
                    if thinking_content:
                        thinking_time = time.time() - thinking_start if thinking_start else 0
                        logger.info(
                            "[THINKING.DONE.EMIT] site=stream_responses#response.completed-fallback "
                            "model=%s elapsed=%.3fs chars=%d",
                            model_name, thinking_time, len(thinking_content),
                        )
                        yield {"type": "thinking.done", "content": thinking_content, "thinking_time": thinking_time, "usage": usage_data}
                        thinking_content = ""
                        thinking_start = None
                    
                    rid = getattr(response, 'id', None)
                    if rid is None and isinstance(response, dict):
                        rid = response.get('id')
                    if rid and isinstance(rid, str):
                        current_response_id = rid

                    served_model = getattr(response, 'model', None)
                    if served_model is None and isinstance(response, dict):
                        served_model = response.get('model')
                    self._warn_if_model_substituted(model_name, served_model)

                    # Mirror the Chat Completions path's finish_reason. The three
                    # terminal Responses events are DISTINCT: response.completed,
                    # response.incomplete (truncation — max_output_tokens arrives
                    # as its OWN event, never as a status on completed), and
                    # response.failed. Deriving stop_reason here is what lets a
                    # truncated turn reach the caller as "length" instead of a
                    # silent partial marked done. Defensive reads: unknown yields
                    # None — never worse than before.
                    stop_reason: Optional[str] = None
                    status = getattr(response, 'status', None)
                    if status is None and isinstance(response, dict):
                        status = response.get('status')
                    if event_type == 'response.failed' or status == 'failed':
                        stop_reason = 'error'
                    elif event_type == 'response.incomplete' or status == 'incomplete':
                        details = getattr(response, 'incomplete_details', None)
                        if details is None and isinstance(response, dict):
                            details = response.get('incomplete_details')
                        reason = getattr(details, 'reason', None)
                        if reason is None and isinstance(details, dict):
                            reason = details.get('reason')
                        # 'max_output_tokens' is the Responses-API spelling of "length".
                        stop_reason = 'length' if reason == 'max_output_tokens' else (reason or 'incomplete')
                    elif tool_calls_by_item_id:
                        stop_reason = 'tool_calls'
                    elif status == 'completed':
                        stop_reason = 'stop'
                    yield {"type": "response.done", "response": response, "response_id": current_response_id, "usage": usage_data, "stop_reason": stop_reason}
                
                elif event_type == 'error':
                    error = getattr(event, 'error', {})
                    yield {"type": "error", "error": str(error)}
                    
        except (AgentModelParamsValidationError, ContextOverflowError):
            # Errors the runner has a specific recovery for must travel
            # up as exceptions, not down as error events (overflow hook).
            raise
        except Exception as e:
            logger.error(f"[RESPONSES] Stream error: {e}")
            yield {"type": "error", "error": str(e), "error_type": type(e).__name__}
