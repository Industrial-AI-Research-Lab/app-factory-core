from datetime import datetime
from typing import Any, Optional
import logging

from bson import ObjectId

from config.configuration_resolution import (
    agent_wire_name_from_doc,
    dedupe_tenant_configs_by_wire_name,
)
from llm.agent_model_params import (
    AgentModelParamsValidationError,
    materialize_agent_model_params,
)
from llm.effective_agent_model_validation import (
    AgentModelParamsTarget,
    validate_effective_agent_model_params,
)

from schemas.configuration_schemas import (
    ENTITY_DESCRIPTION_FIELDS,
    RunConfigurationCreate,
    RunConfigurationResponse,
    RunConfigurationUpdate,
    sync_entity_descriptions_for_save,
)

logger = logging.getLogger(__name__)


class RunConfigStore:
    COLLECTION = "run_configurations"

    def __init__(self, storage_backend):
        self.storage = storage_backend
        self.db = None
        self.collection = None

    async def initialize(self):
        if self.storage is None or self.storage.db is None:
            return

        self.db = self.storage.db
        self.collection = self.db[self.COLLECTION]

        await self.collection.create_index("tenant_id")
        await self.collection.create_index("is_default")
        await self.collection.create_index([("tenant_id", 1), ("name", 1)], unique=False)
        await self.collection.create_index("updated_at")

    def _ensure_collection(self):
        if self.collection is None:
            raise RuntimeError("RunConfigStore collection is not initialized")

    def _doc_to_response(self, doc: Optional[dict[str, Any]]) -> Optional[RunConfigurationResponse]:
        if not doc:
            return None

        normalized = {
            "_id": str(doc.get("_id")),
            "tenant_id": doc.get("tenant_id"),
            "name": doc.get("name"),
            "description": doc.get("description", ""),
            "short_description": doc.get("short_description"),
            "long_description": doc.get("long_description"),
            "models": doc.get("models", {}) or {},
            "agent_configs": doc.get("agent_configs") or None,
            "plugins": doc.get("plugins") or None,
            "is_default": doc.get("is_default", False),
            "approval_mode": doc.get("approval_mode"),
            "metadata": doc.get("metadata") if isinstance(doc.get("metadata"), dict) else {},
            "created_at": doc.get("created_at"),
            "updated_at": doc.get("updated_at"),
        }
        return RunConfigurationResponse.model_validate(normalized)

    async def _is_tenant_provisioned(self, tenant_id: str) -> bool:
        storage = self.storage
        if storage is None or not hasattr(storage, "get_tenant"):
            return False
        tenant = await storage.get_tenant(tenant_id)
        if not tenant:
            return False
        return (
            tenant.get("provisioning_status") == "completed"
            and tenant.get("provisioned_at") is not None
        )

    async def validate_config_document(
        self,
        doc: dict[str, Any],
        *,
        tenant_id: str | None,
    ) -> None:
        """Validate every agent affected by the effective run configuration."""
        raw_agent_configs = doc.get("agent_configs") or {}
        if not isinstance(raw_agent_configs, dict):
            return
        agent_configs: dict[str, dict[str, Any]] = {}
        for agent_id, override in raw_agent_configs.items():
            if hasattr(override, "model_dump"):
                override = override.model_dump(exclude_unset=True)
            if isinstance(override, dict):
                agent_configs[str(agent_id)] = override

        agents_by_id: dict[str, dict[str, Any]] = {}
        models = doc.get("models") or {}
        has_agent_fallback = isinstance(models, dict) and any(
            isinstance(models.get(key), str) and models[key].strip()
            for key in ("agent_default", "default")
        )
        if (
            has_agent_fallback
            and tenant_id
            and hasattr(self.storage, "get_agent_configurations")
        ):
            documents = await self.storage.get_agent_configurations(
                enabled_only=False,
                tenant_id=tenant_id,
            )
            for agent_doc in dedupe_tenant_configs_by_wire_name(
                documents,
                tenant_id,
                enabled_only=True,
            ):
                agent_id = agent_wire_name_from_doc(
                    agent_doc,
                    runtime_tenant_id=tenant_id,
                )
                if agent_id:
                    agents_by_id[agent_id] = agent_doc

        for agent_id in agent_configs:
            if agent_id in agents_by_id:
                continue
            agent_doc = await self.storage.get_agent_configuration(
                agent_id,
                tenant_id=tenant_id,
            )
            if not agent_doc:
                raise AgentModelParamsValidationError(
                    code="unknown_agent",
                    message=f"Unknown agent '{agent_id}' in run configuration",
                    field=f"agent_configs.{agent_id}",
                    context={"agent_id": agent_id},
                )
            agents_by_id[agent_id] = agent_doc

        targets: list[AgentModelParamsTarget] = []
        for agent_id, agent_doc in agents_by_id.items():
            base = materialize_agent_model_params(agent_doc)
            targets.append(
                AgentModelParamsTarget(
                    agent_id=agent_id,
                    model=base["model"],
                    temperature=base["temperature"],
                    reasoning_effort=base["reasoning_effort"],
                )
            )

        await validate_effective_agent_model_params(
            targets,
            run_config={
                "models": models,
                "agent_configs": agent_configs,
            },
            storage=self.storage,
            field_prefix="agent_configs",
        )

    async def list_configs(self, tenant_id: str | None = None) -> list[RunConfigurationResponse]:
        self._ensure_collection()

        if tenant_id is None:
            logger.info(
                "[RUN_CONFIG] tenant_id=None - listing all run configurations for root scope"
            )
            query: dict[str, Any] = {}
        else:
            logger.info(
                "[RUN_CONFIG] tenant_id=%s scope=inheritance",
                tenant_id,
            )
            query = {
                "$or": [
                    {"tenant_id": tenant_id},
                    {"tenant_id": "__system__"},
                ]
            }

        cursor = self.collection.find(query).sort("updated_at", -1)
        docs = await cursor.to_list(length=200)
        return [self._doc_to_response(doc) for doc in docs if doc is not None]

    async def get_config(self, config_id: str) -> Optional[RunConfigurationResponse]:
        self._ensure_collection()

        doc = await self.collection.find_one({"_id": config_id})
        if not doc:
            try:
                doc = await self.collection.find_one({"_id": ObjectId(config_id)})
            except Exception:
                doc = None

        return self._doc_to_response(doc)

    async def create_config(
            self,
            payload: RunConfigurationCreate,
            tenant_id: str | None = None,
    ) -> RunConfigurationResponse:
        self._ensure_collection()

        now = datetime.utcnow()
        data = payload.model_dump(by_alias=True)
        doc = {
            "_id": payload.id,
            "tenant_id": tenant_id,
            "name": data["name"],
            "description": data["description"],
            "short_description": data.get("short_description"),
            "long_description": data.get("long_description"),
            "models": data["models"],
            "agent_configs": data.get("agent_configs"),
            "plugins": data.get("plugins"),
            "is_default": data["is_default"],
            "approval_mode": data.get("approval_mode"),
            "metadata": (
                data.get("metadata") if isinstance(data.get("metadata"), dict) else {}
            ),
            "created_at": now,
            "updated_at": now,
        }
        sync_entity_descriptions_for_save(doc)

        await self.validate_config_document(doc, tenant_id=tenant_id)
        await self.collection.insert_one(doc)
        return self._doc_to_response(doc)

    async def update_config(
            self,
            config_id: str,
            payload: RunConfigurationUpdate,
    ) -> Optional[RunConfigurationResponse]:
        self._ensure_collection()

        doc = await self.collection.find_one({"_id": config_id})
        if not doc:
            try:
                doc = await self.collection.find_one({"_id": ObjectId(config_id)})
            except Exception:
                doc = None

        if not doc:
            return None

        update_data = payload.model_dump(exclude_unset=True)
        if not update_data:
            return self._doc_to_response(doc)

        merged = {**doc, **update_data}
        if ENTITY_DESCRIPTION_FIELDS & update_data.keys():
            sync_entity_descriptions_for_save(
                merged,
                touched=ENTITY_DESCRIPTION_FIELDS & frozenset(update_data.keys()),
                prior=doc,
            )
            for field in ENTITY_DESCRIPTION_FIELDS:
                update_data[field] = merged[field]

        await self.validate_config_document(
            merged,
            tenant_id=doc.get("tenant_id"),
        )
        update_data["updated_at"] = datetime.utcnow()

        await self.collection.update_one(
            {"_id": doc["_id"]},
            {"$set": update_data},
        )

        updated = await self.collection.find_one({"_id": doc["_id"]})
        return self._doc_to_response(updated)

    async def delete_config(self, config_id: str) -> bool:
        self._ensure_collection()

        result = await self.collection.delete_one({"_id": config_id})
        if result.deleted_count > 0:
            return True

        try:
            result = await self.collection.delete_one({"_id": ObjectId(config_id)})
            return result.deleted_count > 0
        except Exception:
            return False
