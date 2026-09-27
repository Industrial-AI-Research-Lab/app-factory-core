"""
MongoDB Storage Backend

Replaces SQLite with MongoDB for production-ready persistence.

Features:
- Structured document storage (no JSON blobs)
- Incremental updates (atomic operations)
- Event persistence with TTL
- Approval state persistence
- Connection pooling and indexes
"""
import uuid
import re
import hashlib
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta, timezone
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
import logging
import inspect
from bson import ObjectId
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError, OperationFailure

from storage.run_config_store import RunConfigStore
from storage.project_finish import TERMINAL_PROJECT_STATUSES, is_finishing
from storage.agent_llm_calls_store import AgentLLMCallsStore
from storage.rolling_summary_store import RollingSummaryStore
from telemetry.run_cache import remember_run_context
from telemetry.run_diagnostics import report_trace_issue
from telemetry.run_identity import finish_run_trace, prepare_run_trace
from schemas.project_listing import (
    PROJECT_LIST_DEFAULT_LIMIT,
    PROJECT_LIST_DEFAULT_SORT,
    PROJECT_LIST_MAX_LIMIT,
    PROJECT_LIST_SORT_FIELDS,
    PROJECT_LIST_UNKNOWN_FACET,
)

logger = logging.getLogger(__name__)

PROTECTED_TENANT_IDS = {"__root__", "__system__"}
EVENTS_TTL_SECONDS = 30 * 24 * 60 * 60
# Map progress holds every finished item answer of a running map node; run
# summaries never show it, and load_map_progress reads it on its own.
RUN_READ_PROJECTION = {"map_progress": 0}


class TenantCascadeNotFoundError(LookupError):
    """Raised when tenant does not exist for cascade delete."""


class TenantCascadeConflictError(RuntimeError):
    """Raised when tenant changed/disappeared during cascade delete."""


class TenantCascadeTransactionsRequiredError(RuntimeError):
    """Raised when atomic cascade cannot run because transactions are unavailable."""


class MongoStorageBackend:
    """
    Async MongoDB storage backend.

    Collections:
    - projects: Project metadata with structured fields
    - tasks: Task execution history
    - events: Event stream with TTL (30 days)
    """
    
    def __init__(
        self,
        connection_string: str,
        database: str = "AppFactory",
        enable_transactions: bool = True
    ):
        """
        Args:
            connection_string: MongoDB connection string
            database: Database name
            enable_transactions: Enable transaction support (requires replica set)
        """
        self.connection_string = connection_string
        self.database_name = database
        self.enable_transactions = enable_transactions
        
        self.client: Optional[AsyncIOMotorClient] = None
        self.db: Optional[AsyncIOMotorDatabase] = None
        
        # Collection references (set in initialize())
        self.projects = None
        self.runs = None
        self.tasks = None
        self.events = None
        self.snapshots = None
        self.password_reset_tokens = None
        self.a2a_collection = None
        self.a2a_task_contracts = None
        self.a2a_task_state = None

    async def initialize(self):
        """Connect to MongoDB and create indexes"""
        logger.info(f"Connecting to MongoDB: {self.database_name}")
        
        self.client = AsyncIOMotorClient(
            self.connection_string,
            maxPoolSize=50,
            minPoolSize=10,
            serverSelectionTimeoutMS=5000
        )
        
        self.db = self.client[self.database_name]
        
        # Collection references
        self.projects = self.db.projects
        self.runs = self.db.runs
        self.tasks = self.db.tasks
        self.events = self.db.events
        self.snapshots = self.db.snapshots
        self.container_logs = self.db.container_logs
        self.agent_configurations = self.db.agent_configurations
        self.workflow_definitions = self.db.workflow_definitions
        self.tool_configurations = self.db.tool_configurations
        self.tool_mcp_configurations = self.db.tool_mcp_configurations
        self.mcp_packages = self.db.mcp_packages
        self.mcp_built_images = self.db.mcp_built_images
        self.mcp_lease_workers = self.db.mcp_lease_workers
        self.users = self.db.users
        self.tenants = self.db.tenants
        self.tenant_settings = self.db.tenant_settings
        self.run_configurations = self.db.run_configurations
        self.password_reset_tokens = self.db.password_reset_tokens
        self.agent_llm_calls = self.db.agent_llm_calls
        self.a2a_collection = self.db.a2a_collection
        self.a2a_task_contracts = self.db.a2a_task_contracts
        self.a2a_task_state = self.db.a2a_task_state
        self.archive_refs = self.db.archive_refs
        self.user_attachments = self.db.user_attachments
        self.tenant_artifacts = self.db.tenant_artifacts
        self.run_config_store = RunConfigStore(self)
        await self.run_config_store.initialize()
        self.agent_llm_calls_store = AgentLLMCallsStore(self)
        await self.agent_llm_calls_store.initialize()
        self.rolling_summary_store = RollingSummaryStore(self)
        await self.rolling_summary_store.initialize()

        # Create indexes
        await self._create_indexes()
        
        logger.info("✅ MongoDB initialized")
    
    async def _create_indexes(self):
        """Create database indexes for performance"""
        logger.info("Creating MongoDB indexes...")
        
        # Projects indexes
        await self.projects.create_index("project_id", unique=True)
        await self.projects.create_index([("status", 1), ("created_at", -1)])
        await self.projects.create_index([("created_at", -1)])
        await self.projects.create_index([("tenant_id", 1), ("created_at", -1)])
        await self.projects.create_index([("tenant_id", 1), ("updated_at", -1)])
        await self.projects.create_index([("tenant_id", 1), ("status", 1), ("created_at", -1)])
        await self.projects.create_index([("tenant_id", 1), ("current_phase", 1), ("created_at", -1)])
        await self.projects.create_index([("tenant_id", 1), ("workflow_id", 1), ("created_at", -1)])
        await self.projects.create_index([("tenant_id", 1), ("run_config_id", 1), ("created_at", -1)])
        
        # Runs indexes
        await self.runs.create_index("run_id", unique=True)
        await self.runs.create_index([("project_id", 1), ("created_at", -1)])
        await self.runs.create_index([("project_id", 1), ("active", 1)])
        
        # Tasks indexes
        await self.tasks.create_index("task_id", unique=True)
        await self.tasks.create_index([("project_id", 1), ("status", 1)])
        await self.tasks.create_index([("created_at", -1)])
        
        # Events indexes with TTL (30 days)
        await self.events.create_index([("project_id", 1), ("timestamp", -1)])
        await self.events.create_index("event_type")
        await self.events.create_index(
            "timestamp",
            expireAfterSeconds=EVENTS_TTL_SECONDS
        )
        # event_id is a UUID v7 minted at emission. Used as the canonical
        # resume pointer on SSE reconnect (Last-Event-ID > event_id) and to
        # drive frontend "process only new events" filtering.
        await self.events.create_index([("project_id", 1), ("event_id", 1)])
        
        # Container logs indexes
        await self.container_logs.create_index([("project_id", 1), ("created_at", -1)])
        await self.container_logs.create_index("kind")
        
        # Snapshots indexes
        await self.snapshots.create_index([("project_id", 1), ("created_at", -1)])
        await self.snapshots.create_index("event_id")
        await self.snapshots.create_index([("type", 1), ("created_at", -1)])
        
        # Agent configurations indexes
        # Drop legacy unique index on 'type' — multiple agents can share a type
        try:
            await self.agent_configurations.drop_index("type_1")
            logger.info("Dropped legacy unique index 'type_1' on agent_configurations")
        except Exception:
            pass  # index doesn't exist, fine
        await self.agent_configurations.create_index("type")
        await self.agent_configurations.create_index("name")
        await self.agent_configurations.create_index("enabled")
        await self.agent_configurations.create_index("tenant_id")
        await self.agent_configurations.create_index(
            [("tenant_id", 1), ("name", 1)],
            unique=True,
            name="tenant_id_1_name_1",
            partialFilterExpression={
                "name": {"$exists": True, "$type": "string"},
            },
        )
        
        # Tool configurations indexes
        try:
            await self.tool_configurations.drop_index("name_1")
            logger.info("Dropped legacy unique index 'name_1' on tool_configurations")
        except Exception:
            pass
        await self.tool_configurations.create_index(
            [("tenant_id", 1), ("name", 1)],
            unique=True,
            name="tenant_id_1_name_1",
        )
        await self.tool_configurations.create_index("category")
        await self.tool_configurations.create_index("enabled")
        await self.tool_configurations.create_index("source")
        await self.tool_configurations.create_index("tenant_id")

        # MCP tool configurations indexes (same shape as builtin tools)
        try:
            await self.tool_mcp_configurations.drop_index("name_1")
            logger.info("Dropped legacy unique index 'name_1' on tool_mcp_configurations")
        except Exception:
            pass
        try:
            await self.tool_mcp_configurations.drop_index("tenant_id_1_mcp_server_1_name_1")
            logger.info(
                "Dropped legacy unique index tenant_id_1_mcp_server_1_name_1 on tool_mcp_configurations"
            )
        except Exception:
            pass
        await self.tool_mcp_configurations.create_index(
            [("tenant_id", 1), ("name", 1)],
            unique=True,
            name="tenant_id_1_mcp_name_1",
            partialFilterExpression={
                "name": {"$exists": True, "$type": "string"},
                "source": "mcp_server",
            },
        )
        await self.tool_mcp_configurations.create_index(
            [("tenant_id", 1), ("mcp_server", 1), ("rpc_name", 1)],
            unique=True,
            name="tenant_id_1_mcp_server_1_rpc_name_1",
            partialFilterExpression={
                "rpc_name": {"$exists": True, "$type": "string"},
                "source": "mcp_server",
            },
        )
        await self.tool_mcp_configurations.create_index("category")
        await self.tool_mcp_configurations.create_index("enabled")
        await self.tool_mcp_configurations.create_index("mcp_server")
        await self.tool_mcp_configurations.create_index("tenant_id")

        await self.mcp_packages.create_index(
            [("tenant_id", 1), ("server_id", 1)],
            unique=True,
            name="tenant_id_1_server_id_1",
        )
        await self.mcp_packages.create_index([("tenant_id", 1), ("status", 1)])
        await self.mcp_built_images.create_index([("tenant_id", 1), ("server_id", 1)])
        await self.mcp_built_images.create_index([("tenant_id", 1), ("image_tag", 1)])
        await self.mcp_lease_workers.create_index("expires_at")
        try:
            await self.tool_mcp_configurations.drop_index("tenant_id_1_llm_function_name_1")
            logger.info(
                "Dropped legacy index tenant_id_1_llm_function_name_1 on tool_mcp_configurations"
            )
        except Exception:
            pass
        
        # Workflow definitions indexes
        await self.workflow_definitions.create_index("is_default")
        await self.workflow_definitions.create_index("tenant_id")
        await self.workflow_definitions.create_index(
            [("tenant_id", 1), ("name", 1)],
            unique=True,
            name="tenant_id_1_name_1",
            partialFilterExpression={
                "name": {"$exists": True, "$type": "string"},
            },
        )
        
        # Projects tenant index (F5 multi-tenancy)
        await self.projects.create_index("tenant_id")
        
        # Users indexes (F5 auth)
        await self.users.create_index("email", unique=True)
        await self.users.create_index("tenant_id")
        await self.users.create_index("role")
        await self.users.create_index("enabled")
        
        # Tenants indexes (F5 multi-tenancy)
        await self.tenants.create_index("enabled")

        # MCP export API keys (tenant_settings.mcp_export_api_keys[].id)
        await self.tenant_settings.create_index("mcp_export_api_keys.id")

        await self.a2a_collection.create_index(
            [("tenant_id", 1), ("name", 1)],
            unique=True,
            name="tenant_name_unique"
        )

        await self.a2a_collection.create_index(
            [("tenant_id", 1), ("enabled", 1)],
            name="tenant_enabled"
        )

        await self.a2a_collection.create_index(
            [("tenant_id", 1), ("last_validated_at", -1)],
            name="tenant_validated_at"
        )

        await self.a2a_task_contracts.create_index(
            [("tenant_id", 1), ("server_id", 1), ("task_id", 1)],
            unique=True,
            name="tenant_server_task_unique",
        )

        await self.a2a_task_state.create_index(
            [("project_id", 1), ("node_id", 1), ("run_id", 1)],
            unique=True,
            name="project_node_run_unique",
        )

        await self.a2a_task_state.create_index(
            [("project_id", 1), ("status", 1)],
            name="project_status",
        )
        # Global (no project_id) — backs list_open_a2a_project_ids' startup-wide
        # sweep for the A2A reconciliation worker (AppFactory-280 Issue 1); the
        # compound index above doesn't help a query with no project_id filter.
        await self.a2a_task_state.create_index(
            [("status", 1)],
            name="status_only",
        )
        # Archive refs — locator records for oversized tool results moved to
        # object storage. Fetch-by-ref is already the default _id index (ref_id
        # IS _id); this compound serves the later tenant-scoped retrieval path,
        # created now so the collection isn't born without it.
        await self.archive_refs.create_index(
            [("tenant_id", 1), ("project_id", 1)],
            name="tenant_project"
        )
        # Serves the per-turn "does this run have refs?" attach check
        # (generic_agent._archive_refs_present) — tenant_project can't (its
        # leading field isn't in that filter).
        await self.archive_refs.create_index(
            [("project_id", 1), ("run_id", 1)],
            name="project_run"
        )

        from storage.file_attachment_store import FileAttachmentStore
        from storage.file_blob_store import FileBlobStore

        await FileAttachmentStore(self, FileBlobStore()).ensure_indexes()

        logger.info("✅ Indexes created")

    async def _upsert_by_tenant_and_name(
        self,
        collection,
        *,
        tenant_id: str,
        wire_name: str,
        doc: dict,
        doc_id: str,
        actor_id: str | None,
        existing_by_name: dict | None = None,
    ) -> str:
        """Upsert configuration by ``(tenant_id, name)``; ``_id`` only set on insert."""
        tenant = str(tenant_id or "").strip()
        wire = str(wire_name or "").strip()
        if not tenant or not wire:
            raise ValueError("tenant_id and wire name are required for configuration upsert")

        existing = existing_by_name
        if existing is None:
            existing = await collection.find_one({"tenant_id": tenant, "name": wire})
        is_new = existing is None
        resolved_id = str(existing["_id"]) if existing is not None else doc_id
        doc["_id"] = resolved_id
        self._stamp_audit_fields(doc, actor_id, is_new)
        set_fields = {key: value for key, value in doc.items() if key != "_id"}
        update_filter = {"_id": resolved_id} if not is_new else {"tenant_id": tenant, "name": wire}
        result = await collection.update_one(
            update_filter,
            {"$set": set_fields, "$setOnInsert": {"_id": resolved_id}},
            upsert=True,
        )
        if result.upserted_id is not None:
            return str(result.upserted_id)
        persisted = await collection.find_one(update_filter)
        return str(persisted["_id"]) if persisted else resolved_id


    def _stamp_audit_fields(self, doc: dict, actor_id: str | None = None, is_new: bool = False) -> dict:
        """Stamp audit fields on a document before saving.

        - is_new=True: set created_at + created_by, also set updated_at/by = same
        - is_new=False: only update updated_at + updated_by
        """
        now_utc = datetime.now(timezone.utc)
        created_at = doc.get("created_at")
        should_set_created_fields = is_new or "created_at" not in doc

        # Best-effort normalization for documents touched after string-based writes.
        if isinstance(created_at, str):
            try:
                created_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            except ValueError:
                created_at = None

        if isinstance(created_at, datetime):
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)
            doc["created_at"] = created_at
        elif should_set_created_fields:
            doc["created_at"] = now_utc

        if should_set_created_fields:
            doc.setdefault("created_by", actor_id or "system")

        doc["updated_at"] = now_utc
        doc["updated_by"] = actor_id or "system"
        return doc

    @staticmethod
    def _serialize_datetimes(value: Any) -> Any:
        """Recursively convert datetime values to ISO strings for API-safe output."""
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, dict):
            return {k: MongoStorageBackend._serialize_datetimes(v) for k, v in value.items()}
        if isinstance(value, list):
            return [MongoStorageBackend._serialize_datetimes(v) for v in value]
        return value

    async def _is_tenant_provisioned(self, tenant_id: str) -> bool:
        """Return True only when tenant provisioning completed successfully."""
        tenant = await self.get_tenant(tenant_id)
        if not tenant:
            return False
        return (
            tenant.get("provisioning_status") == "completed"
            and tenant.get("provisioned_at") is not None
        )

    @staticmethod
    def _tenant_inheritance_scope(tenant_id: str) -> dict:
        """Mongo filter: tenant overrides plus inherited system configs."""
        return {
            "$or": [
                {"tenant_id": tenant_id},
                {"tenant_id": "__system__"},
                {"tenant_id": {"$exists": False}},
            ],
        }

    # ==================== AGENT CONFIGURATIONS ====================

    async def get_agent_configurations(self, enabled_only: bool = True, tenant_id: str | None = None) -> list:
        """Get agent configurations from MongoDB.

        Tenant scope: live inheritance (tenant overrides + ``__system__``).

        Returns the full matching set (no silent ``to_list`` cap). List API
        filter/sort/pagination run in memory after enrich; truncating here
        would drop search hits and break global sort order.
        """
        query = {"enabled": True} if enabled_only else {}
        if tenant_id:
            logger.debug(
                "[TENANT_SCOPE] kind=agents tenant_id=%s scope=inheritance",
                tenant_id,
            )
            query.update(self._tenant_inheritance_scope(tenant_id))
        cursor = self.agent_configurations.find(query)
        # ponytail: full fetch; push sort/skip/limit to Mongo when catalogs hurt
        return await cursor.to_list(length=None)

    async def get_agent_configuration(
        self,
        agent_id: str,
        *,
        tenant_id: str | None = None,
    ) -> dict | None:
        """Get agent by storage ``_id`` or resolve by wire name when ``tenant_id`` is set."""
        from config.configuration_resolution import (
            SYSTEM_TENANT_ID,
            is_wire_configuration_name,
        )

        doc = await self.agent_configurations.find_one({"_id": agent_id})
        if doc:
            return doc
        if not is_wire_configuration_name(agent_id):
            return None
        if tenant_id:
            resolved = await self.resolve_agent_configuration(tenant_id, agent_id)
            if resolved:
                return resolved.document
        system = await self.agent_configurations.find_one(
            {"tenant_id": SYSTEM_TENANT_ID, "name": agent_id},
        )
        if system:
            return system
        query: dict = {"name": agent_id}
        if tenant_id:
            query["tenant_id"] = tenant_id
        return await self.agent_configurations.find_one(query)

    async def find_agent_configuration_by_name(
        self,
        tenant_id: str,
        name: str,
    ) -> dict | None:
        """Find an agent configuration by (tenant_id, name)."""
        return await self.agent_configurations.find_one(
            {"tenant_id": tenant_id, "name": name},
        )

    async def find_agent_configurations_by_wire_id(self, agent_id: str) -> list[dict]:
        """Return every stored document exposing ``agent_id`` as its wire identity."""
        from config.configuration_resolution import agent_wire_name_from_doc

        cursor = self.agent_configurations.find(
            {
                "$or": [
                    {"name": agent_id},
                    {"_id": agent_id},
                    {"type": agent_id},
                ]
            }
        )
        candidates = await cursor.to_list(length=None)
        return [
            doc for doc in candidates
            if agent_wire_name_from_doc(
                doc,
                runtime_tenant_id=str(doc.get("tenant_id") or ""),
            ) == agent_id
        ]

    async def resolve_agent_configuration(self, tenant_id: str, name: str):
        """Resolve agent (tenant, name) with system-tenant fallback."""
        from config.configuration_resolution import resolve_by_tenant_and_name

        return await resolve_by_tenant_and_name(
            self.agent_configurations,
            tenant_id,
            name,
        )

    async def save_agent_configuration(self, config: dict, actor_id: str | None = None) -> str:
        """Upsert agent by ``(tenant_id, name)``; mint opaque ``_id`` for new docs."""
        from config.configuration_resolution import (
            agent_wire_name_from_doc,
            normalize_configuration_identity,
            resolve_configuration_storage_id_for_upsert,
        )

        config = dict(config)
        tenant_id = str(config.get("tenant_id") or "").strip()
        requested_id = str(config.get("_id") or "").strip()
        wire_name = agent_wire_name_from_doc(config, runtime_tenant_id=tenant_id) or str(
            config.get("name") or requested_id or ""
        ).strip()
        if not wire_name:
            raise ValueError("Agent configuration must have a wire name")
        config = normalize_configuration_identity(config, wire_id=wire_name)

        existing_by_name = None
        if tenant_id:
            existing_by_name = await self.find_agent_configuration_by_name(tenant_id, wire_name)
        doc_id = resolve_configuration_storage_id_for_upsert(
            tenant_id=tenant_id,
            wire_name=wire_name,
            existing_by_name=existing_by_name,
            preassigned_id=requested_id,
        )
        return await self._upsert_by_tenant_and_name(
            self.agent_configurations,
            tenant_id=tenant_id,
            wire_name=wire_name,
            doc=config,
            doc_id=doc_id,
            actor_id=actor_id,
            existing_by_name=existing_by_name,
        )

    async def count_agent_configurations(self) -> int:
        """Count documents in agent_configurations collection."""
        return await self.agent_configurations.count_documents({})

    # ==================== WORKFLOW DEFINITIONS ====================

    async def get_workflow_definition(
        self,
        workflow_id: str,
        *,
        tenant_id: str | None = None,
    ) -> dict | None:
        """Get workflow by storage ``_id`` or resolve by wire name."""
        from config.configuration_resolution import is_wire_configuration_name

        doc = await self.workflow_definitions.find_one({"_id": workflow_id})
        if doc:
            return doc
        if not is_wire_configuration_name(workflow_id):
            return None
        if tenant_id:
            resolved = await self.resolve_workflow_definition(tenant_id, workflow_id)
            if resolved:
                return resolved.document
            return await self.workflow_definitions.find_one(
                {"tenant_id": tenant_id, "name": workflow_id},
            )
        from config.configuration_resolution import SYSTEM_TENANT_ID

        return await self.workflow_definitions.find_one(
            {"tenant_id": SYSTEM_TENANT_ID, "name": workflow_id},
        )

    async def find_workflow_definition_by_name(
        self,
        tenant_id: str,
        name: str,
    ) -> dict | None:
        """Find a workflow definition by (tenant_id, name)."""
        return await self.workflow_definitions.find_one(
            {"tenant_id": tenant_id, "name": name},
        )

    async def resolve_workflow_definition(self, tenant_id: str, name: str):
        """Resolve workflow (tenant, name) with system-tenant fallback."""
        from config.configuration_resolution import resolve_by_tenant_and_name

        return await resolve_by_tenant_and_name(
            self.workflow_definitions,
            tenant_id,
            name,
        )

    async def get_default_workflow(self) -> dict | None:
        """Get the default workflow definition."""
        return await self.workflow_definitions.find_one({"is_default": True})

    async def get_workflow_definitions(self, tenant_id: str | None = None) -> list:
        """Get all workflow definitions.

        Tenant scope: live inheritance (tenant overrides + ``__system__``).
        """
        query: dict = {}
        if tenant_id:
            logger.debug(
                "[TENANT_SCOPE] kind=workflows tenant_id=%s scope=inheritance",
                tenant_id,
            )
            query.update(self._tenant_inheritance_scope(tenant_id))
        cursor = self.workflow_definitions.find(query)
        return await cursor.to_list(length=100)

    async def save_workflow_definition(self, definition: dict, actor_id: str | None = None) -> str:
        """Upsert workflow by ``(tenant_id, name)``; mint opaque ``_id`` for new docs."""
        from config.configuration_resolution import (
            agent_wire_name_from_doc,
            normalize_configuration_identity,
            resolve_configuration_storage_id_for_upsert,
        )

        definition = dict(definition)
        tenant_id = str(definition.get("tenant_id") or "").strip()
        requested_id = str(definition.get("_id") or "").strip()
        wire_name = agent_wire_name_from_doc(definition, runtime_tenant_id=tenant_id) or str(
            definition.get("name") or requested_id or ""
        ).strip()
        if not wire_name:
            raise ValueError("Workflow definition must have a wire name")
        definition = normalize_configuration_identity(
            definition,
            wire_id=wire_name,
        )
        existing_by_name = None
        if tenant_id:
            existing_by_name = await self.find_workflow_definition_by_name(tenant_id, wire_name)
        doc_id = resolve_configuration_storage_id_for_upsert(
            tenant_id=tenant_id,
            wire_name=wire_name,
            existing_by_name=existing_by_name,
            preassigned_id=requested_id,
        )
        return await self._upsert_by_tenant_and_name(
            self.workflow_definitions,
            tenant_id=tenant_id,
            wire_name=wire_name,
            doc=definition,
            doc_id=doc_id,
            actor_id=actor_id,
            existing_by_name=existing_by_name,
        )

    async def update_workflow_definition_by_id(
        self, workflow_id: str, tenant_id: str, doc: dict, actor_id: str | None = None
    ) -> str | None:
        """Update a tenant-owned workflow definition directly by ``_id``."""
        set_fields = {k: v for k, v in doc.items() if k != "_id"}
        self._stamp_audit_fields(set_fields, actor_id, is_new=False)
        result = await self.workflow_definitions.update_one(
            {"_id": workflow_id, "tenant_id": tenant_id},
            {"$set": set_fields},
        )
        return workflow_id if result.matched_count > 0 else None

    async def count_workflow_definitions(self) -> int:
        """Count documents in workflow_definitions collection."""
        return await self.workflow_definitions.count_documents({})

    # ==================== TOOL CONFIGURATIONS ====================

    _BUILTIN_TOOL_SOURCE_FILTER = {
        "$or": [
            {"source": {"$exists": False}},
            {"source": {"$ne": "mcp_server"}},
        ],
    }

    async def _build_tool_list_query(
        self,
        *,
        enabled_only: bool,
        tenant_id: str | None,
        extra_filters: dict | None = None,
    ) -> dict:
        parts: list[dict] = []
        if enabled_only:
            parts.append({"enabled": True})
        if extra_filters:
            parts.append(extra_filters)
        if tenant_id:
            logger.debug(
                "[TENANT_SCOPE] kind=tools tenant_id=%s scope=inheritance",
                tenant_id,
            )
            parts.append(self._tenant_inheritance_scope(tenant_id))
        if not parts:
            return {}
        if len(parts) == 1:
            return parts[0]
        return {"$and": parts}

    async def get_tool_configurations(self, enabled_only: bool = True, tenant_id: str | None = None) -> list:
        """Get builtin tool configurations (excludes MCP; see ``get_mcp_tool_configurations``).

        Full matching set (no silent ``to_list`` cap): list filters and the
        category dictionary scan the whole tenant scope; truncating here would
        hide tools that still appear in ``GET /tools/categories``.
        """
        query = await self._build_tool_list_query(
            enabled_only=enabled_only,
            tenant_id=tenant_id,
            extra_filters=self._BUILTIN_TOOL_SOURCE_FILTER,
        )
        cursor = self.tool_configurations.find(query)
        # ponytail: full fetch; push sort/skip/limit to Mongo when catalogs hurt
        return await cursor.to_list(length=None)

    async def get_tool_configuration_category_values(
        self,
        tenant_id: str | None = None,
    ) -> list:
        """Distinct normalized ``category`` values for builtin tools in tenant scope."""
        query = await self._build_tool_list_query(
            enabled_only=False,
            tenant_id=tenant_id,
            extra_filters=self._BUILTIN_TOOL_SOURCE_FILTER,
        )
        pipeline = [
            {"$match": query},
            {
                "$project": {
                    "_id": 0,
                    "category_key": {
                        "$let": {
                            "vars": {
                                "trimmed": {
                                    "$trim": {
                                        # $toString matches str(...) in _tool_list_category_key
                                        "input": {
                                            "$toString": {"$ifNull": ["$category", ""]},
                                        },
                                    },
                                },
                            },
                            "in": {
                                "$cond": [
                                    {"$eq": ["$$trimmed", ""]},
                                    "__unassigned__",
                                    "$$trimmed",
                                ],
                            },
                        },
                    },
                },
            },
            {"$group": {"_id": "$category_key"}},
            {"$sort": {"_id": 1}},
        ]
        # Full distinct set — dictionary has no pagination in the API contract.
        rows = await self.tool_configurations.aggregate(pipeline).to_list(length=None)
        return [str(row.get("_id") or "") for row in rows if row.get("_id")]

    async def get_tool_configuration(self, tool_id: str) -> dict | None:
        """Get a single builtin tool configuration by ``_id``."""
        doc = await self.tool_configurations.find_one({"_id": tool_id})
        if isinstance(doc, dict) and doc.get("source") == "mcp_server":
            return None
        return doc

    async def find_tool_configuration_by_name(
        self,
        tenant_id: str,
        name: str,
    ) -> dict | None:
        """Find a builtin tool by (tenant_id, name)."""
        doc = await self.tool_configurations.find_one(
            {
                "tenant_id": tenant_id,
                "name": name,
                **self._BUILTIN_TOOL_SOURCE_FILTER,
            },
        )
        return doc

    async def resolve_tool_configuration(self, tenant_id: str, name: str):
        """Resolve builtin tool (tenant, name) with system-tenant fallback."""
        from config.configuration_resolution import resolve_by_tenant_and_name

        return await resolve_by_tenant_and_name(
            self.tool_configurations,
            tenant_id,
            name,
        )

    async def find_tool_configurations_by_name(self, name: str, tenant_id: str) -> list:
        """Find builtin tools with the given display name scoped to a tenant."""
        cursor = self.tool_configurations.find(
            {
                "$and": [
                    {"name": name, "tenant_id": {"$in": [tenant_id, "__system__"]}},
                    self._BUILTIN_TOOL_SOURCE_FILTER,
                ],
            }
        )
        return await cursor.to_list(length=20)

    async def save_tool_configuration(self, config: dict, actor_id: str | None = None) -> str:
        """Upsert builtin tool by ``(tenant_id, name)``; mint opaque ``_id`` for new docs."""
        from config.configuration_resolution import (
            agent_wire_name_from_doc,
            normalize_configuration_identity,
            resolve_configuration_storage_id_for_upsert,
        )

        if config.get("source") == "mcp_server":
            raise ValueError("MCP tools must be saved via save_mcp_tool_configuration")
        config = dict(config)
        config.pop("allowed_agents", None)
        tenant_id = str(config.get("tenant_id") or "").strip()
        requested_id = str(config.get("_id") or "").strip()
        wire_name = agent_wire_name_from_doc(config, runtime_tenant_id=tenant_id) or str(
            config.get("name") or requested_id or ""
        ).strip()
        if not wire_name:
            raise ValueError("Tool configuration must have a wire name")
        config = normalize_configuration_identity(config, wire_id=wire_name)
        existing_by_name = None
        if tenant_id:
            existing_by_name = await self.find_tool_configuration_by_name(tenant_id, wire_name)
        existing_by_id = (
            await self.tool_configurations.find_one({"_id": requested_id})
            if requested_id
            else None
        )
        doc_id = resolve_configuration_storage_id_for_upsert(
            tenant_id=tenant_id,
            wire_name=wire_name,
            existing_by_id=existing_by_id,
            existing_by_name=existing_by_name,
            preassigned_id=requested_id,
        )
        return await self._upsert_by_tenant_and_name(
            self.tool_configurations,
            tenant_id=tenant_id,
            wire_name=wire_name,
            doc=config,
            doc_id=doc_id,
            actor_id=actor_id,
            existing_by_name=existing_by_name,
        )

    async def count_tool_configurations(self) -> int:
        """Count builtin documents in tool_configurations (excludes MCP)."""
        return await self.tool_configurations.count_documents(self._BUILTIN_TOOL_SOURCE_FILTER)

    # ==================== MCP TOOL CONFIGURATIONS ====================

    async def get_mcp_tool_configurations(
        self,
        enabled_only: bool = True,
        tenant_id: str | None = None,
    ) -> list:
        """Get imported MCP tools; wizard records live in dedicated collections.

        Full matching set (no silent ``to_list`` cap): connection fan-out and
        list UIs filter in memory after fetch; truncating here would drop tools
        that still appear for the same tenant on a narrower query.
        """
        query = await self._build_tool_list_query(
            enabled_only=enabled_only,
            tenant_id=tenant_id,
            extra_filters={"source": "mcp_server"},
        )
        cursor = self.tool_mcp_configurations.find(query)
        # ponytail: full fetch; push server_id filter to Mongo when catalogs hurt
        return await cursor.to_list(length=None)

    async def get_mcp_tool_configuration(self, tool_id: str) -> dict | None:
        """Get a single MCP tool by storage ``_id``."""
        doc = await self.tool_mcp_configurations.find_one({"_id": tool_id})
        if doc is not None:
            return doc
        legacy = await self.tool_configurations.find_one(
            {"_id": tool_id, "source": "mcp_server"},
        )
        return legacy

    async def find_mcp_tool_configuration_by_wire_name(
        self,
        tenant_id: str,
        wire_name: str,
    ) -> dict | None:
        """Resolve MCP tool by path-A wire ``name`` within a tenant.

        Prefers an exact ``tenant_id`` hit before ``__root__``/``__default__`` alias
        partners so dual legacy rows do not shadow a newer canonical doc.
        """
        wire = str(wire_name or "").strip()
        tid = str(tenant_id or "__root__")
        if not wire:
            return None
        from tools.mcp_tool_ids import tenant_ids_for_storage_lookup

        async def _find_in(collection, tenant_values: list[str]):
            if not tenant_values:
                return None
            if len(tenant_values) == 1:
                return await collection.find_one({
                    "tenant_id": tenant_values[0],
                    "name": wire,
                    "source": "mcp_server",
                })
            return await collection.find_one({
                "tenant_id": {"$in": tenant_values},
                "name": wire,
                "source": "mcp_server",
            })

        tenant_ids = list(tenant_ids_for_storage_lookup(tid))
        exact_first = [tid] if tid in tenant_ids else []
        alias_rest = [t for t in tenant_ids if t != tid]

        for collection in (self.tool_mcp_configurations, self.tool_configurations):
            doc = await _find_in(collection, exact_first)
            if doc is not None:
                return doc
            doc = await _find_in(collection, alias_rest)
            if doc is not None:
                return doc
        return None

    async def find_mcp_tool_configuration_by_llm_function_name(
        self,
        tenant_id: str,
        llm_function_name: str,
    ) -> dict | None:
        """Resolve MCP tool by wire name (legacy ``llm_function_name`` alias)."""
        return await self.find_mcp_tool_configuration_by_wire_name(
            tenant_id,
            llm_function_name,
        )

    async def find_mcp_tool_configurations_by_name(
        self,
        name: str,
        tenant_id: str,
        *,
        mcp_server: str | None = None,
    ) -> list:
        from tools.mcp_tool_ids import tenant_ids_for_storage_lookup

        needle = str(name or "").strip()
        tenant_ids = list(tenant_ids_for_storage_lookup(tenant_id))
        if "__system__" not in tenant_ids:
            tenant_ids.append("__system__")
        query: dict = {
            "tenant_id": {"$in": tenant_ids},
            "source": "mcp_server",
            "$or": [
                {"name": needle},
                {"rpc_name": needle},
            ],
        }
        if mcp_server:
            query["mcp_server"] = mcp_server
        cursor = self.tool_mcp_configurations.find(query)
        return await cursor.to_list(length=20)

    async def claim_mcp_package_build_job(
        self,
        doc_id: str,
        upload_id: str,
        claim: dict,
        *,
        force_rebuild: bool = False,
        actor_id: str | None = None,
    ) -> bool:
        """Atomically set package meta to ``building`` when no in-flight job holds the row."""
        from tools.mcp_package_storage import MCP_PACKAGE_DELETE_RESERVATION_FIELD
        from tools.mcp_zip_build_policy import PACKAGE_BUILD_IN_FLIGHT_STATUSES

        uid = str(upload_id or "").strip()
        if not doc_id or not uid or not isinstance(claim, dict):
            return False
        in_flight = list(PACKAGE_BUILD_IN_FLIGHT_STATUSES)
        if force_rebuild:
            status_filter = {"status": "ready"}
        else:
            status_filter = {
                "status": {"$nin": in_flight + ["ready"]},
            }
        filt: dict = {
            "_id": doc_id,
            "upload_id": uid,
            MCP_PACKAGE_DELETE_RESERVATION_FIELD: {"$exists": False},
            **status_filter,
        }
        set_fields = dict(claim)
        update_doc: dict = {"$set": set_fields}
        audit: dict = {}
        self._stamp_audit_fields(audit, actor_id, is_new=False)
        for key, val in audit.items():
            if key != "_id":
                update_doc["$set"][key] = val
        result = await self.mcp_packages.update_one(filt, update_doc)
        return bool(getattr(result, "matched_count", 0))

    async def claim_mcp_package_deletion(self, doc_id: str, reservation_id: str) -> bool:
        """Atomically reserve a package only while no build or smoke is active."""
        from tools.mcp_package_storage import (
            MCP_PACKAGE_DELETE_RESERVATION_EXPIRES_AT_FIELD,
            MCP_PACKAGE_DELETE_RESERVATION_FIELD,
            MCP_LEASE_OWNER_EPOCH_FIELD,
            _lease_expires_at_iso,
            mcp_lease_worker_epoch,
        )
        from tools.mcp_zip_build_policy import PACKAGE_BUILD_IN_FLIGHT_STATUSES

        token = str(reservation_id or "").strip()
        if not doc_id or not token:
            return False
        result = await self.mcp_packages.update_one(
            {
                "_id": doc_id,
                "status": {"$nin": list(PACKAGE_BUILD_IN_FLIGHT_STATUSES)},
                MCP_PACKAGE_DELETE_RESERVATION_FIELD: {"$exists": False},
            },
            {
                "$set": {
                    MCP_PACKAGE_DELETE_RESERVATION_FIELD: token,
                    MCP_PACKAGE_DELETE_RESERVATION_EXPIRES_AT_FIELD: _lease_expires_at_iso(),
                    MCP_LEASE_OWNER_EPOCH_FIELD: mcp_lease_worker_epoch(),
                }
            },
        )
        return bool(getattr(result, "matched_count", 0))

    async def release_mcp_package_deletion(self, doc_id: str, reservation_id: str) -> bool:
        """Release only the reservation owned by this failed lifecycle delete."""
        from tools.mcp_package_storage import (
            MCP_PACKAGE_DELETE_RESERVATION_EXPIRES_AT_FIELD,
            MCP_PACKAGE_DELETE_RESERVATION_FIELD,
            MCP_LEASE_OWNER_EPOCH_FIELD,
        )

        token = str(reservation_id or "").strip()
        if not doc_id or not token:
            return False
        result = await self.mcp_packages.update_one(
            {"_id": doc_id, MCP_PACKAGE_DELETE_RESERVATION_FIELD: token},
            {
                "$unset": {
                    MCP_PACKAGE_DELETE_RESERVATION_FIELD: "",
                    MCP_PACKAGE_DELETE_RESERVATION_EXPIRES_AT_FIELD: "",
                    MCP_LEASE_OWNER_EPOCH_FIELD: "",
                }
            },
        )
        return bool(getattr(result, "matched_count", 0))

    async def renew_mcp_package_deletion(self, doc_id: str, reservation_id: str) -> bool:
        """Extend only the package deletion lease owned by this request."""
        from tools.mcp_package_storage import (
            MCP_PACKAGE_DELETE_RESERVATION_EXPIRES_AT_FIELD,
            MCP_PACKAGE_DELETE_RESERVATION_FIELD,
            MCP_LEASE_OWNER_EPOCH_FIELD,
            _lease_expires_at_iso,
            mcp_lease_worker_epoch,
        )

        token = str(reservation_id or "").strip()
        if not doc_id or not token:
            return False
        result = await self.mcp_packages.update_one(
            {
                "_id": doc_id,
                MCP_PACKAGE_DELETE_RESERVATION_FIELD: token,
                MCP_LEASE_OWNER_EPOCH_FIELD: mcp_lease_worker_epoch(),
            },
            {"$set": {MCP_PACKAGE_DELETE_RESERVATION_EXPIRES_AT_FIELD: _lease_expires_at_iso()}},
        )
        return bool(getattr(result, "matched_count", 0))

    async def claim_mcp_built_image_deletion(self, doc_id: str, reservation_id: str) -> bool:
        """Atomically reserve one image after all configuration writes finish."""
        from tools.mcp_package_storage import (
            MCP_BUILT_IMAGE_DELETE_RESERVATION_EXPIRES_AT_FIELD,
            MCP_BUILT_IMAGE_DELETE_RESERVATION_FIELD,
            MCP_BUILT_IMAGE_REFERENCE_WRITE_FIELD,
            MCP_LEASE_OWNER_EPOCH_FIELD,
            _lease_expires_at_iso,
            mcp_lease_worker_epoch,
        )

        token = str(reservation_id or "").strip()
        if not doc_id or not token:
            return False
        result = await self.mcp_built_images.update_one(
            {
                "_id": doc_id,
                "record_type": "mcp_built_image",
                MCP_BUILT_IMAGE_DELETE_RESERVATION_FIELD: {"$exists": False},
                f"{MCP_BUILT_IMAGE_REFERENCE_WRITE_FIELD}.0": {"$exists": False},
            },
            {
                "$set": {
                    MCP_BUILT_IMAGE_DELETE_RESERVATION_FIELD: token,
                    MCP_BUILT_IMAGE_DELETE_RESERVATION_EXPIRES_AT_FIELD: _lease_expires_at_iso(),
                    MCP_LEASE_OWNER_EPOCH_FIELD: mcp_lease_worker_epoch(),
                }
            },
        )
        return bool(getattr(result, "matched_count", 0))

    async def release_mcp_built_image_deletion(self, doc_id: str, reservation_id: str) -> bool:
        """Release only the deletion lease held by this request."""
        from tools.mcp_package_storage import (
            MCP_BUILT_IMAGE_DELETE_RESERVATION_EXPIRES_AT_FIELD,
            MCP_BUILT_IMAGE_DELETE_RESERVATION_FIELD,
            MCP_LEASE_OWNER_EPOCH_FIELD,
        )

        token = str(reservation_id or "").strip()
        if not doc_id or not token:
            return False
        result = await self.mcp_built_images.update_one(
            {"_id": doc_id, MCP_BUILT_IMAGE_DELETE_RESERVATION_FIELD: token},
            {
                "$unset": {
                    MCP_BUILT_IMAGE_DELETE_RESERVATION_FIELD: "",
                    MCP_BUILT_IMAGE_DELETE_RESERVATION_EXPIRES_AT_FIELD: "",
                    MCP_LEASE_OWNER_EPOCH_FIELD: "",
                }
            },
        )
        return bool(getattr(result, "matched_count", 0))

    async def renew_mcp_built_image_deletion(self, doc_id: str, reservation_id: str) -> bool:
        """Extend only the image deletion lease owned by this request."""
        from tools.mcp_package_storage import (
            MCP_BUILT_IMAGE_DELETE_RESERVATION_EXPIRES_AT_FIELD,
            MCP_BUILT_IMAGE_DELETE_RESERVATION_FIELD,
            MCP_LEASE_OWNER_EPOCH_FIELD,
            _lease_expires_at_iso,
            mcp_lease_worker_epoch,
        )

        token = str(reservation_id or "").strip()
        if not doc_id or not token:
            return False
        result = await self.mcp_built_images.update_one(
            {
                "_id": doc_id,
                MCP_BUILT_IMAGE_DELETE_RESERVATION_FIELD: token,
                MCP_LEASE_OWNER_EPOCH_FIELD: mcp_lease_worker_epoch(),
            },
            {
                "$set": {
                    MCP_BUILT_IMAGE_DELETE_RESERVATION_EXPIRES_AT_FIELD: _lease_expires_at_iso()
                }
            },
        )
        return bool(getattr(result, "matched_count", 0))

    async def claim_mcp_built_image_reference_write(
        self, doc_id: str, reservation_id: str
    ) -> bool:
        """Atomically prevent image deletion until a configuration write commits."""
        from tools.mcp_package_storage import (
            MCP_BUILT_IMAGE_DELETE_RESERVATION_FIELD,
            MCP_BUILT_IMAGE_REFERENCE_WRITE_EXPIRES_AT_FIELD,
            MCP_BUILT_IMAGE_REFERENCE_WRITE_FIELD,
            _lease_expires_at_iso,
        )

        token = str(reservation_id or "").strip()
        if not doc_id or not token:
            return False
        result = await self.mcp_built_images.update_one(
            {
                "_id": doc_id,
                "record_type": "mcp_built_image",
                "status": "ready",
                MCP_BUILT_IMAGE_DELETE_RESERVATION_FIELD: {"$exists": False},
            },
            {
                "$addToSet": {MCP_BUILT_IMAGE_REFERENCE_WRITE_FIELD: token},
                "$set": {
                    MCP_BUILT_IMAGE_REFERENCE_WRITE_EXPIRES_AT_FIELD: _lease_expires_at_iso()
                },
            },
        )
        return bool(getattr(result, "matched_count", 0))

    async def release_mcp_built_image_reference_write(
        self, doc_id: str, reservation_id: str
    ) -> bool:
        """Release one configuration-write image lease."""
        from tools.mcp_package_storage import (
            MCP_BUILT_IMAGE_REFERENCE_WRITE_EXPIRES_AT_FIELD,
            MCP_BUILT_IMAGE_REFERENCE_WRITE_FIELD,
        )

        token = str(reservation_id or "").strip()
        if not doc_id or not token:
            return False
        result = await self.mcp_built_images.update_one(
            {"_id": doc_id, MCP_BUILT_IMAGE_REFERENCE_WRITE_FIELD: token},
            {"$pull": {MCP_BUILT_IMAGE_REFERENCE_WRITE_FIELD: token}},
        )
        if getattr(result, "matched_count", 0):
            await self.mcp_built_images.update_one(
                {
                    "_id": doc_id,
                    f"{MCP_BUILT_IMAGE_REFERENCE_WRITE_FIELD}.0": {"$exists": False},
                },
                {"$unset": {MCP_BUILT_IMAGE_REFERENCE_WRITE_EXPIRES_AT_FIELD: ""}},
            )
        return bool(getattr(result, "matched_count", 0))

    async def heartbeat_mcp_lease_worker(self, worker_epoch: str, expires_at: str) -> None:
        """Publish a worker liveness heartbeat used by MCP lease recovery."""
        from tools.mcp_package_storage import _utc_now_iso

        await self.mcp_lease_workers.update_one(
            {"_id": worker_epoch},
            {
                "$set": {"expires_at": expires_at, "heartbeat_at": _utc_now_iso()},
                "$setOnInsert": {"created_at": _utc_now_iso()},
            },
            upsert=True,
        )

    async def active_mcp_lease_worker_epochs(self) -> list[str]:
        """List worker epochs whose heartbeat has not expired."""
        from tools.mcp_package_storage import _utc_now_iso

        cursor = self.mcp_lease_workers.find(
            {"expires_at": {"$gt": _utc_now_iso()}}, {"_id": 1}
        )
        return [str(doc.get("_id") or "") for doc in await cursor.to_list(length=None)]

    async def reconcile_expired_mcp_leases(
        self, active_worker_epochs: set[str]
    ) -> dict[str, int]:
        """Clear expired leases only after their owning worker stopped heartbeating."""
        from tools.mcp_package_storage import (
            MCP_BUILT_IMAGE_DELETE_RESERVATION_EXPIRES_AT_FIELD,
            MCP_BUILT_IMAGE_DELETE_RESERVATION_FIELD,
            MCP_BUILT_IMAGE_REFERENCE_WRITE_EXPIRES_AT_FIELD,
            MCP_BUILT_IMAGE_REFERENCE_WRITE_FIELD,
            MCP_LEASE_OWNER_EPOCH_FIELD,
            MCP_PACKAGE_DELETE_RESERVATION_EXPIRES_AT_FIELD,
            MCP_PACKAGE_DELETE_RESERVATION_FIELD,
            _utc_now_iso,
        )

        now = _utc_now_iso()

        def expired_or_orphaned(lease_field: str, expires_at_field: str) -> dict:
            return {
                "$and": [
                    {lease_field: {"$exists": True}},
                    {
                        "$or": [
                            {expires_at_field: {"$exists": False}},
                            {expires_at_field: {"$lte": now}},
                        ]
                    },
                    {MCP_LEASE_OWNER_EPOCH_FIELD: {"$nin": list(active_worker_epochs)}},
                ]
            }

        package_result = await self.mcp_packages.update_many(
            expired_or_orphaned(
                MCP_PACKAGE_DELETE_RESERVATION_FIELD,
                MCP_PACKAGE_DELETE_RESERVATION_EXPIRES_AT_FIELD,
            ),
            {
                "$unset": {
                    MCP_PACKAGE_DELETE_RESERVATION_FIELD: "",
                    MCP_PACKAGE_DELETE_RESERVATION_EXPIRES_AT_FIELD: "",
                    MCP_LEASE_OWNER_EPOCH_FIELD: "",
                }
            },
        )
        image_delete_result = await self.mcp_built_images.update_many(
            expired_or_orphaned(
                MCP_BUILT_IMAGE_DELETE_RESERVATION_FIELD,
                MCP_BUILT_IMAGE_DELETE_RESERVATION_EXPIRES_AT_FIELD,
            ),
            {
                "$unset": {
                    MCP_BUILT_IMAGE_DELETE_RESERVATION_FIELD: "",
                    MCP_BUILT_IMAGE_DELETE_RESERVATION_EXPIRES_AT_FIELD: "",
                    MCP_LEASE_OWNER_EPOCH_FIELD: "",
                }
            },
        )
        image_write_result = await self.mcp_built_images.update_many(
            expired_or_orphaned(
                MCP_BUILT_IMAGE_REFERENCE_WRITE_FIELD,
                MCP_BUILT_IMAGE_REFERENCE_WRITE_EXPIRES_AT_FIELD,
            ),
            {
                "$unset": {
                    MCP_BUILT_IMAGE_REFERENCE_WRITE_FIELD: "",
                    MCP_BUILT_IMAGE_REFERENCE_WRITE_EXPIRES_AT_FIELD: "",
                    MCP_LEASE_OWNER_EPOCH_FIELD: "",
                }
            },
        )
        return {
            "package_deletions": int(getattr(package_result, "modified_count", 0)),
            "image_deletions": int(getattr(image_delete_result, "modified_count", 0)),
            "image_writes": int(getattr(image_write_result, "modified_count", 0)),
        }

    async def update_mcp_package_meta_if_job(
        self,
        doc_id: str,
        job_id: str,
        patch: dict,
        actor_id: str | None = None,
    ) -> bool:
        """Conditionally merge package fields when the top-level ``job_id`` matches."""
        jid = str(job_id or "").strip()
        if not doc_id or not jid or not isinstance(patch, dict):
            return False
        doc = await self.mcp_packages.find_one({"_id": doc_id, "job_id": jid})
        if not doc:
            return False
        update_doc = dict(patch)
        self._stamp_audit_fields(update_doc, actor_id, is_new=False)
        result = await self.mcp_packages.update_one(
            {"_id": doc_id, "job_id": jid},
            {"$set": update_doc},
        )
        return bool(getattr(result, "matched_count", 0))

    async def save_mcp_tool_configuration(self, config: dict, actor_id: str | None = None) -> None:
        from config.configuration_resolution import resolve_configuration_storage_id_for_upsert
        from config.tool_configuration_schema import tool_wire_name_from_doc

        stored_name = str(config.get("name") or "").strip()
        from tools.mcp_internal_docs import (
            MCP_SERVER_INTERNAL_SOURCE,
            is_mcp_internal_tool_name,
        )

        if is_mcp_internal_tool_name(stored_name):
            config.setdefault("source", MCP_SERVER_INTERNAL_SOURCE)
        else:
            config.setdefault("source", "mcp_server")
        config.pop("allowed_agents", None)
        tenant_id = str(config.get("tenant_id") or "__root__").strip()
        from config.configuration_resolution import is_wire_configuration_name
        from tools.mcp_tool_ids import mcp_rpc_name_from_doc, parse_mcp_public_tool_id

        if is_mcp_internal_tool_name(stored_name):
            doc_id = str(config.get("_id") or "").strip()
            if not doc_id:
                raise ValueError("Internal MCP tool configuration must have an _id")
            config["name"] = stored_name
            config["_id"] = doc_id
            existing = await self.tool_mcp_configurations.find_one({"_id": doc_id})
            self._stamp_audit_fields(config, actor_id, is_new=existing is None)
            await self.tool_mcp_configurations.replace_one(
                {"_id": doc_id},
                config,
                upsert=True,
            )
            return

        parsed = parse_mcp_public_tool_id(stored_name)
        if parsed:
            sid, mname = parsed
            if sid:
                config["mcp_server"] = str(config.get("mcp_server") or sid)
            config["rpc_name"] = str(config.get("rpc_name") or mname)
        rpc = str(config.get("rpc_name") or "").strip() or mcp_rpc_name_from_doc(config)
        if rpc:
            config["rpc_name"] = rpc
        if (
            not is_mcp_internal_tool_name(stored_name)
            and not (is_wire_configuration_name(stored_name) and "_" in stored_name)
        ):
            from config.tool_configuration_schema import tool_wire_name_from_doc
            from tools.mcp_llm_function_names import (
                LlmFunctionNameError,
                assign_wire_name_to_mcp_doc,
                resolve_server_abbrs,
            )

            requested_id = str(config.get("_id") or "").strip()
            tenant_docs = await self.get_mcp_tool_configurations(
                enabled_only=False,
                tenant_id=tenant_id,
            )
            existing_names: set[str] = set()
            server_ids: set[str] = set()
            for doc in tenant_docs:
                if not isinstance(doc, dict):
                    continue
                if requested_id and str(doc.get("_id") or "").strip() == requested_id:
                    continue
                wire = str(doc.get("name") or "").strip() or tool_wire_name_from_doc(doc)
                if wire:
                    existing_names.add(wire)
                sid = str(doc.get("mcp_server") or "").strip()
                if sid:
                    server_ids.add(sid)
            server_id = str(config.get("mcp_server") or "").strip()
            if server_id:
                server_ids.add(server_id)
            abbr_map = resolve_server_abbrs(server_ids)
            try:
                assign_wire_name_to_mcp_doc(
                    config,
                    existing_names=existing_names,
                    server_abbr_map=abbr_map,
                    preserve_existing=True,
                )
            except LlmFunctionNameError:
                if parsed:
                    assign_wire_name_to_mcp_doc(
                        config,
                        existing_names=existing_names,
                        server_abbr_map=abbr_map,
                        preserve_existing=False,
                    )
                else:
                    raise
        wire_name = tool_wire_name_from_doc(config)
        if not wire_name:
            raise ValueError("MCP tool configuration must have a wire name")
        from tools.agent_allowed_tools import known_builtin_tool_ids

        # Dispatch resolves MCP docs by name before builtins, so a colliding
        # name silently shadows the builtin for the whole tenant (AppFactory-268).
        if wire_name in known_builtin_tool_ids():
            raise ValueError(
                f"MCP tool wire name '{wire_name}' collides with a builtin tool id"
            )

        requested_id = str(config.get("_id") or "").strip()
        existing_by_id = (
            await self.tool_mcp_configurations.find_one({"_id": requested_id})
            if requested_id
            else None
        )
        existing_by_name = await self.find_mcp_tool_configuration_by_wire_name(tenant_id, wire_name)

        storage_id = resolve_configuration_storage_id_for_upsert(
            tenant_id=tenant_id,
            wire_name=wire_name,
            existing_by_id=existing_by_id,
            existing_by_name=existing_by_name,
            preassigned_id=requested_id,
            incoming_rpc_name=rpc,
            incoming_mcp_server=str(config.get("mcp_server") or "").strip() or None,
        )

        config["name"] = wire_name
        config.pop("llm_function_name", None)
        if existing_by_id is not None:
            storage_id = str(existing_by_id["_id"])
            config["_id"] = storage_id
            self._stamp_audit_fields(config, actor_id, is_new=False)
            set_fields = {key: value for key, value in config.items() if key != "_id"}
            await self.tool_mcp_configurations.update_one(
                {"_id": storage_id},
                {"$set": set_fields},
            )
        else:
            storage_id = await self._upsert_by_tenant_and_name(
                self.tool_mcp_configurations,
                tenant_id=tenant_id,
                wire_name=wire_name,
                doc=config,
                doc_id=storage_id,
                actor_id=actor_id,
                existing_by_name=existing_by_name,
            )
        legacy_id = str(requested_id or "")
        if legacy_id and legacy_id != storage_id and await self.tool_configurations.find_one(
            {"_id": legacy_id, "source": "mcp_server"},
        ):
            await self.tool_configurations.delete_one({"_id": legacy_id})

    async def insert_mcp_tool_configuration(
        self, config: dict, actor_id: str | None = None
    ) -> dict:
        """Insert-only MCP tool: never ``$set`` an existing ``(tenant_id, name)`` row."""
        from pymongo.errors import DuplicateKeyError

        from config.tool_configuration_schema import tool_wire_name_from_doc
        from storage.tool_doc_storage import McpToolInsertConflict
        from tools.mcp_tool_ids import mcp_public_tool_id, mcp_rpc_name_from_doc

        config = dict(config)
        config.setdefault("source", "mcp_server")
        tenant_id = str(config.get("tenant_id") or "__root__").strip()
        rpc = str(config.get("rpc_name") or "").strip() or mcp_rpc_name_from_doc(config)
        if rpc:
            config["rpc_name"] = rpc
        wire_name = tool_wire_name_from_doc(config) or str(config.get("name") or "").strip()
        if not wire_name:
            raise ValueError("MCP tool configuration must have a wire name")
        config["name"] = wire_name
        config.pop("llm_function_name", None)

        claimed_id = str(config.get("_id") or "").strip()
        if not claimed_id:
            from config.configuration_resolution import mint_configuration_storage_id

            claimed_id = mint_configuration_storage_id()
        config["_id"] = claimed_id
        self._stamp_audit_fields(config, actor_id, is_new=True)

        server_id = str(config.get("mcp_server") or "").strip()
        public_id = mcp_public_tool_id(server_id, rpc) if server_id and rpc else ""

        try:
            result = await self.tool_mcp_configurations.update_one(
                {"tenant_id": tenant_id, "name": wire_name},
                {"$setOnInsert": config},
                upsert=True,
            )
        except DuplicateKeyError as exc:
            raise McpToolInsertConflict(
                "MCP tool identity already exists",
                reason="name_conflict",
                public_id=public_id,
                rpc_name=rpc,
            ) from exc

        if result.upserted_id is None:
            raise McpToolInsertConflict(
                f"MCP tool wire '{wire_name}' already exists",
                reason="id_conflict",
                public_id=public_id,
                rpc_name=rpc,
            )

        saved = await self.get_mcp_tool_configuration(claimed_id)
        if not saved:
            raise RuntimeError("insert_mcp_tool_configuration failed to persist document")
        if str(saved.get("tenant_id") or "").strip() != tenant_id:
            raise RuntimeError("insert_mcp_tool_configuration landed under unexpected tenant")
        return saved

    async def delete_mcp_tool_configuration(self, tool_id: str) -> bool:
        result = await self.tool_mcp_configurations.delete_one({"_id": tool_id})
        legacy = await self.tool_configurations.delete_one(
            {"_id": tool_id, "source": "mcp_server"},
        )
        return bool(result.deleted_count or legacy.deleted_count)

    async def count_mcp_tool_configurations(self) -> int:
        return await self.tool_mcp_configurations.count_documents({})

    async def migrate_mcp_tools_to_dedicated_collection(self) -> int:
        """Move ``source=mcp_server`` rows from tool_configurations to tool_mcp_configurations."""
        from tools.agent_allowed_tools import known_builtin_tool_ids

        cursor = self.tool_configurations.find({"source": "mcp_server"})
        docs = await cursor.to_list(length=10000)
        moved = 0
        skipped = 0
        for doc in docs:
            doc_id = doc.get("_id")
            if not doc_id:
                continue
            # The raw replace_one below bypasses save_mcp_tool_configuration's
            # builtin-shadow guard (AppFactory-268); don't launder a colliding
            # legacy row into the dispatchable collection.
            if str(doc.get("name") or "").strip() in known_builtin_tool_ids():
                logger.warning(
                    "[MCP_MIGRATE] skip builtin-colliding legacy row _id=%s name=%s",
                    doc_id,
                    doc.get("name"),
                )
                skipped += 1
                continue
            tenant_id = doc.get("tenant_id")
            mcp_server = doc.get("mcp_server")
            name = doc.get("name")
            if tenant_id is not None and mcp_server and name:
                conflict = await self.tool_mcp_configurations.find_one(
                    {
                        "tenant_id": tenant_id,
                        "mcp_server": mcp_server,
                        "name": name,
                        "_id": {"$ne": doc_id},
                    }
                )
                if conflict:
                    logger.warning(
                        "[MCP_MIGRATE] skip duplicate triple tenant_id=%s mcp_server=%s "
                        "name=%s legacy_id=%s kept_id=%s",
                        tenant_id,
                        mcp_server,
                        name,
                        doc_id,
                        conflict.get("_id"),
                    )
                    await self.tool_configurations.delete_one({"_id": doc_id})
                    skipped += 1
                    continue
            try:
                await self.tool_mcp_configurations.replace_one(
                    {"_id": doc_id},
                    doc,
                    upsert=True,
                )
            except DuplicateKeyError:
                logger.warning(
                    "[MCP_MIGRATE] skip DuplicateKeyError legacy_id=%s tenant_id=%s "
                    "mcp_server=%s name=%s",
                    doc_id,
                    tenant_id,
                    mcp_server,
                    name,
                )
                await self.tool_configurations.delete_one({"_id": doc_id})
                skipped += 1
                continue
            await self.tool_configurations.delete_one({"_id": doc_id})
            moved += 1
        if moved or skipped:
            logger.info(
                "[MCP_MIGRATE] moved=%d skipped=%d — MCP tools in tool_mcp_configurations",
                moved,
                skipped,
            )
        return moved

    # ==================== RUN CONFIGURATIONS ====================

    async def get_run_configurations(self, tenant_id: str | None = None) -> list:
        """Get run configurations.

        If tenant_id is provided, returns only that tenant's run configs.
        """
        if self.run_configurations is None:
            return []
        query: dict[str, Any] = {}
        if tenant_id is not None:
            query["tenant_id"] = tenant_id
        cursor = self.run_configurations.find(query)
        return await cursor.to_list(length=200)

    async def get_run_configuration(self, config_id: str) -> dict | None:
        """Get a single run configuration by _id."""
        if self.run_configurations is None:
            return None
        return await self.run_configurations.find_one({"_id": config_id})

    async def save_run_configuration(self, config: dict, actor_id: str | None = None) -> None:
        """Upsert a run configuration by _id."""
        if self.run_configurations is None:
            raise RuntimeError("Run configurations collection is not initialized")

        doc_id = config.get("_id")
        if not doc_id:
            raise ValueError("Run configuration must have an '_id' field")

        existing = await self.run_configurations.find_one({"_id": doc_id})
        is_new = existing is None
        self._stamp_audit_fields(config, actor_id, is_new)
        await self.run_configurations.replace_one({"_id": doc_id}, config, upsert=True)

    async def delete_run_configuration(self, config_id: str) -> bool:
        """Delete a run configuration by _id. Returns True if deleted."""
        if self.run_configurations is None:
            return False
        result = await self.run_configurations.delete_one({"_id": config_id})
        return result.deleted_count > 0

    # ==================== DELETE OPERATIONS ====================

    def _validate_tenant_delete_scope(self, tenant_id: str) -> str:
        """Validate tenant_id for tenant-scoped destructive operations."""
        normalized = (tenant_id or "").strip()
        if not normalized:
            raise ValueError("tenant_id must not be empty for tenant-scoped delete")
        if normalized in PROTECTED_TENANT_IDS:
            raise ValueError(f"Cannot delete data for protected tenant '{normalized}'")
        return normalized

    @staticmethod
    def _is_transaction_support_error(exc: Exception) -> bool:
        """Detect errors that specifically indicate unavailable transaction support."""
        transaction_support_error_codes = {20, 251, 263}
        transaction_support_markers = (
            "transaction numbers are only allowed on a replica set member or mongos",
            "transactions are not supported by this deployment",
            "transactions are not supported",
        )

        candidates = [exc, getattr(exc, "__cause__", None), getattr(exc, "__context__", None)]
        for candidate in candidates:
            if candidate is None:
                continue

            code = None
            if isinstance(candidate, OperationFailure):
                code = candidate.code
                if code is None and isinstance(candidate.details, dict):
                    details_code = candidate.details.get("code")
                    if isinstance(details_code, int):
                        code = details_code
            else:
                raw_code = getattr(candidate, "code", None)
                if isinstance(raw_code, int):
                    code = raw_code

            if isinstance(code, int) and code in transaction_support_error_codes:
                return True

            message = str(candidate).lower()
            if any(marker in message for marker in transaction_support_markers):
                return True

        return False

    @staticmethod
    def _empty_tenant_cascade_counters() -> dict[str, int]:
        return {
            "agents": 0,
            "workflows": 0,
            "run_configs": 0,
            "tools": 0,
            "mcp_tools": 0,
            "users": 0,
            "projects": 0,
            "runs": 0,
            "tasks": 0,
            "events": 0,
            "snapshots": 0,
            "container_logs": 0,
            "deployments": 0,
            "archive_refs": 0,
            "a2a_task_contracts": 0,
            "tenant_settings": 0,
            "tenants": 0,
            "user_attachments": 0,
            "tenant_artifacts": 0,
        }

    @staticmethod
    async def _collect_cursor_docs(cursor) -> list[dict]:
        return [doc async for doc in cursor]

    def _get_deployments_collection(self):
        if getattr(self, "db", None) is None:
            return None
        return getattr(self, "deployments_col", None) or self.db.deployments

    async def delete_projects_by_tenant(self, tenant_id: str, session=None) -> tuple[list[str], int]:
        """Delete all projects by tenant_id and return (project_ids, deleted_count)."""
        normalized_tenant_id = self._validate_tenant_delete_scope(tenant_id)
        project_docs = await self._collect_cursor_docs(
            self.projects.find(
                {"tenant_id": normalized_tenant_id},
                {"project_id": 1, "_id": 0},
                session=session,
            )
        )
        project_ids = [doc.get("project_id") for doc in project_docs if doc.get("project_id")]
        result = await self.projects.delete_many({"tenant_id": normalized_tenant_id}, session=session)
        return project_ids, int(result.deleted_count)

    async def delete_project_related_by_project_ids(
        self,
        project_ids: list[str],
        session=None,
    ) -> dict[str, int]:
        """Delete collections linked by project_id and return deleted counters."""
        counters = {
            "runs": 0,
            "tasks": 0,
            "events": 0,
            "snapshots": 0,
            "container_logs": 0,
            "deployments": 0,
            "archive_refs": 0,
            "user_attachments": 0,
            "a2a_task_state": 0,
        }
        if not project_ids:
            return counters

        project_scope = {"$in": project_ids}
        counters["runs"] = int((await self.runs.delete_many({"project_id": project_scope}, session=session)).deleted_count)
        counters["tasks"] = int((await self.tasks.delete_many({"project_id": project_scope}, session=session)).deleted_count)
        counters["events"] = int((await self.events.delete_many({"project_id": project_scope}, session=session)).deleted_count)
        counters["snapshots"] = int((await self.snapshots.delete_many({"project_id": project_scope}, session=session)).deleted_count)
        counters["container_logs"] = int(
            (await self.container_logs.delete_many({"project_id": project_scope}, session=session)).deleted_count
        )

        deployments_collection = self._get_deployments_collection()
        if deployments_collection is not None:
            counters["deployments"] = int(
                (await deployments_collection.delete_many({"project_id": project_scope}, session=session)).deleted_count
            )

        # Archive locators for oversized tool results. Deleting these removes the
        # only handle to the S3 blobs (object_key), so a deleted tenant/project
        # leaves nothing addressable in Mongo; the blob bytes are reclaimed
        # best-effort by the tenant route and, failing that, the bucket lifecycle.
        counters["archive_refs"] = int(
            (await self.archive_refs.delete_many({"project_id": project_scope}, session=session)).deleted_count
        )
        # User attachments: Mongo only here. S3 prefixes are deleted post-commit in
        # tenant_routes (S3 cannot join the Mongo transaction / compensating rollback).
        counters["user_attachments"] = int(
            (await self.user_attachments.delete_many({"project_id": project_scope}, session=session)).deleted_count
        )
        if counters["user_attachments"]:
            logger.info(
                "[ATTACH] cascade deleted count=%d project_ids=%d",
                counters["user_attachments"],
                len(project_ids),
            )

        counters["a2a_task_state"] = int(
            (await self.a2a_task_state.delete_many({"project_id": project_scope}, session=session)).deleted_count
        )

        return counters

    async def _execute_tenant_cascade_delete(self, normalized_tenant_id: str, session=None) -> dict[str, int]:
        """Execute cascade delete operations. Caller controls transaction/session boundaries."""
        tenant = await self.tenants.find_one({"_id": normalized_tenant_id}, session=session)
        if not tenant:
            raise TenantCascadeNotFoundError(f"Tenant '{normalized_tenant_id}' not found")

        deleted = self._empty_tenant_cascade_counters()
        deleted["agents"] = await self.delete_agent_configurations_by_tenant(
            normalized_tenant_id,
            session=session,
        )
        deleted["workflows"] = await self.delete_workflow_definitions_by_tenant(
            normalized_tenant_id,
            session=session,
        )
        deleted["run_configs"] = await self.delete_run_configurations_by_tenant(
            normalized_tenant_id,
            session=session,
        )
        deleted["tools"] = await self.delete_tool_configurations_by_tenant(
            normalized_tenant_id,
            session=session,
        )
        deleted["mcp_tools"] = await self.delete_mcp_tool_configurations_by_tenant(
            normalized_tenant_id,
            session=session,
        )
        deleted["users"] = await self.delete_users_by_tenant(
            normalized_tenant_id,
            session=session,
        )
        project_ids, deleted_projects = await self.delete_projects_by_tenant(
            normalized_tenant_id,
            session=session,
        )
        deleted["projects"] = deleted_projects
        project_related_counts = await self.delete_project_related_by_project_ids(
            project_ids=project_ids,
            session=session,
        )
        deleted.update(project_related_counts)
        deleted["tenant_artifacts"] = int(
            (await self.tenant_artifacts.delete_many({"tenant_id": normalized_tenant_id}, session=session)).deleted_count
        )
        if deleted["tenant_artifacts"]:
            logger.info(
                "[TENANT_ARTIFACT] cascade deleted count=%d tenant=%s",
                deleted["tenant_artifacts"],
                normalized_tenant_id,
            )
        deleted["a2a_task_contracts"] = (
            await self.delete_a2a_task_contracts_by_tenant(
                normalized_tenant_id,
                session=session,
            )
        )

        deleted_settings = await self.delete_tenant_settings(normalized_tenant_id, session=session)
        deleted["tenant_settings"] = 1 if deleted_settings else 0

        deleted_tenant = await self.delete_tenant(normalized_tenant_id, session=session)
        if not deleted_tenant:
            raise TenantCascadeConflictError(
                f"Tenant '{normalized_tenant_id}' could not be deleted due to concurrent modification",
            )
        deleted["tenants"] = 1
        return deleted

    async def _snapshot_tenant_cascade_documents(self, normalized_tenant_id: str) -> dict[str, list[dict]]:
        """Collect tenant-scoped documents for compensating rollback."""
        run_configs: list[dict] = []
        if self.run_configurations is not None:
            run_configs = await self._collect_cursor_docs(
                self.run_configurations.find({"tenant_id": normalized_tenant_id})
            )

        tenant_projects = await self._collect_cursor_docs(
            self.projects.find({"tenant_id": normalized_tenant_id})
        )
        project_ids = [doc.get("project_id") for doc in tenant_projects if doc.get("project_id")]
        project_scope = {"$in": project_ids} if project_ids else {"$in": []}

        tenant_doc = await self.tenants.find_one({"_id": normalized_tenant_id})
        tenant_settings = await self.tenant_settings.find({"_id": normalized_tenant_id}).to_list(length=1)
        deployments_collection = self._get_deployments_collection()
        deployments_docs: list[dict] = []
        if deployments_collection is not None and project_ids:
            deployments_docs = await self._collect_cursor_docs(
                deployments_collection.find({"project_id": project_scope})
            )

        return {
            "agents": await self._collect_cursor_docs(
                self.agent_configurations.find({"tenant_id": normalized_tenant_id})
            ),
            "workflows": await self._collect_cursor_docs(
                self.workflow_definitions.find({"tenant_id": normalized_tenant_id})
            ),
            "run_configs": run_configs,
            "tools": await self._collect_cursor_docs(
                self.tool_configurations.find({"tenant_id": normalized_tenant_id})
            ),
            "mcp_tools": await self._collect_cursor_docs(
                self.tool_mcp_configurations.find({"tenant_id": normalized_tenant_id})
            ),
            "users": await self._collect_cursor_docs(
                self.users.find({"tenant_id": normalized_tenant_id})
            ),
            "projects": tenant_projects,
            "runs": await self._collect_cursor_docs(self.runs.find({"project_id": project_scope})) if project_ids else [],
            "tasks": await self._collect_cursor_docs(self.tasks.find({"project_id": project_scope})) if project_ids else [],
            "events": await self._collect_cursor_docs(self.events.find({"project_id": project_scope})) if project_ids else [],
            "snapshots_docs": await self._collect_cursor_docs(self.snapshots.find({"project_id": project_scope})) if project_ids else [],
            "container_logs": await self._collect_cursor_docs(self.container_logs.find({"project_id": project_scope})) if project_ids else [],
            "archive_refs": await self._collect_cursor_docs(self.archive_refs.find({"project_id": project_scope})) if project_ids else [],
            "user_attachments": await self._collect_cursor_docs(self.user_attachments.find({"project_id": project_scope})) if project_ids else [],
            "tenant_artifacts": await self._collect_cursor_docs(
                self.tenant_artifacts.find({"tenant_id": normalized_tenant_id})
            ),
            "a2a_task_contracts": await self._collect_cursor_docs(
                self.a2a_task_contracts.find({"tenant_id": normalized_tenant_id})
            ),
            "deployments": deployments_docs,
            "tenant_settings": tenant_settings,
            "tenants": [tenant_doc] if tenant_doc else [],
        }

    async def _restore_documents(self, collection, docs: list[dict]) -> None:
        for doc in docs:
            doc_id = doc.get("_id")
            if doc_id is None:
                continue
            await collection.replace_one({"_id": doc_id}, doc, upsert=True)

    async def _restore_tenant_cascade_documents(self, snapshots: dict[str, list[dict]]) -> None:
        """Compensating rollback for non-transactional tenant cascade delete."""
        await self._restore_documents(self.tenants, snapshots.get("tenants", []))
        await self._restore_documents(self.tenant_settings, snapshots.get("tenant_settings", []))
        await self._restore_documents(self.projects, snapshots.get("projects", []))
        await self._restore_documents(self.runs, snapshots.get("runs", []))
        await self._restore_documents(self.tasks, snapshots.get("tasks", []))
        await self._restore_documents(self.events, snapshots.get("events", []))
        await self._restore_documents(self.snapshots, snapshots.get("snapshots_docs", []))
        await self._restore_documents(self.container_logs, snapshots.get("container_logs", []))
        await self._restore_documents(self.archive_refs, snapshots.get("archive_refs", []))
        await self._restore_documents(self.user_attachments, snapshots.get("user_attachments", []))
        await self._restore_documents(self.tenant_artifacts, snapshots.get("tenant_artifacts", []))
        await self._restore_documents(
            self.a2a_task_contracts,
            snapshots.get("a2a_task_contracts", []),
        )
        deployments_collection = self._get_deployments_collection()
        if deployments_collection is not None:
            await self._restore_documents(deployments_collection, snapshots.get("deployments", []))
        await self._restore_documents(self.users, snapshots.get("users", []))
        await self._restore_documents(self.tool_configurations, snapshots.get("tools", []))
        await self._restore_documents(self.tool_mcp_configurations, snapshots.get("mcp_tools", []))
        if self.run_configurations is not None:
            await self._restore_documents(self.run_configurations, snapshots.get("run_configs", []))
        await self._restore_documents(self.workflow_definitions, snapshots.get("workflows", []))
        await self._restore_documents(self.agent_configurations, snapshots.get("agents", []))

    async def _delete_tenant_cascade_with_compensation(self, normalized_tenant_id: str) -> dict[str, int]:
        """Fallback for standalone MongoDB: delete + compensating rollback on failure."""
        snapshots = await self._snapshot_tenant_cascade_documents(normalized_tenant_id)
        if not snapshots.get("tenants"):
            raise TenantCascadeNotFoundError(f"Tenant '{normalized_tenant_id}' not found")

        try:
            return await self._execute_tenant_cascade_delete(normalized_tenant_id, session=None)
        except Exception:
            await self._restore_tenant_cascade_documents(snapshots)
            raise

    async def delete_agent_configurations_by_tenant(self, tenant_id: str, session=None) -> int:
        """Delete all agent configurations by tenant_id. Returns deleted_count."""
        normalized_tenant_id = self._validate_tenant_delete_scope(tenant_id)
        result = await self.agent_configurations.delete_many(
            {"tenant_id": normalized_tenant_id},
            session=session,
        )
        return int(result.deleted_count)

    async def delete_workflow_definitions_by_tenant(self, tenant_id: str, session=None) -> int:
        """Delete all workflow definitions by tenant_id. Returns deleted_count."""
        normalized_tenant_id = self._validate_tenant_delete_scope(tenant_id)
        result = await self.workflow_definitions.delete_many(
            {"tenant_id": normalized_tenant_id},
            session=session,
        )
        return int(result.deleted_count)

    async def delete_run_configurations_by_tenant(self, tenant_id: str, session=None) -> int:
        """Delete all run configurations by tenant_id. Returns deleted_count."""
        normalized_tenant_id = self._validate_tenant_delete_scope(tenant_id)
        if self.run_configurations is None:
            return 0
        result = await self.run_configurations.delete_many(
            {"tenant_id": normalized_tenant_id},
            session=session,
        )
        return int(result.deleted_count)

    async def delete_tool_configurations_by_tenant(self, tenant_id: str, session=None) -> int:
        """Delete all builtin tool configurations by tenant_id. Returns deleted_count."""
        normalized_tenant_id = self._validate_tenant_delete_scope(tenant_id)
        result = await self.tool_configurations.delete_many(
            {
                "tenant_id": normalized_tenant_id,
                **self._BUILTIN_TOOL_SOURCE_FILTER,
            },
            session=session,
        )
        legacy_mcp = await self.tool_configurations.delete_many(
            {"tenant_id": normalized_tenant_id, "source": "mcp_server"},
            session=session,
        )
        return int(result.deleted_count) + int(legacy_mcp.deleted_count)

    async def delete_mcp_tool_configurations_by_tenant(self, tenant_id: str, session=None) -> int:
        """Delete tenant MCP tools and all dedicated wizard records."""
        normalized_tenant_id = self._validate_tenant_delete_scope(tenant_id)
        result = await self.tool_mcp_configurations.delete_many(
            {"tenant_id": normalized_tenant_id},
            session=session,
        )
        packages = await self.mcp_packages.delete_many(
            {"tenant_id": normalized_tenant_id},
            session=session,
        )
        images = await self.mcp_built_images.delete_many(
            {"tenant_id": normalized_tenant_id},
            session=session,
        )
        return int(result.deleted_count) + int(packages.deleted_count) + int(images.deleted_count)

    async def delete_users_by_tenant(self, tenant_id: str, session=None) -> int:
        """Delete all users by tenant_id. Returns deleted_count."""
        normalized_tenant_id = self._validate_tenant_delete_scope(tenant_id)
        result = await self.users.delete_many(
            {"tenant_id": normalized_tenant_id},
            session=session,
        )
        return int(result.deleted_count)

    async def delete_tenant_cascade_atomic(self, tenant_id: str) -> dict[str, int]:
        """Atomically delete tenant-scoped objects, tenant settings, and tenant document.

        Returns deleted counters on success.
        Uses Mongo transactions when available; falls back to compensating rollback
        mode on standalone deployments that do not support transactions.

        Raises:
            - ValueError for invalid/protected tenant ids.
            - TenantCascadeNotFoundError when tenant does not exist.
            - TenantCascadeConflictError on concurrent tenant deletion race.
            - TenantCascadeTransactionsRequiredError when storage is not initialized.
            - Other exceptions for unexpected failures.
        """
        normalized_tenant_id = self._validate_tenant_delete_scope(tenant_id)

        if not self.enable_transactions:
            logger.warning(
                "[TENANT_CASCADE] tenant_id=%s mode=compensating_rollback reason=transactions_disabled",
                normalized_tenant_id,
            )
            return await self._delete_tenant_cascade_with_compensation(normalized_tenant_id)
        if self.client is None:
            raise TenantCascadeTransactionsRequiredError(
                "MongoDB client is not initialized; cannot run atomic tenant cascade delete",
            )

        try:
            async with await self.client.start_session() as session:
                async with session.start_transaction():
                    return await self._execute_tenant_cascade_delete(normalized_tenant_id, session=session)
        except TenantCascadeNotFoundError:
            raise
        except TenantCascadeConflictError:
            raise
        except TenantCascadeTransactionsRequiredError:
            raise
        except Exception as exc:
            if self._is_transaction_support_error(exc):
                logger.warning(
                    "[TENANT_CASCADE] tenant_id=%s mode=compensating_rollback reason=transactions_unsupported",
                    normalized_tenant_id,
                )
                return await self._delete_tenant_cascade_with_compensation(normalized_tenant_id)
            raise

    async def delete_agent_configuration(self, agent_id: str) -> bool:
        """Delete an agent configuration by _id. Returns True if deleted."""
        result = await self.agent_configurations.delete_one({"_id": agent_id})
        return result.deleted_count > 0

    async def delete_workflow_definition(self, workflow_id: str) -> bool:
        """Delete a workflow definition by _id. Returns True if deleted."""
        result = await self.workflow_definitions.delete_one({"_id": workflow_id})
        return result.deleted_count > 0

    async def delete_tool_configuration(self, tool_id: str) -> bool:
        """Delete a tool configuration by _id. Returns True if deleted."""
        result = await self.tool_configurations.delete_one({"_id": tool_id})
        return result.deleted_count > 0

    # ==================== USERS (F5 AUTH) ====================

    async def get_user_by_email(self, email: str) -> dict | None:
        """Get a user by email address."""
        return await self.users.find_one({"email": email})

    async def get_user_by_id(self, user_id: str) -> dict | None:
        """Get a user by _id."""
        try:
            return await self.users.find_one({"_id": ObjectId(user_id)})
        except Exception:
            return await self.users.find_one({"_id": user_id})

    async def get_users(self, tenant_id: str | None = None) -> list:
        """List users, optionally filtered by tenant_id."""
        query = {"tenant_id": tenant_id} if tenant_id else {}
        cursor = self.users.find(query).sort("created_at", -1)
        return await cursor.to_list(length=500)

    async def get_user_names(self, user_ids: list[str]) -> dict[str, str | None]:
        """Ids without a user document are absent from the result."""
        cursor = self.users.find({"_id": {"$in": list(user_ids)}}, {"name": 1})
        users = await cursor.to_list(length=len(user_ids))
        return {str(user["_id"]): user.get("name") for user in users}

    async def save_user(self, user: dict, actor_id: str | None = None) -> None:
        """Upsert a user by _id."""
        doc_id = user.get("_id")
        if not doc_id:
            raise ValueError("User must have an '_id' field")
        existing = await self.users.find_one({"_id": doc_id})
        is_new = existing is None
        self._stamp_audit_fields(user, actor_id, is_new)
        await self.users.replace_one({"_id": doc_id}, user, upsert=True)

    async def delete_user(self, user_id: str) -> bool:
        """Delete a user by _id. Returns True if deleted."""
        result = await self.users.delete_one({"_id": user_id})
        return result.deleted_count > 0

    async def count_users(self) -> int:
        """Count documents in users collection."""
        return await self.users.count_documents({})

    # ==================== TENANTS (F5 MULTI-TENANCY) ====================

    async def get_tenant(self, tenant_id: str) -> dict | None:
        """Get a tenant by _id."""
        doc = await self.tenants.find_one({"_id": tenant_id})
        return self._serialize_datetimes(doc) if doc else None

    async def get_tenants(self) -> list:
        """List all tenants."""
        cursor = self.tenants.find({})
        docs = await cursor.to_list(length=100)
        return [self._serialize_datetimes(doc) for doc in docs]

    async def save_tenant(self, tenant: dict, actor_id: str | None = None) -> None:
        """Upsert a tenant by _id."""
        doc_id = tenant.get("_id")
        if not doc_id:
            raise ValueError("Tenant must have an '_id' field")
        existing = await self.tenants.find_one({"_id": doc_id})
        is_new = existing is None
        self._stamp_audit_fields(tenant, actor_id, is_new)
        await self.tenants.replace_one({"_id": doc_id}, tenant, upsert=True)

    async def delete_tenant(self, tenant_id: str, session=None) -> bool:
        """Delete a tenant by _id. Returns True if deleted."""
        result = await self.tenants.delete_one({"_id": tenant_id}, session=session)
        return result.deleted_count > 0

    async def count_tenants(self) -> int:
        """Count documents in tenants collection."""
        return await self.tenants.count_documents({})

    # ==================== TENANT SETTINGS (F5 MULTI-TENANCY) ====================

    async def get_tenant_settings(self, tenant_id: str) -> dict | None:
        """Get tenant settings by _id (= tenant_id)."""
        return await self.tenant_settings.find_one({"_id": tenant_id})

    async def find_tenant_settings_by_mcp_export_key_id(self, key_id: str) -> dict | None:
        """Find tenant_settings owning an MCP export API key by public key id."""
        if not key_id:
            return None
        return await self.tenant_settings.find_one({"mcp_export_api_keys.id": key_id})

    async def touch_tenant_mcp_export_api_key_last_used(
        self,
        tenant_id: str,
        key_id: str,
        last_used_at: str,
        actor_id: str | None = None,
    ) -> bool:
        """Atomically set last_used_at on one tenant export key."""
        if not tenant_id or not key_id:
            return False
        now_utc = datetime.now(timezone.utc)
        audit_set: dict = {"updated_at": now_utc}
        if actor_id:
            audit_set["updated_by"] = actor_id
        result = await self.tenant_settings.update_one(
            {"_id": tenant_id, "mcp_export_api_keys.id": key_id},
            {
                "$set": {
                    "mcp_export_api_keys.$[k].last_used_at": last_used_at,
                    **audit_set,
                }
            },
            array_filters=[{"k.id": key_id}],
        )
        return result.modified_count > 0

    async def pull_tenant_mcp_export_api_key(
        self,
        tenant_id: str,
        key_id: str,
        actor_id: str | None = None,
    ) -> bool:
        """Atomically remove one tenant export key by id."""
        if not tenant_id or not key_id:
            return False
        now_utc = datetime.now(timezone.utc)
        audit_set: dict = {"updated_at": now_utc}
        if actor_id:
            audit_set["updated_by"] = actor_id
        result = await self.tenant_settings.update_one(
            {"_id": tenant_id},
            {"$pull": {"mcp_export_api_keys": {"id": key_id}}, "$set": audit_set},
        )
        return result.modified_count > 0

    async def push_tenant_mcp_export_api_key(
        self,
        tenant_id: str,
        key_row: dict,
        actor_id: str | None = None,
    ) -> None:
        """Atomically append one export key to tenant_settings."""
        if not tenant_id or not isinstance(key_row, dict) or not key_row.get("id"):
            raise ValueError("tenant_id and key_row.id are required")
        now_utc = datetime.now(timezone.utc)
        audit_set: dict = {"updated_at": now_utc}
        if actor_id:
            audit_set["updated_by"] = actor_id
        await self.tenant_settings.update_one(
            {"_id": tenant_id},
            {"$push": {"mcp_export_api_keys": key_row}, "$set": audit_set},
            upsert=True,
        )

    async def save_tenant_settings(self, settings: dict, actor_id: str | None = None) -> None:
        """Upsert tenant settings by _id."""
        doc_id = settings.get("_id")
        if not doc_id:
            raise ValueError("Tenant settings must have an '_id' field")
        existing = await self.tenant_settings.find_one({"_id": doc_id})
        is_new = existing is None
        self._stamp_audit_fields(settings, actor_id, is_new)
        await self.tenant_settings.replace_one(
            {"_id": doc_id}, settings, upsert=True
        )

    async def delete_tenant_settings(self, tenant_id: str, session=None) -> bool:
        """Delete tenant settings by _id. Returns True if deleted."""
        result = await self.tenant_settings.delete_one({"_id": tenant_id}, session=session)
        return result.deleted_count > 0

    # ==================== EXTERNAL MCP RUNTIME SETTINGS ====================

    async def get_default_external_mcp_image(self) -> Optional[str]:
        """Return default image for local external MCP runtimes from system_info."""
        coll = self.db.system_info
        doc = await coll.find_one({"_id": "external_mcp_runtime"}) or {}
        image = doc.get("default_external_mcp_image")
        if isinstance(image, str) and image.strip():
            logger.info("[EXTERNAL_MCP] default image loaded from MongoDB: %s", image.strip())
            return image.strip()
        if image is not None:
            logger.warning("[EXTERNAL_MCP] invalid default_external_mcp_image type=%s", type(image).__name__)
        else:
            logger.info("[EXTERNAL_MCP] default image not found in MongoDB")
        return None

    async def get_external_mcp_server_port(self, tenant_id: str, server_id: str) -> Optional[int]:
        """Return persisted host port for one external MCP server."""
        coll = self.db.system_info
        doc = await coll.find_one({"_id": "external_mcp_ports"}) or {}
        ports = doc.get("ports")
        if isinstance(ports, list):
            for entry in ports:
                if not isinstance(entry, dict):
                    continue
                if entry.get("tenant_id") == tenant_id and entry.get("server_id") == server_id:
                    p = entry.get("port")
                    if isinstance(p, int) and 1 <= p <= 65535:
                        return p
            return None
        if isinstance(ports, dict):
            key = f"{tenant_id}:{server_id}"
            value = ports.get(key)
            if isinstance(value, int) and value > 0:
                return value
        return None

    async def save_external_mcp_server_port(self, tenant_id: str, server_id: str, port: int) -> None:
        """Persist host port mapping for one external MCP server.

        Stores ``ports`` as a list of subdocuments so tenant/server IDs may contain ``.``
        without Mongo dotted-path ambiguity. Legacy dict layout is migrated on write.
        """
        if not isinstance(port, int) or port < 1 or port > 65535:
            raise ValueError("external MCP port must be in range 1..65535")
        coll = self.db.system_info
        now = datetime.now(timezone.utc)
        doc = await coll.find_one({"_id": "external_mcp_ports"}) or {}
        raw = doc.get("ports")
        entries: List[Dict[str, Any]] = []
        if isinstance(raw, list):
            for e in raw:
                if isinstance(e, dict) and "tenant_id" in e and "server_id" in e:
                    entries.append(dict(e))
        elif isinstance(raw, dict):
            for k, v in raw.items():
                if not isinstance(k, str) or ":" not in k:
                    continue
                t, s = k.split(":", 1)
                if isinstance(v, int) and v > 0:
                    entries.append(
                        {"tenant_id": t, "server_id": s, "port": int(v), "updated_at": now}
                    )
        replaced = False
        for i, e in enumerate(entries):
            if e.get("tenant_id") == tenant_id and e.get("server_id") == server_id:
                entries[i] = {
                    "tenant_id": tenant_id,
                    "server_id": server_id,
                    "port": int(port),
                    "updated_at": now,
                }
                replaced = True
                break
        if not replaced:
            entries.append(
                {
                    "tenant_id": tenant_id,
                    "server_id": server_id,
                    "port": int(port),
                    "updated_at": now,
                }
            )
        await coll.update_one(
            {"_id": "external_mcp_ports"},
            {"$set": {"ports": entries, "updated_at": now}},
            upsert=True,
        )
        logger.info(
            "[EXTERNAL_MCP] persisted port tenant=%s server=%s port=%s (array layout)",
            tenant_id,
            server_id,
            port,
        )

    async def delete_external_mcp_server_port(self, tenant_id: str, server_id: str) -> None:
        """Remove persisted host port mapping for one external MCP server (no-op if missing)."""
        coll = self.db.system_info
        doc = await coll.find_one({"_id": "external_mcp_ports"}) or {}
        raw = doc.get("ports")
        if not isinstance(raw, list):
            return
        entries = [
            dict(e)
            for e in raw
            if isinstance(e, dict)
            and not (e.get("tenant_id") == tenant_id and e.get("server_id") == server_id)
        ]
        if len(entries) == len(raw):
            return
        await coll.update_one(
            {"_id": "external_mcp_ports"},
            {"$set": {"ports": entries, "updated_at": datetime.now(timezone.utc)}},
            upsert=True,
        )
        logger.info(
            "[EXTERNAL_MCP] removed persisted port tenant=%s server=%s",
            tenant_id,
            server_id,
        )

    # ==================== INTERNAL HELPERS ====================
    def _sanitize(self, value: Any) -> Any:
        """Recursively convert Mongo-only types to JSON-serializable values.
        - datetime -> ISO string
        - ObjectId -> str
        - dict/list -> recurse
        """
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, ObjectId):
            return str(value)
        if isinstance(value, dict):
            return {k: self._sanitize(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self._sanitize(v) for v in value]
        return value
    
    # ==================== PROJECTS ====================
    
    async def save_project(self, project_id: str, data: Dict[str, Any], expected_version: Optional[int] = None, actor_id: str | None = None):
        """Save or update project with optimistic concurrency control.
        
        If expected_version is provided, only update if current version matches.
        Returns (success: bool, version: int, conflict: bool)
        """

        existing = await self.projects.find_one({"project_id": project_id})

        doc = {
            "project_id": project_id,
            "user_prompt": data.get("user_prompt", ""),
            "title": data.get("title", "Untitled Project"),
            "status": data.get("status", "initialized"),
            "current_phase": data.get("current_phase", "requirements"),
            "approval_mode": data.get("approval_mode", "human"),
            "metadata": data.get("metadata", {}),
            "version": data.get("version", 1)
        }
        if "workflow_id" in data:
            doc["workflow_id"] = data.get("workflow_id")
        if "model_id" in data:
            doc["model_id"] = data.get("model_id")
        if "force_model" in data:
            doc["force_model"] = bool(data.get("force_model"))
        if "reasoning_effort" in data:
            doc["reasoning_effort"] = data.get("reasoning_effort")
        if "temperature" in data:
            doc["temperature"] = data.get("temperature")
        if "run_config_id" in data:
            doc["run_config_id"] = data.get("run_config_id")
        if "tenant_id" in data:
            doc["tenant_id"] = data.get("tenant_id")
        elif existing and existing.get("tenant_id") is not None:
            # Preserve tenant ownership across status/phase updates that omit tenant_id.
            doc["tenant_id"] = existing.get("tenant_id")

        # Doc is rebuilt from scratch; carry creation audit so _stamp_audit_fields
        # does not treat every update as "missing created_at → now" (AppFactory-317).
        if existing:
            for field in ("created_at", "created_by"):
                if existing.get(field) is not None and not doc.get(field):
                    doc[field] = existing[field]

        # The first save keeps the launch time the caller passes: the in-memory project took
        # it before its container was created, and memory and database must show one start time.
        records_creation = not (existing or {}).get("created_at")
        if records_creation and data.get("created_at"):
            doc["created_at"] = data["created_at"]
        self._stamp_audit_fields(doc, actor_id, is_new=records_creation)
        if is_finishing((existing or {}).get("status"), doc["status"]):
            doc["finished_at"] = doc["updated_at"]

        # Optimistic concurrency: if expected_version provided, check before updating
        if expected_version is not None:
            result = await self.projects.update_one(
                {"project_id": project_id, "version": expected_version},
                {"$set": {**doc, "version": expected_version + 1}},
                upsert=False
            )
            if result.matched_count == 0:
                # Version conflict: fetch current version for client retry
                current = await self.projects.find_one({"project_id": project_id})
                if inspect.isawaitable(current):
                    current = await current
                current_version = (current or {}).get("version", 1) if current else 1
                return False, current_version, True
            return True, expected_version + 1, False
        else:
            # Atomically increment version using $inc (no separate find needed)
            doc.pop("version", None)
            updated = await self.projects.find_one_and_update(
                {"project_id": project_id},
                {"$set": doc, "$inc": {"version": 1}},
                upsert=True,
                return_document=True
            )
            if inspect.isawaitable(updated):
                updated = await updated
            new_version = (updated or {}).get("version", 1)
            return True, new_version, False
    
    async def load_project(self, project_id: str) -> Optional[Dict]:
        """Load project by ID"""
        doc = await self.projects.find_one({"project_id": project_id})
        
        if not doc:
            return None
        
        # Convert to JSON-safe dict
        result = {
            "project_id": doc["project_id"],
            "user_prompt": doc.get("user_prompt", ""),
            "title": doc.get("title", "Untitled Project"),
            "status": doc.get("status", "initialized"),
            "current_phase": doc.get("current_phase"),
            "approval_mode": doc.get("approval_mode", "human"),
            "model_id": doc.get("model_id"),
            "force_model": bool(doc.get("force_model")),
            "reasoning_effort": doc.get("reasoning_effort"),
            "temperature": doc.get("temperature"),
            "workflow_id": doc.get("workflow_id"),
            "run_config_id": doc.get("run_config_id"),
            "tenant_id": doc.get("tenant_id"),
            "created_at": doc.get("created_at"),
            "created_by": doc.get("created_by"),
            "updated_at": doc.get("updated_at"),
            "finished_at": doc.get("finished_at"),
            "metadata": doc.get("metadata", {}),
            "version": doc.get("version", 1)
        }
        if doc.get("checkpoint_restore"):
            result["checkpoint_restore"] = doc["checkpoint_restore"]
        return self._sanitize(result)
    
    @staticmethod
    def _build_project_list_query(
        *,
        tenant_id: str | None = None,
        status: str | None = None,
        current_phase: str | None = None,
        q: str | None = None,
        workflow_id: str | None = None,
        run_config_id: str | None = None,
        created_from: datetime | None = None,
        created_to: datetime | None = None,
        updated_from: datetime | None = None,
        updated_to: datetime | None = None,
    ) -> dict[str, Any]:
        """Build the shared tenant-scoped filter for project list reads."""
        query: dict[str, Any] = {}
        if tenant_id:
            query["tenant_id"] = tenant_id
        if status:
            query["status"] = (
                {"$in": [None, PROJECT_LIST_UNKNOWN_FACET]}
                if status == PROJECT_LIST_UNKNOWN_FACET
                else status
            )
        if current_phase:
            query["current_phase"] = (
                {"$in": [None, PROJECT_LIST_UNKNOWN_FACET]}
                if current_phase == PROJECT_LIST_UNKNOWN_FACET
                else current_phase
            )
        if workflow_id:
            query["workflow_id"] = workflow_id
        if run_config_id:
            query["run_config_id"] = run_config_id

        if created_from or created_to:
            query["created_at"] = {}
            if created_from:
                query["created_at"]["$gte"] = created_from
            if created_to:
                query["created_at"]["$lte"] = created_to

        if updated_from or updated_to:
            query["updated_at"] = {}
            if updated_from:
                query["updated_at"]["$gte"] = updated_from
            if updated_to:
                query["updated_at"]["$lte"] = updated_to

        search = str(q or "").strip()
        if search:
            escaped_search = re.escape(search)
            query["$or"] = [
                {"title": {"$regex": escaped_search, "$options": "i"}},
                {"user_prompt": {"$regex": escaped_search, "$options": "i"}},
                {"project_id": {"$regex": escaped_search, "$options": "i"}},
            ]
        return query

    async def list_project_facets(
        self,
        **filters: Any,
    ) -> Dict[str, Any]:
        """Return project counters and created-at bounds without loading projects."""
        query = self._build_project_list_query(**filters)
        facet_dimensions = {"status", "current_phase", "created_at"}
        shared_query = {
            key: value for key, value in query.items() if key not in facet_dimensions
        }

        def branch_query(*excluded: str) -> dict[str, Any]:
            excluded_keys = set(excluded) | set(shared_query)
            return {
                key: value for key, value in query.items() if key not in excluded_keys
            }

        pipeline = [
            {"$match": shared_query},
            {
                "$facet": {
                    "summary": [
                        {"$match": branch_query()},
                        {"$count": "total"},
                    ],
                    "date_range": [
                        {"$match": branch_query("created_at")},
                        {
                            "$group": {
                                "_id": None,
                                "min_date": {"$min": "$created_at"},
                                "max_date": {"$max": "$created_at"},
                            }
                        }
                    ],
                    "by_status": [
                        {"$match": branch_query("status")},
                        {
                            "$group": {
                                "_id": {
                                    "$ifNull": ["$status", PROJECT_LIST_UNKNOWN_FACET]
                                },
                                "count": {"$sum": 1},
                            }
                        },
                    ],
                    "by_phase": [
                        {"$match": branch_query("current_phase")},
                        {
                            "$group": {
                                "_id": {
                                    "$ifNull": [
                                        "$current_phase",
                                        PROJECT_LIST_UNKNOWN_FACET,
                                    ]
                                },
                                "count": {"$sum": 1},
                            }
                        },
                    ],
                }
            },
        ]
        rows = await self.projects.aggregate(pipeline).to_list(length=1)
        facets = rows[0] if rows else {}
        summary_rows = facets.get("summary") or []
        summary = summary_rows[0] if summary_rows else {}
        date_range_rows = facets.get("date_range") or []
        date_range = date_range_rows[0] if date_range_rows else {}
        result = {
            "total": summary.get("total", 0),
            "by_status": {row["_id"]: row["count"] for row in facets.get("by_status", [])},
            "by_phase": {row["_id"]: row["count"] for row in facets.get("by_phase", [])},
            "date_range": {
                "min": date_range.get("min_date"),
                "max": date_range.get("max_date"),
            },
            "available_sort_fields": sorted(PROJECT_LIST_SORT_FIELDS),
        }
        return self._sanitize(result)

    async def list_projects(
        self,
        limit: int = PROJECT_LIST_DEFAULT_LIMIT,
        offset: int = 0,
        tenant_id: str | None = None,
        status: str | None = None,
        current_phase: str | None = None,
        q: str | None = None,
        workflow_id: str | None = None,
        run_config_id: str | None = None,
        created_from: datetime | None = None,
        created_to: datetime | None = None,
        updated_from: datetime | None = None,
        updated_to: datetime | None = None,
        sort_by: str = PROJECT_LIST_DEFAULT_SORT,
        sort_dir: str = "desc",
    ) -> Dict[str, Any]:
        """List projects with tenant scope, filters, sorting, and offset pagination."""

        safe_limit = max(1, min(int(limit or PROJECT_LIST_DEFAULT_LIMIT), PROJECT_LIST_MAX_LIMIT))
        safe_offset = max(0, int(offset or 0))
        safe_sort_by = sort_by if sort_by in PROJECT_LIST_SORT_FIELDS else PROJECT_LIST_DEFAULT_SORT
        safe_sort_dir = 1 if sort_dir == "asc" else -1

        query = self._build_project_list_query(
            tenant_id=tenant_id,
            status=status,
            current_phase=current_phase,
            q=q,
            workflow_id=workflow_id,
            run_config_id=run_config_id,
            created_from=created_from,
            created_to=created_to,
            updated_from=updated_from,
            updated_to=updated_to,
        )

        total = await self.projects.count_documents(query)
        cursor = (
            self.projects.find(query)
            .sort([(safe_sort_by, safe_sort_dir), ("project_id", safe_sort_dir)])
            .skip(safe_offset)
            .limit(safe_limit)
        )
        projects = await cursor.to_list(length=safe_limit)

        result: List[Dict[str, Any]] = []
        for doc in projects:
            item = {
                "project_id": doc.get("project_id"),
                "user_prompt": doc.get("user_prompt", ""),
                "title": doc.get("title", "Untitled Project"),
                "status": (
                    doc.get("status")
                    if doc.get("status") is not None
                    else PROJECT_LIST_UNKNOWN_FACET
                ),
                "current_phase": (
                    doc.get("current_phase")
                    if doc.get("current_phase") is not None
                    else PROJECT_LIST_UNKNOWN_FACET
                ),
                "approval_mode": doc.get("approval_mode", "human"),
                "model_id": doc.get("model_id"),
                "force_model": bool(doc.get("force_model")),
                "reasoning_effort": doc.get("reasoning_effort"),
                "temperature": doc.get("temperature"),
                "workflow_id": doc.get("workflow_id"),
                "run_config_id": doc.get("run_config_id"),
                "tenant_id": doc.get("tenant_id"),
                "created_at": doc.get("created_at"),
                "created_by": doc.get("created_by"),
                "updated_at": doc.get("updated_at"),
                "finished_at": doc.get("finished_at"),
                "metadata": doc.get("metadata", {}),
            }
            result.append(self._sanitize(item))

        next_offset = safe_offset + safe_limit
        return {
            "projects": result,
            "total": total,
            "limit": safe_limit,
            "offset": safe_offset,
            "next_offset": next_offset if next_offset < total else None,
        }

    # ==================== RUNS (NEW) ====================
    
    async def create_run(
        self,
        project_id: str,
        run_id: str,
        parent_run_id: Optional[str] = None,
        forked_from_conversation_index: Optional[int] = None
    ) -> Dict[str, Any]:
        """Create a new run for a project."""
        doc = {
            "run_id": run_id,
            "project_id": project_id,
            "parent_run_id": parent_run_id,
            "forked_from_conversation_index": forked_from_conversation_index,
            "run_status": "initialized",
            "workflow_phase": "requirements",
            "active": True,
            "created_at": datetime.utcnow(),
            "deleted_at": None,
        }
        doc["telemetry"], root_span = prepare_run_trace(project_id, run_id)
        try:
            await self.runs.insert_one(doc)
            if doc["telemetry"].get("status") != "degraded":
                remember_run_context(
                    self, project_id, run_id, doc["telemetry"]["traceparent"]
                )
            else:
                await report_trace_issue(self, project_id, run_id, "root_span_failed")
        finally:
            finish_run_trace(root_span, project_id=project_id, run_id=run_id)
        return self._sanitize(doc)
    
    async def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Get a run by ID."""
        doc = await self.runs.find_one({"run_id": run_id}, RUN_READ_PROJECTION)
        if not doc:
            return None
        return self._sanitize({
            "run_id": doc["run_id"],
            "project_id": doc["project_id"],
            "parent_run_id": doc.get("parent_run_id"),
            "restored_from": doc.get("restored_from"),
            "restore_state": doc.get("restore_state"),
            "forked_from_conversation_index": doc.get("forked_from_conversation_index"),
            "telemetry": doc.get("telemetry"),
            "run_status": doc.get("run_status"),
            "workflow_phase": doc.get("workflow_phase"),
            "resume_blocked_reason": doc.get("resume_blocked_reason"),
            "backend_boot_id": doc.get("backend_boot_id"),
            "active": doc.get("active", False),
            "created_at": doc.get("created_at"),
            "deleted_at": doc.get("deleted_at"),
        })
    
    async def list_runs(self, project_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """List all runs for a project (newest first)."""
        cursor = self.runs.find(
            {"project_id": project_id, "deleted_at": None}, RUN_READ_PROJECTION
        ).sort("created_at", -1).limit(limit)
        docs = await cursor.to_list(length=limit)
        return [
            self._sanitize({
                "run_id": doc["run_id"],
                "project_id": doc["project_id"],
                "parent_run_id": doc.get("parent_run_id"),
                "restored_from": doc.get("restored_from"),
                "restore_state": doc.get("restore_state"),
                "forked_from_conversation_index": doc.get("forked_from_conversation_index"),
                "telemetry": doc.get("telemetry"),
                "run_status": doc.get("run_status"),
                "workflow_phase": doc.get("workflow_phase"),
                "active": doc.get("active", False),
                "created_at": doc.get("created_at"),
            })
            for doc in docs
        ]

    async def list_live_runs_for_trace(self, project_id: str) -> List[Dict[str, Any]]:
        """Return every non-deleted run with only trace-safe summary fields."""
        cursor = self.runs.find(
            {"project_id": project_id, "deleted_at": None},
            {"_id": 0, "run_id": 1, "project_id": 1, "parent_run_id": 1,
             "run_status": 1, "workflow_phase": 1, "active": 1, "created_at": 1},
        ).sort([("created_at", 1), ("run_id", 1)])
        rows = await cursor.to_list(length=None)
        return [self._sanitize(row) for row in rows]

    async def list_run_scopes_for_trace(self, project_id: str) -> List[Dict[str, Any]]:
        """Return run IDs and deletion state for conservative legacy attribution."""
        cursor = self.runs.find(
            {"project_id": project_id},
            {"_id": 0, "run_id": 1, "deleted_at": 1},
        )
        rows = await cursor.to_list(length=None)
        return [self._sanitize(row) for row in rows]
    
    async def get_active_run(self, project_id: str) -> Optional[Dict[str, Any]]:
        """Get the currently active run for a project."""
        doc = await self.runs.find_one({
            "project_id": project_id,
            "active": True,
            "deleted_at": None
        })
        if not doc:
            return None
        return self._sanitize({
            "run_id": doc["run_id"],
            "project_id": doc["project_id"],
            "parent_run_id": doc.get("parent_run_id"),
            "restored_from": doc.get("restored_from"),
            "restore_state": doc.get("restore_state"),
            "forked_from_conversation_index": doc.get("forked_from_conversation_index"),
            "run_status": doc.get("run_status"),
            "workflow_phase": doc.get("workflow_phase"),
            "resume_blocked_reason": doc.get("resume_blocked_reason"),
            "backend_boot_id": doc.get("backend_boot_id"),
            "active": doc.get("active", False),
            "created_at": doc.get("created_at"),
        })
    
    async def set_active_run(self, project_id: str, run_id: str) -> None:
        """Set a run as active and deactivate others for the project.

        Atomic when transactions are available: the old deactivate-all →
        activate-one order left a window where the project had ZERO active
        runs, and a concurrent get_active_run() returned None — which now
        fails every project load loudly (SharedContext requires a run_id).
        """
        if self.enable_transactions and self.client is not None:
            try:
                async with await self.client.start_session() as session:
                    async with session.start_transaction():
                        await self._swap_active_run(project_id, run_id, session=session)
                return
            except Exception as exc:
                if not self._is_transaction_support_error(exc):
                    raise
                logger.warning(
                    "[SET_ACTIVE_RUN] project_id=%s mode=non_transactional reason=transactions_unsupported",
                    project_id,
                )
        await self._swap_active_run(project_id, run_id)

    async def _swap_active_run(self, project_id: str, run_id: str, session=None) -> None:
        # Activate first: without a transaction a concurrent reader mid-swap
        # then sees two active runs (both real, one about to win) instead of
        # zero. A crash mid-swap leaves both active — recoverable noise —
        # rather than a project no load will accept.
        result = await self.runs.update_one(
            {"run_id": run_id, "deleted_at": None},
            {"$set": {"active": True}},
            session=session,
        )
        if result.matched_count == 0:
            # Refuse BEFORE deactivating the others: proceeding would leave
            # the project with zero active runs — the exact state this
            # method exists to prevent.
            raise ValueError(f"Cannot activate run {run_id}: not found or deleted")
        await self.runs.update_many(
            {"project_id": project_id, "deleted_at": None, "run_id": {"$ne": run_id}},
            {"$set": {"active": False}},
            session=session,
        )
    
    async def update_run(self, run_id: str, updates: Dict[str, Any]) -> None:
        """Update run fields."""
        await self.runs.update_one(
            {"run_id": run_id},
            {"$set": updates}
        )
    
    async def update_run_status(self, run_id: str, status: str) -> None:
        """Update run status (running, paused, completed, failed)."""
        await self.runs.update_one(
            {"run_id": run_id},
            {"$set": {"run_status": status}}
        )

    async def stamp_run_backend_boot_id(self, run_id: str, boot_id: str) -> None:
        """Bind active run to the backend boot that started it (restart evidence)."""
        await self.runs.update_one(
            {"run_id": run_id},
            {"$set": {"backend_boot_id": boot_id}},
        )

    async def _apply_terminal_transition(
        self,
        project_id: str,
        run_id: str,
        status: str,
        *,
        clear_resume_blocked: bool = False,
        resume_blocked_reason: str | None = None,
        session=None,
    ) -> bool:
        # The status is part of the filter so that the finish time is written only
        # when this write ends the run, never over the finish of an earlier one.
        finish_result = await self.projects.update_one(
            {
                "project_id": project_id,
                "status": {"$nin": sorted(TERMINAL_PROJECT_STATUSES)},
            },
            {"$set": {"status": status, "finished_at": datetime.now(timezone.utc)}},
            session=session,
        )
        if finish_result.matched_count == 0:
            proj_result = await self.projects.update_one(
                {"project_id": project_id},
                {"$set": {"status": status}},
                session=session,
            )
            if proj_result.matched_count == 0:
                return False
        run_update: Dict[str, Any] = {"$set": {"run_status": status}}
        if clear_resume_blocked:
            run_update["$unset"] = {"resume_blocked_reason": ""}
        elif resume_blocked_reason:
            run_update["$set"]["resume_blocked_reason"] = resume_blocked_reason
        run_result = await self.runs.update_one(
            {"run_id": run_id},
            run_update,
            session=session,
        )
        return run_result.matched_count > 0

    async def transition_project_run_terminal(
        self,
        project_id: str,
        run_id: str,
        status: str,
        *,
        clear_resume_blocked: bool = False,
        resume_blocked_reason: str | None = None,
    ) -> bool:
        """Atomically persist terminal project + run status (AppFactory-291)."""
        if status not in TERMINAL_PROJECT_STATUSES:
            raise ValueError(f"Not a terminal status: {status!r}")

        if self.enable_transactions and self.client is not None:
            try:
                async with await self.client.start_session() as session:
                    async with session.start_transaction():
                        ok = await self._apply_terminal_transition(
                            project_id,
                            run_id,
                            status,
                            clear_resume_blocked=clear_resume_blocked,
                            resume_blocked_reason=resume_blocked_reason,
                            session=session,
                        )
                        if not ok:
                            raise ValueError(
                                f"terminal transition incomplete for project={project_id} run={run_id}"
                            )
                return True
            except ValueError:
                return False
            except Exception as exc:
                if not self._is_transaction_support_error(exc):
                    raise
                logger.warning(
                    "[TERMINAL_TRANSITION] project_id=%s mode=non_transactional "
                    "reason=transactions_unsupported",
                    project_id,
                )

        return await self._apply_terminal_transition(
            project_id,
            run_id,
            status,
            clear_resume_blocked=clear_resume_blocked,
            resume_blocked_reason=resume_blocked_reason,
        )

    async def set_run_resume_blocked(self, run_id: str, reason: str) -> None:
        """Durable marker: rehydrate must not resume after failed terminal persist."""
        await self.runs.update_one(
            {"run_id": run_id},
            {"$set": {"resume_blocked_reason": reason}},
        )

    async def clear_run_resume_blocked(self, run_id: str) -> None:
        await self.runs.update_one(
            {"run_id": run_id},
            {"$unset": {"resume_blocked_reason": ""}},
        )
    
    async def update_run_phase(self, run_id: str, phase: str) -> None:
        """Update run workflow phase (requirements, planning, execution, completed)."""
        await self.runs.update_one(
            {"run_id": run_id},
            {"$set": {"workflow_phase": phase}}
        )

    async def claim_task_attempt(
        self,
        run_id: str,
        task_id: str,
        agent_id: str,
    ) -> Optional[int]:
        """Atomically allocate the next lifecycle attempt for one task/agent pair.

        Counters deliberately live on the existing run document.  The map key
        is a digest rather than a caller-controlled field path, so arbitrary
        task and agent identifiers cannot inject MongoDB path syntax.  `$inc`
        is one document-level atomic operation, so concurrent claimers cannot
        receive the same ordinal.
        """
        if not run_id or not task_id or not agent_id:
            return None

        counter_key = hashlib.sha256(
            f"{task_id}\0{agent_id}".encode("utf-8")
        ).hexdigest()
        counter_path = f"trace_attempt_counters.{counter_key}"
        updated = await self.runs.find_one_and_update(
            {"run_id": run_id, "deleted_at": None},
            {"$inc": {counter_path: 1}},
            projection={"_id": 0, counter_path: 1},
            return_document=ReturnDocument.AFTER,
        )
        if not updated:
            return None

        value = (updated.get("trace_attempt_counters") or {}).get(counter_key)
        if isinstance(value, int) and value > 0:
            return value

        logger.error(
            "[ATTEMPT] run_id=%s task_id=%s agent_id=%s status=invalid_counter",
            run_id,
            task_id,
            agent_id,
        )
        return None

    async def add_run_completed_attempt(self, run_id: str, task_id: str) -> None:
        """Durably mark a phase attempt as completed on its run document.

        A gated phase suppresses the assistant-final that normally marks an
        attempt done, and the gate's approval row is only written AFTER an
        await window (build_approval_data). A restart in that window would
        otherwise make the FINISHED attempt look interrupted and re-run it
        (double side effects). This is the third completion marker restart
        recovery consults — see find_interrupted_attempt.
        """
        if not run_id or not task_id:
            return
        await self.runs.update_one(
            {"run_id": run_id},
            {"$addToSet": {"completed_attempts": task_id}},
        )

    @staticmethod
    def _map_progress_path(node_id: str) -> str:
        # Node ids come from workflow authors; one with '.' or '$' would
        # otherwise address a different field of the run document.
        if re.fullmatch(r"[A-Za-z0-9_-]+", node_id or ""):
            return f"map_progress.{node_id}"
        return f"map_progress.{hashlib.sha256(node_id.encode('utf-8')).hexdigest()}"

    async def save_map_progress(
        self, run_id: str, node_id: str, index: int, record: Dict[str, Any]
    ) -> None:
        path = f"{self._map_progress_path(node_id)}.done.{int(index)}"
        await self.runs.update_one({"run_id": run_id}, {"$set": {path: record}})

    async def load_map_progress(self, run_id: str, node_id: str) -> Dict[int, Dict[str, Any]]:
        path = self._map_progress_path(node_id)
        doc = await self.runs.find_one({"run_id": run_id}, {"_id": 0, path: 1})
        node_key = path.split(".", 1)[1]
        progress = ((doc or {}).get("map_progress") or {}).get(node_key) or {}
        return {int(index): record for index, record in (progress.get("done") or {}).items()}

    async def clear_map_progress(self, run_id: str, node_id: str) -> None:
        path = self._map_progress_path(node_id)
        await self.runs.update_one({"run_id": run_id}, {"$unset": {path: ""}})

    async def delete_run(self, run_id: str) -> None:
        """Soft-delete a run (Milestone 5: soft-delete only, mark deleted_at)."""
        await self.runs.update_one(
            {"run_id": run_id},
            {"$set": {"deleted_at": datetime.utcnow()}}
        )
        
        # Soft-delete associated data for this run
        # Events
        await self.events.update_many(
            {"run_id": run_id},
            {"$set": {"deleted_at": datetime.utcnow()}}
        )
        
        # Snapshots
        await self.snapshots.update_many(
            {"run_id": run_id},
            {"$set": {"deleted_at": datetime.utcnow()}}
        )

    # ==================== CONTAINER LOGS (NEW) ====================
    async def save_container_log(self, project_id: str, kind: str, payload: Dict[str, Any], actor_id: str | None = None):
        """Persist a container-related event/log for a project."""
        doc = {
            "project_id": project_id,
            "kind": kind,
            "payload": payload or {},
        }

        existing = await self.container_logs.find_one({"project_id": project_id})
        is_new = existing is None
        self._stamp_audit_fields(doc, actor_id, is_new)
        await self.container_logs.insert_one(doc)

    async def get_container_logs(self, project_id: str, limit: int = 200) -> List[Dict]:
        """Fetch recent container logs for a project (newest first)."""
        cursor = self.container_logs.find({
            "project_id": project_id
        }).sort("created_at", -1).limit(limit)
        rows = await cursor.to_list(length=limit)
        return [
            {
                "id": str(doc.get("_id")),
                "kind": doc.get("kind"),
                "payload": self._sanitize(doc.get("payload", {})),
                "created_at": doc.get("created_at").isoformat() if isinstance(doc.get("created_at"), datetime) else doc.get("created_at"),
            }
            for doc in rows
        ]
    
    async def find_running_projects(self) -> List[Dict]:
        """Find projects that are currently running (for recovery)"""
        cursor = self.projects.find({
            "status": {"$in": ["started", "initialized", "running"]}
        })
        projects = await cursor.to_list(length=100)
        
        # Sanitize for JSON encoding (remove _id, convert datetimes)
        result: List[Dict[str, Any]] = []
        for doc in projects:
            item = {
                "project_id": doc.get("project_id"),
                "user_prompt": doc.get("user_prompt", ""),
                "title": doc.get("title", "Untitled Project"),
                "status": doc.get("status", "initialized"),
                "current_phase": doc.get("current_phase"),
                "approval_mode": doc.get("approval_mode", "human"),
                "model_id": doc.get("model_id"),
                "force_model": bool(doc.get("force_model")),
                "reasoning_effort": doc.get("reasoning_effort"),
                "temperature": doc.get("temperature"),
                "workflow_id": doc.get("workflow_id"),
                "run_config_id": doc.get("run_config_id"),
                "created_at": doc.get("created_at"),
                "updated_at": doc.get("updated_at"),
                "metadata": doc.get("metadata", {}),
                "tenant_id": doc.get("tenant_id"),
            }
            if isinstance(item.get("created_at"), datetime):
                item["created_at"] = item["created_at"].isoformat()
            if isinstance(item.get("updated_at"), datetime):
                item["updated_at"] = item["updated_at"].isoformat()
            result.append(item)
        
        return result
    
    # ==================== SNAPSHOTS (NEW) ====================
    async def save_snapshot(self, project_id: str, snapshot: Dict[str, Any], actor_id: str | None = None) -> str:
        """Persist a snapshot document and return its id."""
        doc = {
            "project_id": project_id,
            "type": snapshot.get("type"),
            "label": snapshot.get("label"),
            "phase": snapshot.get("phase"),
            "event_id": snapshot.get("event_id"),
            "run_id": snapshot.get("run_id"),
            "git_commit": snapshot.get("git_commit"),
            "context_state": snapshot.get("context_state", {}),
            "meta": snapshot.get("meta", {}),
        }

        existing = await self.snapshots.find_one({"_id": project_id})
        is_new = existing is None
        self._stamp_audit_fields(doc, actor_id, is_new)
        res = await self.snapshots.insert_one(doc)

        return str(res.inserted_id)

    async def get_snapshot(self, snapshot_id: str) -> Optional[Dict[str, Any]]:
        try:
            oid = ObjectId(snapshot_id)
        except Exception:
            return None
        doc = await self.snapshots.find_one({"_id": oid})
        if not doc:
            return None
        out = {
            "id": str(doc.get("_id")),
            "project_id": doc.get("project_id"),
            "type": doc.get("type"),
            "label": doc.get("label"),
            "phase": doc.get("phase"),
            "event_id": doc.get("event_id"),
            "run_id": doc.get("run_id"),
            "created_at": doc.get("created_at").isoformat() if isinstance(doc.get("created_at"), datetime) else doc.get("created_at"),
            "git_commit": doc.get("git_commit"),
            "context_state": self._sanitize(doc.get("context_state", {})),
            "meta": self._sanitize(doc.get("meta", {})),
        }
        return out

    async def list_snapshots(self, project_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        cursor = self.snapshots.find({"project_id": project_id}).sort("created_at", -1).limit(limit)
        rows = await cursor.to_list(length=limit)
        out: List[Dict[str, Any]] = []
        for doc in rows:
            out.append({
                "id": str(doc.get("_id")),
                "project_id": doc.get("project_id"),
                "type": doc.get("type"),
                "label": doc.get("label"),
                "phase": doc.get("phase"),
                "event_id": doc.get("event_id"),
                "run_id": doc.get("run_id"),
                "created_at": doc.get("created_at").isoformat() if isinstance(doc.get("created_at"), datetime) else doc.get("created_at"),
                "git_commit": doc.get("git_commit"),
                "meta": self._sanitize(doc.get("meta", {})),
            })
        return out

    async def list_trace_snapshots(
        self,
        project_id: str,
        *,
        run_id: Optional[str] = None,
        snapshot_ids: Optional[List[str]] = None,
        limit: int = 1000,
        include_unscoped: bool = False,
    ) -> Dict[str, Any]:
        query: Dict[str, Any] = {"project_id": project_id}
        if run_id is not None:
            legacy_ids = [
                ObjectId(snapshot_id)
                for snapshot_id in snapshot_ids or []
                if ObjectId.is_valid(snapshot_id)
            ]
            query["$or"] = [{"run_id": run_id}]
            if include_unscoped:
                query["$or"].extend([
                    {"run_id": None},
                    {"run_id": {"$exists": False}},
                ])
            if legacy_ids:
                query["$or"].append({"_id": {"$in": legacy_ids}})
        cursor = self.snapshots.find(
            query,
            {"_id": 1, "project_id": 1, "run_id": 1, "type": 1, "label": 1, "phase": 1,
             "event_id": 1, "created_at": 1, "meta": 1},
        ).sort([("created_at", -1), ("_id", -1)]).limit(limit + 1)
        docs = await cursor.to_list(length=limit + 1)
        capped = len(docs) > limit
        docs = docs[:limit]
        rows = [{"id": str(d.get("_id")), "project_id": d.get("project_id"), "run_id": d.get("run_id"),
                 "type": d.get("type"), "label": d.get("label"), "phase": d.get("phase"),
                 "event_id": d.get("event_id"), "created_at": d.get("created_at"),
                 "meta": self._sanitize(d.get("meta", {}))} for d in docs]
        rows.reverse()
        return {"items": rows, "limit_reached": capped}

    async def get_latest_snapshot_by_types(self, project_id: str, types: List[str]) -> Optional[Dict[str, Any]]:
        cursor = self.snapshots.find({"project_id": project_id, "type": {"$in": types}}).sort("created_at", -1).limit(1)
        rows = await cursor.to_list(length=1)
        if not rows:
            return None
        return await self.get_snapshot(str(rows[0]["_id"]))

    # ==================== CONTEXT (stored in projects.context field) ====================
    
    async def save_context(self, project_id: str, context: Dict[str, Any], run_id: Optional[str] = None, actor_id: str | None = None):
        """Save shared context in projects collection (migration: contexts collection removed).
        
        Context is now stored in projects.context field instead of separate contexts collection.
        """
        context_doc = {
            "user_prompt": context.get("user_prompt", ""),
            "requirements": context.get("requirements", {}),
            "plan": context.get("plan", {}),
            "decisions": context.get("decisions", []),
            "insights": context.get("insights", []),
            "custom_context": context.get("custom_context", {}),
            "workflow_approval": context.get("workflow_approval"),
            "deploy_status": context.get("deploy_status", "not_started"),
            "deploy_error": context.get("deploy_error"),
            "deployments": context.get("deployments", []),
        }
        # DEPRECATED compatibility only. New file artifacts are stored in file_artifacts.
        if "artifacts" in context:
            context_doc["artifacts"] = context.get("artifacts", [])
        # Note: conversation_history removed - use messages collection instead

        project = await self.projects.find_one({"project_id": project_id})
        is_new = project.get("context") if project else None
        self._stamp_audit_fields(context_doc, actor_id, is_new)

        if run_id:
            context_doc["run_id"] = run_id
        
        await self.projects.update_one(
            {"project_id": project_id},
            {"$set": {"context": context_doc}},
            upsert=True
        )
    
    async def load_context(self, project_id: str, run_id: Optional[str] = None) -> Optional[Dict]:
        """Load shared context from projects collection.
        
        Context is now stored in projects.context field instead of separate contexts collection.
        """
        doc = await self.projects.find_one(
            {"project_id": project_id},
            {"context": 1, "_id": 0}
        )
        
        if not doc or not doc.get("context"):
            return None
        
        ctx = doc["context"]
        ctx.pop("updated_at", None)
        return ctx
    
    async def update_context_field(
        self,
        project_id: str,
        field: str,
        value: Any,
        run_id: Optional[str] = None
    ):
        """Update a specific field in context (incremental)"""
        await self.projects.update_one(
            {"project_id": project_id},
            {"$set": {f"context.{field}": value, "context.updated_at": datetime.utcnow()}},
            upsert=True
        )
    
    async def append_to_context_array(
        self,
        project_id: str,
        field: str,
        item: Any,
        run_id: Optional[str] = None
    ):
        """Append item to array field in context (atomic)"""
        await self.projects.update_one(
            {"project_id": project_id},
            {
                "$push": {f"context.{field}": item},
                "$set": {"context.updated_at": datetime.utcnow()}
            },
            upsert=True
        )
    
    # ==================== TASKS ====================
    
    async def save_task(self, task_id: str, project_id: str, task_data: Dict, actor_id: str | None = None):
        """Save task"""
        doc = {
            "task_id": task_id,
            "project_id": project_id,
            "task_type": task_data.get("type", ""),
            "description": task_data.get("description", ""),
            "assigned_agent": task_data.get("assigned_agent", ""),
            "status": task_data.get("status", "pending"),
            "result": task_data.get("result", {}),
            "completed_at": task_data.get("completed_at")
        }

        existing = await self.tasks.find_one({"task_id": task_id})
        is_new = existing is None
        self._stamp_audit_fields(doc, actor_id, is_new)

        if isinstance(doc.get("completed_at"), str):
            doc["completed_at"] = datetime.fromisoformat(doc["completed_at"])
        
        await self.tasks.update_one(
            {"task_id": task_id},
            {"$set": doc},
            upsert=True
        )
    
    # ==================== EVENTS ====================
    
    async def save_event(
        self,
        project_id: str,
        event_type: str,
        data: Dict[str, Any],
        event_id: Optional[str] = None,
        run_id: Optional[str] = None,
        actor_id: str | None = None,
    ):
        """Save event to history (with 30-day TTL).

        event_id (UUID v7) is the canonical resume pointer. The emitter mints
        it at emission time and passes it here so both Mongo persistence and
        the live SSE queue carry the same ID. run_id is passed explicitly
        rather than fished out of `data` so that lifecycle events (which
        legitimately have no run) are unambiguous.
        """
        doc = {
            "event_id": event_id,
            "project_id": project_id,
            "event_type": event_type,
            "data": data,
            "timestamp": datetime.utcnow(),
            "run_id": run_id if run_id is not None else data.get("run_id"),
        }

        is_new = True
        self._stamp_audit_fields(doc, actor_id, is_new)

        await self.events.insert_one(doc)

    async def get_events(
        self,
        project_id: str,
        since: Optional[datetime] = None,
        after_event_id: Optional[str] = None,
        event_type: Optional[str] = None,
        run_id: Optional[str] = None,
        limit: int = 1000,
    ) -> List[Dict]:
        """Get event history for a project.

        Resume modes (mutually exclusive — prefer after_event_id):
          * after_event_id: precise resume via UUID v7 lex order. Used by the
            SSE replay path when the client sends Last-Event-ID.
          * since: timestamp-based, used on first connect when no Last-Event-ID
            has been established yet.
        """
        query = {"project_id": project_id}

        if after_event_id:
            query["event_id"] = {"$gt": after_event_id}
        elif since:
            query["timestamp"] = {"$gt": since}

        if event_type:
            query["event_type"] = event_type

        if run_id:
            # Also return project-level events with no run (null or missing
            # field, treated alike): revert/snapshot/post-run-chat routing are
            # stamped run_id=null and belong to every run's view. Same rule the
            # message read and the FE live filter already apply.
            query["$or"] = [
                {"run_id": run_id},
                {"run_id": None},
                {"run_id": {"$exists": False}},
            ]

        # Sort by event_id (UUID v7) ascending — matches wire chronology and
        # the order live events arrive in. Fall back to timestamp sort only if
        # legacy docs lack event_id; the backfill migration eliminates that.
        cursor = self.events.find(query).sort("event_id", 1).limit(limit)
        events = await cursor.to_list(length=limit)

        # Convert to expected format
        result = []
        for doc in events:
            result.append({
                "event_id": doc.get("event_id"),
                "type": doc["event_type"],
                "data": self._sanitize(doc.get("data", {})),
                "timestamp": doc["timestamp"].isoformat() if isinstance(doc.get("timestamp"), datetime) else doc.get("timestamp"),
                "run_id": doc.get("run_id"),
            })

        return result

    async def list_trace_events(
        self,
        project_id: str,
        run_id: Optional[str] = None,
        limit: int = 5000,
        include_unscoped: bool = False,
    ) -> Dict[str, Any]:
        query = {"project_id": project_id}
        if run_id and include_unscoped:
            query["$or"] = [
                {"run_id": run_id},
                {"run_id": None, "data.run_id": run_id},
                {"run_id": None, "data.run_id": None},
                {"run_id": {"$exists": False}, "data.run_id": run_id},
                {"run_id": {"$exists": False}, "data.run_id": None},
            ]
        elif run_id:
            query["run_id"] = run_id
        projection = {"_id": 0, "event_id": 1, "event_type": 1, "timestamp": 1, "run_id": 1,
                      "data.run_id": 1, "data.task_id": 1, "data.agent_id": 1,
                      "data.agent_display_name": 1,
                      "data.workflow_node_id": 1, "data.node_id": 1, "data.phase": 1,
                      "data.selected_agent": 1, "data.agent": 1,
                      "data.workflow_phase": 1,
                      "data.task_description": 1, "data.description": 1, "data.attempt": 1,
                      "data.selection_mode": 1,
                      "data.auction_id": 1, "data.winner_id": 1, "data.winner_agent_id": 1,
                      "data.selected_agent_id": 1, "data.bids": 1, "data.fit_score": 1,
                      "data.reasoning": 1, "data.reason": 1, "data.critic_validation": 1,
                      "data.error": 1, "data.agent_count": 1,
                      "data.target_sequence": 1, "data.snapshot_id": 1, "data.event_id": 1,
                      "data.parent_agent": 1, "data.child_agent": 1, "data.child_agent_name": 1,
                      "data.parent_task_id": 1,
                      "data.parent_attempt_id": 1, "data.target_kind": 1,
                      "data.server_id": 1, "data.agent_name": 1, "data.error_type": 1,
                      "data.status": 1, "data.approval_id": 1, "data.approval_status": 1,
                      "data.artifacts_count": 1, "data.final_state": 1,
                      "data.progress.schema_version": 1, "data.progress.event_id": 1,
                      "data.progress.run_id": 1, "data.progress.sequence": 1,
                      "data.progress.type": 1, "data.progress.agent": 1,
                      "data.progress.tool_name": 1}
        docs = await self.events.find(query, projection).sort([("timestamp", -1), ("event_id", -1)]).limit(limit + 1).to_list(length=limit + 1)
        capped = len(docs) > limit
        docs = docs[:limit]
        rows = [{"event_id": d.get("event_id"), "event_type": d.get("event_type"),
                 "timestamp": d.get("timestamp"), "run_id": d.get("run_id"), "data": d.get("data", {})} for d in docs]
        rows.reverse()
        return {"items": [self._sanitize(r) for r in rows], "limit_reached": capped}

    async def list_trace_a2a_state(
        self,
        project_id: str,
        run_id: Optional[str] = None,
        limit: int = 5000,
        include_unscoped: bool = False,
    ) -> Dict[str, Any]:
        query = {"project_id": project_id}
        if run_id and include_unscoped:
            query["$or"] = [
                {"run_id": run_id},
                {"run_id": None},
                {"run_id": {"$exists": False}},
            ]
        elif run_id:
            query["run_id"] = run_id
        cursor = self.a2a_task_state.find(query, {"_id": 0, "project_id": 1, "run_id": 1, "node_id": 1, "server_id": 1, "task_id": 1, "status": 1, "final_status": 1, "started_at": 1, "completed_at": 1, "closed_at": 1}).sort([("started_at", -1), ("node_id", -1)]).limit(limit + 1)
        rows = await cursor.to_list(length=limit + 1)
        capped = len(rows) > limit
        rows = rows[:limit]
        rows.reverse()
        return {"items": [self._sanitize(r) for r in rows], "limit_reached": capped}

    async def delete_events_since(
        self,
        project_id: str,
        since: datetime,
    ) -> None:
        """Delete all events for a project strictly after the given timestamp."""
        query = {"project_id": project_id, "timestamp": {"$gt": since}}
        await self.events.delete_many(query)

    async def get_user_snapshot_by_conversation_index(
        self,
        project_id: str,
        conversation_index: int,
    ) -> Optional[Dict[str, Any]]:
        """Get a user_message snapshot by its conversation_index, or None if not found."""
        cursor = self.snapshots.find(
            {
                "project_id": project_id,
                "type": "user_message",
                "meta.conversation_index": conversation_index,
            }
        ).sort("created_at", -1).limit(1)
        rows = await cursor.to_list(length=1)
        if not rows:
            return None
        return await self.get_snapshot(str(rows[0]["_id"]))

    async def get_latest_user_snapshot(self, project_id: str) -> Optional[Dict[str, Any]]:
        """Get the most recent user_message snapshot for a project, or None if none exist."""
        return await self.get_latest_snapshot_by_types(project_id, ["user_message"])
    
    # ==================== DEPLOYMENTS (NEW - Section 2.3) ====================
    
    async def save_deployment(
        self,
        project_id: str,
        deployment_id: str,
        data: Dict[str, Any],
        actor_id: str | None = None
    ) -> None:
        """
        Save or update a deployment record.
        
        Schema:
        {
            "project_id": "uuid",
            "deployment_id": "uuid",
            "slug": "my-app",
            "namespace": "AppFactory-apps",
            "image_ref": "registry.../AppFactory/my-app:v1",
            "status": "running" | "failed" | "stopped" | "deleted",
            "url": "https://my-app.AppFactory.dev",
            "last_health_check": datetime,
            "created_at": datetime,
            "updated_at": datetime,
        }
        """
        if not hasattr(self, 'deployments_col') or self.deployments_col is None:
            self.deployments_col = self.db.deployments
            # Create indexes on first use
            await self.deployments_col.create_index("deployment_id", unique=True)
            await self.deployments_col.create_index([("project_id", 1), ("status", 1)])
            await self.deployments_col.create_index([("project_id", 1), ("created_at", -1)])

        
        doc = {
            "project_id": project_id,
            "deployment_id": deployment_id,
            "slug": data.get("slug"),
            "namespace": data.get("namespace"),
            "image_ref": data.get("image_ref"),
            "status": data.get("status", "pending"),
            "url": data.get("url"),
            "deploy_type": data.get("deploy_type", "local"),
            "last_health_check": data.get("last_health_check"),
            "error": data.get("error"),
            "metadata": data.get("metadata", {}),
        }
        
        # Upsert: create or update
        existing = await self.deployments_col.find_one({"deployment_id": deployment_id})
        is_new = existing is None
        if existing:
            self._stamp_audit_fields(doc, actor_id, is_new)
            await self.deployments_col.update_one(
                {"deployment_id": deployment_id},
                {"$set": doc}
            )
        else:
            self._stamp_audit_fields(doc, actor_id, is_new)
            await self.deployments_col.insert_one(doc)
        
        logger.debug(f"Saved deployment: {deployment_id} for project {project_id}")
    
    async def get_deployment(self, deployment_id: str) -> Optional[Dict[str, Any]]:
        """Get a deployment by ID."""
        if not hasattr(self, 'deployments_col') or self.deployments_col is None:
            self.deployments_col = self.db.deployments
        
        doc = await self.deployments_col.find_one({"deployment_id": deployment_id})
        if not doc:
            return None
        
        return self._sanitize({
            "deployment_id": doc["deployment_id"],
            "project_id": doc["project_id"],
            "slug": doc.get("slug"),
            "namespace": doc.get("namespace"),
            "image_ref": doc.get("image_ref"),
            "status": doc.get("status"),
            "url": doc.get("url"),
            "deploy_type": doc.get("deploy_type", "local"),
            "last_health_check": doc.get("last_health_check"),
            "error": doc.get("error"),
            "metadata": doc.get("metadata", {}),
            "created_at": doc.get("created_at"),
            "updated_at": doc.get("updated_at"),
        })
    
    async def get_project_deployments(
        self,
        project_id: str,
        include_deleted: bool = False,
    ) -> List[Dict[str, Any]]:
        """Get all deployments for a project."""
        if not hasattr(self, 'deployments_col') or self.deployments_col is None:
            self.deployments_col = self.db.deployments
        
        query = {"project_id": project_id}
        if not include_deleted:
            query["status"] = {"$ne": "deleted"}
        
        cursor = self.deployments_col.find(query).sort("created_at", -1)
        docs = await cursor.to_list(length=100)
        
        return [
            self._sanitize({
                "deployment_id": doc["deployment_id"],
                "project_id": doc["project_id"],
                "slug": doc.get("slug"),
                "namespace": doc.get("namespace"),
                "image_ref": doc.get("image_ref"),
                "status": doc.get("status"),
                "url": doc.get("url"),
                "deploy_type": doc.get("deploy_type", "local"),
                "last_health_check": doc.get("last_health_check"),
                "error": doc.get("error"),
                "metadata": doc.get("metadata", {}),
                "created_at": doc.get("created_at"),
                "updated_at": doc.get("updated_at"),
            })
            for doc in docs
        ]

    async def list_trace_deployments(self, project_id: str, limit: int = 1000) -> Dict[str, Any]:
        """Return bounded deployment metadata for the read-only trace view."""
        if not hasattr(self, "deployments_col") or self.deployments_col is None:
            self.deployments_col = self.db.deployments
        cursor = self.deployments_col.find(
            {"project_id": project_id, "status": {"$ne": "deleted"}},
            {"_id": 0, "deployment_id": 1, "project_id": 1, "status": 1, "url": 1,
             "deploy_type": 1, "created_at": 1, "updated_at": 1},
        ).sort([("created_at", -1), ("deployment_id", 1)]).limit(limit + 1)
        docs = await cursor.to_list(length=limit + 1)
        capped = len(docs) > limit
        return {
            "items": [self._sanitize({
                "deployment_id": doc.get("deployment_id"), "project_id": doc.get("project_id"),
                "status": doc.get("status"), "url": doc.get("url"), "deploy_type": doc.get("deploy_type"),
                "created_at": doc.get("created_at"), "updated_at": doc.get("updated_at"),
            }) for doc in docs[:limit]],
            "limit_reached": capped,
        }
    
    async def get_active_deployment(self, project_id: str) -> Optional[Dict[str, Any]]:
        """Get the most recent running deployment for a project."""
        if not hasattr(self, 'deployments_col') or self.deployments_col is None:
            self.deployments_col = self.db.deployments
        
        doc = await self.deployments_col.find_one(
            {"project_id": project_id, "status": "running"},
            sort=[("created_at", -1)]
        )
        
        if not doc:
            return None
        
        return self._sanitize({
            "deployment_id": doc["deployment_id"],
            "project_id": doc["project_id"],
            "slug": doc.get("slug"),
            "namespace": doc.get("namespace"),
            "image_ref": doc.get("image_ref"),
            "status": doc.get("status"),
            "url": doc.get("url"),
            "deploy_type": doc.get("deploy_type", "local"),
            "last_health_check": doc.get("last_health_check"),
            "error": doc.get("error"),
            "metadata": doc.get("metadata", {}),
            "created_at": doc.get("created_at"),
            "updated_at": doc.get("updated_at"),
        })
    
    async def update_deployment_status(
        self,
        deployment_id: str,
        status: str,
        error: Optional[str] = None,
    ) -> None:
        """Update deployment status."""
        if not hasattr(self, 'deployments_col') or self.deployments_col is None:
            self.deployments_col = self.db.deployments
        
        updates = {
            "status": status,
            "updated_at": datetime.utcnow(),
        }
        if error is not None:
            updates["error"] = error
        if status == "running":
            updates["last_health_check"] = datetime.utcnow()
        
        await self.deployments_col.update_one(
            {"deployment_id": deployment_id},
            {"$set": updates}
        )
    
    async def update_deployment_health(self, deployment_id: str) -> None:
        """Update last health check timestamp."""
        if not hasattr(self, 'deployments_col') or self.deployments_col is None:
            self.deployments_col = self.db.deployments
        
        await self.deployments_col.update_one(
            {"deployment_id": deployment_id},
            {"$set": {"last_health_check": datetime.utcnow()}}
        )

    # ==================== CLEANUP ====================
    
    async def close(self):
        """Close database connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")

    async def save_reset_token(
        self, user_id: str, tenant_id: str, ttl_minutes: int = 15
    ) -> str:
        """Save password reset token with TTL."""
        token_id = str(uuid.uuid4())
        expires_at = datetime.utcnow() + timedelta(minutes=ttl_minutes)

        doc = {
            "_id": token_id,
            "user_id": user_id,
            "tenant_id": tenant_id,
            "created_at": datetime.utcnow(),
            "expires_at": expires_at,
        }

        await self.db.password_reset_tokens.insert_one(doc)
        return token_id

    async def get_reset_token(self, token_id: str) -> Optional[dict]:
        """Get reset token if not expired."""
        token = await self.db.password_reset_tokens.find_one({"_id": token_id})
        if token and token["expires_at"] > datetime.utcnow():
            return token
        return None

    async def delete_reset_token(self, token_id: str):
        """Delete used reset token."""
        await self.db.password_reset_tokens.delete_one({"_id": token_id})

    async def get_recent_reset_requests(self, email: str, hours: int = 1) -> int:
        """Count recent reset requests for rate limiting."""
        since = datetime.utcnow() - timedelta(hours=hours)
        user = await self.db.users.find_one({"email": email})
        if not user:
            return 0

        count = await self.db.password_reset_tokens.count_documents(
            {"user_id": user["_id"], "created_at": {"$gte": since}}
        )
        return count

    async def ensure_indexes(self):
        """Create all necessary indexes for all collections."""
        if self.db is None:
            raise RuntimeError("Storage not initialized. Call initialize() first.")
        if hasattr(self, 'password_reset_tokens'):
            await self.password_reset_tokens.create_index(
                "expires_at",
                expireAfterSeconds=0,
                name="expires_at_ttl"
            )
            logger.info("Created TTL index on password_reset_tokens.expires_at")

    async def get_a2a_servers(
        self,
        tenant_id: str,
        skip: int = 0,
        limit: int = 100,
        include_disabled: bool = True,
    ) -> List[Dict[str, Any]]:
        """Get all A2A servers for a tenant."""
        query = {"tenant_id": tenant_id}
        if not include_disabled:
            query["enabled"] = True

        cursor = self.a2a_collection.find(query).skip(skip).limit(limit).sort("created_at", -1)
        return await cursor.to_list(length=limit)

    async def save_a2a_task_contract(
        self,
        *,
        tenant_id: str,
        server_id: str,
        task_id: str,
        contract: Dict[str, Any],
        skill_id: str | None,
    ) -> Dict[str, Any]:
        """Persist the first validated contract selected for an external A2A task."""
        key = {
            "tenant_id": tenant_id,
            "server_id": server_id,
            "task_id": task_id,
        }
        document = {
            **key,
            "contract": dict(contract),
            "skill_id": skill_id,
            "created_at": datetime.utcnow(),
        }
        await self.a2a_task_contracts.update_one(
            key,
            {"$setOnInsert": document},
            upsert=True,
        )
        stored = await self.a2a_task_contracts.find_one(key)
        if not stored:
            raise RuntimeError(
                f"A2A task contract was not persisted for task '{task_id}'"
            )
        return stored

    async def get_a2a_task_contract(
        self,
        tenant_id: str,
        server_id: str,
        task_id: str,
    ) -> Dict[str, Any] | None:
        """Load the immutable output contract selected when an A2A task was created."""
        return await self.a2a_task_contracts.find_one(
            {
                "tenant_id": tenant_id,
                "server_id": server_id,
                "task_id": task_id,
            }
        )

    async def create_a2a_task_state(
        self,
        *,
        project_id: str,
        run_id: str,
        node_id: str,
        tenant_id: str,
        server_id: str,
        task_id: str | None = None,
        context_id: str | None = None,
        message_id: str | None = None,
    ) -> Dict[str, Any]:
        """Persist an A2A task cursor BEFORE the node keeps waiting on it.

        task_id given -> status "in_flight" (the task_id is already confirmed
        by the adapter). task_id absent -> status "pending_submit": the
        correlation id (message_id) is durably recorded BEFORE the message/send
        call is even made, so a process death during that call still leaves
        something to resume from (AppFactory-280 finding #2) — mark_a2a_task_submitted
        completes the transition once the adapter's task_id is known.

        Idempotent on (project_id, node_id, run_id): a retry of the same node
        attempt reuses the existing open record instead of minting a second
        cursor — same "write before you keep waiting" principle as
        save_a2a_task_contract's $setOnInsert upsert one collection over.
        """
        key = {"project_id": project_id, "node_id": node_id, "run_id": run_id}
        now = datetime.utcnow()
        document = {
            **key,
            "tenant_id": tenant_id,
            "server_id": server_id,
            "task_id": task_id,
            "context_id": context_id,
            "message_id": message_id,
            "status": "in_flight" if task_id else "pending_submit",
            "created_at": now,
            "updated_at": now,
            "closed_at": None,
            "final_status": None,
        }
        await self.a2a_task_state.update_one(
            key,
            {"$setOnInsert": document},
            upsert=True,
        )
        stored = await self.a2a_task_state.find_one(key)
        if not stored:
            raise RuntimeError(
                f"A2A task state was not persisted for node '{node_id}' "
                f"(task_id={task_id!r} message_id={message_id!r})"
            )
        return stored

    async def mark_a2a_task_submitted(
        self,
        *,
        project_id: str,
        run_id: str,
        node_id: str,
        task_id: str,
        context_id: str | None = None,
    ) -> None:
        """Complete the pending_submit -> in_flight transition once the
        adapter's task_id is known — the second half of the two-step write
        create_a2a_task_state(task_id=None) started (AppFactory-280 finding #2).

        Scoped to status "pending_submit" so it can't accidentally resurrect
        an already-closed cursor (e.g. a stale resume racing a fresh attempt).
        """
        await self.a2a_task_state.update_one(
            {
                "project_id": project_id,
                "node_id": node_id,
                "run_id": run_id,
                "status": "pending_submit",
            },
            {
                "$set": {
                    "status": "in_flight",
                    "task_id": task_id,
                    "context_id": context_id,
                    "updated_at": datetime.utcnow(),
                }
            },
        )

    async def mark_a2a_task_awaiting_human(
        self,
        *,
        project_id: str,
        run_id: str,
        node_id: str,
        question_text: str,
        question_message_id: str | None = None,
        role: str | None = None,
        step: str | None = None,
    ) -> None:
        """Park an in-flight A2A task on a human question (AppFactory-281).

        Scoped to status "in_flight" for the same reason mark_a2a_task_submitted
        is scoped to "pending_submit" — can't resurrect an already-closed or
        already-parked cursor. The question fields are read back by the answer
        route (to reconstruct the card) and are NOT cleared by
        mark_a2a_task_human_answered below — kept for the audit trail.

        question_message_id (AppFactory-281 P1 review fix, bug #3 follow-up): the
        adapter's own message_id for THIS specific question, per the A2A
        protocol (every Message carries one — unlike role/step this is not our
        own convention). One task can ask several questions in sequence; only
        this id (not question_text or state alone) lets a later crash-mid-
        dispatch reconciliation tell "still the same unanswered question" apart
        from "adapter already moved on to a new one after accepting the answer".
        """
        await self.a2a_task_state.update_one(
            {
                "project_id": project_id,
                "node_id": node_id,
                "run_id": run_id,
                "status": "in_flight",
            },
            {
                "$set": {
                    "status": "awaiting_human",
                    "question_text": question_text,
                    "question_message_id": question_message_id,
                    "role": role,
                    "step": step,
                    "asked_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                }
            },
        )

    async def mark_a2a_task_human_answered(
        self,
        *,
        project_id: str,
        run_id: str,
        node_id: str,
        answer_text: str,
        session=None,
    ) -> str:
        """Record a human's answer and reopen the cursor for polling resume.

        Scoped to status "awaiting_human" — same double-answer guard pattern as
        mark_a2a_task_submitted. The cursor goes straight back to "in_flight"
        (not a fourth status) carrying pending_human_answer: this is what makes
        it visible again to get_open_a2a_task_state/_maybe_resume_interrupted_a2a
        with zero query changes on either. The resume path (AppFactory-281 plan)
        reads pending_human_answer, sends it via continue_task on the same
        task_id, then the normal in_flight polling logic takes over unchanged.

        Also mints answer_message_id (AppFactory-281 P1 review fix — bug #3, mirrors
        the original submission's message_id, AppFactory-280 finding #2): a stable
        correlation id for THIS answer, persisted before any network call is
        attempted, reused on every (re)dispatch attempt so an adapter that dedups
        on it can recognize a retry. Returns it so a caller doesn't need a second
        read. answer_dispatch_started_at is explicitly reset to None here too —
        belt-and-suspenders in case this transition is ever reachable a second
        time for the same cursor (it currently isn't: mark_a2a_task_awaiting_human
        only re-fires after mark_a2a_task_answer_delivered already cleared it).
        """
        answer_message_id = str(uuid.uuid4())
        await self.a2a_task_state.update_one(
            {
                "project_id": project_id,
                "node_id": node_id,
                "run_id": run_id,
                "status": "awaiting_human",
            },
            {
                "$set": {
                    "status": "in_flight",
                    "pending_human_answer": answer_text,
                    "answer_message_id": answer_message_id,
                    "answer_dispatch_started_at": None,
                    "answered_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                }
            },
            session=session,
        )
        return answer_message_id

    async def mark_a2a_task_answer_dispatching(
        self,
        *,
        project_id: str,
        run_id: str,
        node_id: str,
    ) -> None:
        """Record that the answer's continuation call is ABOUT to be sent — set
        right before continue_task (AppFactory-281 P1 review fix, bug #3). A crash
        between this write and the matching mark_a2a_task_answer_delivered below
        is what makes the next resume's cursor read genuinely ambiguous (rather
        than silently indistinguishable from "never attempted"), which is what
        lets _maybe_resume_interrupted_a2a tell the two cases apart.

        Scoped to status "in_flight" — same guard family as this cursor's other
        transitions, so a stale/already-closed cursor can't be resurrected.
        """
        await self.a2a_task_state.update_one(
            {
                "project_id": project_id,
                "node_id": node_id,
                "run_id": run_id,
                "status": "in_flight",
            },
            {"$set": {"answer_dispatch_started_at": datetime.utcnow()}},
        )

    async def mark_a2a_task_answer_delivered(
        self,
        *,
        project_id: str,
        run_id: str,
        node_id: str,
    ) -> None:
        """Record that the answer's continuation call returned WITHOUT raising —
        the send definitely reached the adapter (AppFactory-281 P1 review fix, bug
        #3: this is the write that was simply missing before — nothing ever
        marked a dispatch as delivered, so a restart's resume marker kept
        carrying the same pending_human_answer forever and resent it).

        Clears pending_human_answer/answer_message_id/answer_dispatch_started_at
        so the next _maybe_resume_interrupted_a2a sees a plain reconnect
        (task_id only) instead of rebuilding a human_answer marker.

        Scoped to status "in_flight" — same guard family as this cursor's other
        transitions.
        """
        await self.a2a_task_state.update_one(
            {
                "project_id": project_id,
                "node_id": node_id,
                "run_id": run_id,
                "status": "in_flight",
            },
            {
                "$set": {
                    "pending_human_answer": None,
                    "answer_message_id": None,
                    "answer_dispatch_started_at": None,
                }
            },
        )

    async def get_a2a_task_state(
        self, *, project_id: str, node_id: str, run_id: str
    ) -> Dict[str, Any] | None:
        """The cursor for one specific node's attempt, open or closed."""
        return await self.a2a_task_state.find_one(
            {"project_id": project_id, "node_id": node_id, "run_id": run_id}
        )

    async def mark_a2a_task_cancellation_pending(
        self,
        *,
        project_id: str,
        node_id: str,
        run_id: str | None,
        reason: str,
    ) -> None:
        """Durably record that a known remote task must be cancelled.

        A failed local workflow must not close its cursor before the adapter
        confirms a terminal cancellation result.  The background reconciler
        owns cursors in this state and retries only ``tasks/cancel``.
        """
        result = await self.a2a_task_state.update_one(
            {
                "project_id": project_id,
                "node_id": node_id,
                "run_id": run_id,
                "status": {"$in": ["in_flight", "cancellation_pending"]},
            },
            {
                "$set": {
                    "status": "cancellation_pending",
                    "cancellation_reason": reason,
                    "cancellation_requested_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                },
                "$inc": {"cancellation_attempt_count": 1},
            },
        )
        if result.matched_count != 1:
            raise RuntimeError(
                f"Could not persist cancellation intent for A2A node '{node_id}' "
                f"in run '{run_id}'"
            )

    async def persist_a2a_task_cancellation_intent(
        self,
        *,
        project_id: str,
        node_id: str,
        run_id: str | None,
        tenant_id: str,
        server_id: str,
        task_id: str,
        context_id: str | None,
        reason: str,
    ) -> Dict[str, Any]:
        """Durably record cancellation even when the original cursor was never saved.

        A task id can arrive from an A2A stream while MongoDB is temporarily
        unavailable.  The normal ``mark_*`` method cannot help in that case
        because there is no ``in_flight`` document to update.  This method
        atomically updates an open cursor or inserts a full cancellation outbox
        record; it never reopens a cursor another actor has already closed.
        """
        key = {"project_id": project_id, "node_id": node_id, "run_id": run_id}
        now = datetime.utcnow()
        update = {
            "$set": {
                "tenant_id": tenant_id,
                "server_id": server_id,
                "task_id": task_id,
                "context_id": context_id,
                "status": "cancellation_pending",
                "cancellation_reason": reason,
                "cancellation_requested_at": now,
                "updated_at": now,
            },
            "$inc": {"cancellation_attempt_count": 1},
        }
        open_statuses = ["pending_submit", "in_flight", "cancellation_pending"]
        result = await self.a2a_task_state.update_one(
            {**key, "status": {"$in": open_statuses}}, update
        )
        if result.matched_count == 1:
            return {**key, "status": "cancellation_pending", "task_id": task_id}

        existing = await self.a2a_task_state.find_one(key)
        if existing:
            if existing.get("status") == "closed":
                return existing
            raise RuntimeError(
                f"A2A task state for node '{node_id}' has an unsupported status "
                f"{existing.get('status')!r}"
            )

        document = {
            **key,
            "tenant_id": tenant_id,
            "server_id": server_id,
            "task_id": task_id,
            "context_id": context_id,
            "message_id": None,
            "status": "cancellation_pending",
            "cancellation_reason": reason,
            "cancellation_requested_at": now,
            "cancellation_attempt_count": 1,
            "created_at": now,
            "updated_at": now,
            "closed_at": None,
            "final_status": None,
        }
        try:
            await self.a2a_task_state.insert_one(document)
            return document
        except DuplicateKeyError:
            existing = await self.a2a_task_state.find_one(key)
            if existing and existing.get("status") == "closed":
                return existing
            if existing:
                result = await self.a2a_task_state.update_one(
                    {**key, "status": {"$in": open_statuses}}, update
                )
                if result.matched_count == 1:
                    return {
                        **key,
                        "status": "cancellation_pending",
                        "task_id": task_id,
                    }
            raise

    async def list_a2a_tasks_pending_cancellation(self) -> List[Dict[str, Any]]:
        """Return every durable cancellation request across the deployment."""
        return await self.a2a_task_state.find(
            {"status": "cancellation_pending"}
        ).to_list(length=None)

    async def get_active_a2a_task_state(self, project_id: str) -> Dict[str, Any] | None:
        """Return the active task cursor, including one awaiting cancellation."""
        return await self.a2a_task_state.find_one(
            {
                "project_id": project_id,
                "status": {"$in": ["in_flight", "cancellation_pending"]},
            },
            sort=[("created_at", -1)],
        )

    async def get_open_a2a_task_state(self, project_id: str) -> Dict[str, Any] | None:
        """The still-open A2A task cursor for a project, if any — either
        "in_flight" (task_id confirmed, resume by polling) or "pending_submit"
        (task_id never confirmed, resume by re-submitting with the same
        message_id — AppFactory-280 finding #2).

        Used by ensure_workflow_running's a2a-reconciliation arm to find what
        to resume without re-submitting message/send needlessly. A project
        should have at most one open cursor at a time; if more than one somehow
        exists, the most recently created wins.
        """
        return await self.a2a_task_state.find_one(
            {"project_id": project_id, "status": {"$in": ["in_flight", "pending_submit"]}},
            sort=[("created_at", -1)],
        )

    async def get_awaiting_human_a2a_task_state(
        self, project_id: str, task_id: str
    ) -> Dict[str, Any] | None:
        """The parked-on-a-human-question cursor for this exact task_id, if any
        (AppFactory-281). Used by the a2a-human-input answer route: only a cursor
        genuinely at "awaiting_human" is answerable — anything else (never
        existed, already answered, already closed) collapses to the same 404 the
        route raises, mirroring human_input.py's _open_question_or_raise pattern
        for the unrelated ask_human answer route.
        """
        return await self.a2a_task_state.find_one(
            {"project_id": project_id, "task_id": task_id, "status": "awaiting_human"}
        )

    async def get_a2a_task_state_by_task_id(
        self, project_id: str, task_id: str
    ) -> Dict[str, Any] | None:
        """The cursor for this exact task_id, in ANY status (open or closed).

        Unlike get_awaiting_human_a2a_task_state (scoped to "awaiting_human" —
        the only status a NEW answer may target), this is used by the answer
        route's crash-gap self-heal (AppFactory-281 P1 review fix, 5th finding):
        the non-transactional fallback writes the cursor transition FIRST, so a
        crash strictly between that write and the paired journal append leaves
        the cursor already at "in_flight" with pending_human_answer set —
        already resumable via the normal recovery machinery — while the journal
        record is missing. Needs to be found by task_id regardless of status to
        backfill just that missing record instead of 404ing a real prior answer.
        """
        return await self.a2a_task_state.find_one(
            {"project_id": project_id, "task_id": task_id}
        )

    async def list_open_a2a_project_ids(self) -> List[str]:
        """Every project with an open A2A cursor, across the whole deployment —
        not scoped to one project like get_open_a2a_task_state above.

        Used by the startup A2A reconciliation worker (AppFactory-280 Issue 1) to
        find every run that needs resuming after a backend restart, instead of
        waiting for a human to touch each project first.
        """
        return await self.a2a_task_state.distinct(
            "project_id", {"status": {"$in": ["in_flight", "pending_submit"]}}
        )

    async def close_a2a_task_state(
        self,
        *,
        project_id: str,
        node_id: str,
        run_id: str,
        final_status: str,
    ) -> None:
        """Mark a task cursor closed — call ONLY after its terminal result has
        been fully processed (artifacts saved, chat message sent). Closing
        before that finishes would let a crash in between lose the terminal
        result with nothing left to reconcile against.

        Idempotent: closing an already-closed or nonexistent cursor is a
        no-op, not an error — the reconciliation path may legitimately race a
        normal completion closing the same cursor.
        """
        await self.a2a_task_state.update_one(
            {"project_id": project_id, "node_id": node_id, "run_id": run_id},
            {
                "$set": {
                    "status": "closed",
                    "final_status": final_status,
                    "closed_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                }
            },
        )


    async def delete_a2a_task_contracts_by_server(
        self,
        tenant_id: str,
        server_id: str,
        *,
        session=None,
    ) -> int:
        """Delete task contracts belonging to one tenant-scoped A2A server."""
        result = await self.a2a_task_contracts.delete_many(
            {"tenant_id": tenant_id, "server_id": server_id},
            session=session,
        )
        return int(result.deleted_count)

    async def delete_a2a_task_contracts_by_tenant(
        self,
        tenant_id: str,
        *,
        session=None,
    ) -> int:
        """Delete all external A2A task contracts owned by a tenant."""
        normalized_tenant_id = self._validate_tenant_delete_scope(tenant_id)
        result = await self.a2a_task_contracts.delete_many(
            {"tenant_id": normalized_tenant_id},
            session=session,
        )
        return int(result.deleted_count)

    async def get_a2a_server(
        self,
        server_id: str,
        tenant_id: str,
    ) -> Dict[str, Any] | None:
        """Get single A2A server by ID."""
        return await self.a2a_collection.find_one({"_id": server_id, "tenant_id": tenant_id})

    async def get_a2a_server_by_name(
        self,
        name: str,
        tenant_id: str,
    ) -> Dict[str, Any] | None:
        """Get A2A server by name (for uniqueness check)."""
        return await self.a2a_collection.find_one({"name": name, "tenant_id": tenant_id})

    async def create_a2a_server(
        self,
        tenant_id: str,
        server_data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Create new A2A server."""
        now = datetime.utcnow()

        document = {
            "_id": str(uuid.uuid4()),
            "tenant_id": tenant_id,
            "created_at": now,
            "updated_at": now,
            **server_data
        }

        await self.a2a_collection.insert_one(document)
        return document

    async def update_a2a_server(
        self,
        server_id: str,
        tenant_id: str,
        update_data: Dict[str, Any],
        expected_updated_at: Optional[datetime] = None,
    ) -> Dict[str, Any] | None:
        """Update existing A2A server."""
        fields = {**update_data, "updated_at": datetime.utcnow()}
        query = {"_id": server_id, "tenant_id": tenant_id}
        if expected_updated_at is not None:
            query["updated_at"] = expected_updated_at

        result = await self.a2a_collection.find_one_and_update(
            query,
            {"$set": fields},
            return_document=True
        )
        return result

    async def update_a2a_server_rotated_refresh_token(
        self,
        server_id: str,
        tenant_id: str,
        refresh_token: str,
        *,
        expected_updated_at: Optional[datetime] = None,
    ) -> Dict[str, Any] | None:
        """Persist an OAuth2-rotated refresh token without changing configuration state.

        A refresh token is runtime credential state, not an administrator's configuration
        edit.  Keep ``updated_at`` stable so a preflight CAS remains valid, but condition
        on it when known to avoid overwriting an administrator's concurrent auth update.
        """
        query = {"_id": server_id, "tenant_id": tenant_id}
        if expected_updated_at is not None:
            query["updated_at"] = expected_updated_at
        return await self.a2a_collection.find_one_and_update(
            query,
            {"$set": {"auth.refresh_token": refresh_token}},
            return_document=True,
        )

    async def save_a2a_server(self, doc: Dict[str, Any], actor_id: str | None = None) -> str:
        """Audit-stamped upsert of an A2A server by ``_id`` (minted when absent).

        Config-bundle import needs the same ``save_X(doc, actor_id=...)`` contract
        the other configuration kinds expose; UI create/edit use the narrower
        create_a2a_server/update_a2a_server. Uniqueness is ``(tenant_id, name)``
        (tenant_name_unique index), so the caller must resolve the target ``_id`` by
        name first — otherwise a rename would collide on insert. The doc fully
        replaces the stored one, dropping any cached agent card: an import can change
        the endpoint the cache was keyed to, so a stale card would misrepresent it.
        created_at/created_by are carried over so a re-import doesn't reset them.
        """
        doc = dict(doc)
        server_id = str(doc.get("_id") or "").strip() or str(uuid.uuid4())
        doc["_id"] = server_id
        existing = await self.a2a_collection.find_one({"_id": server_id})
        if existing:
            for field in ("created_at", "created_by"):
                if existing.get(field) is not None and not doc.get(field):
                    doc[field] = existing[field]
        self._stamp_audit_fields(doc, actor_id, is_new=existing is None)
        await self.a2a_collection.replace_one({"_id": server_id}, doc, upsert=True)
        return server_id

    @staticmethod
    def build_a2a_card_summary(agent_card: Dict[str, Any]) -> Dict[str, Any]:
        """Project an A2A agent card to the stored summary (powers the UI list + skills count)."""
        # v0.3 cards expose a top-level `url`; v1.0 cards move it into supportedInterfaces[].url.
        url = agent_card.get("url")
        if not url:
            interfaces = agent_card.get("supportedInterfaces") or []
            if interfaces:
                url = interfaces[0].get("url")
        return {
            "name": agent_card.get("name"),
            "version": agent_card.get("version"),
            "url": url,
            "defaultInputModes": agent_card.get("defaultInputModes", []),
            "defaultOutputModes": agent_card.get("defaultOutputModes", []),
            "skills": [
                {
                    "id": s.get("id"),
                    "name": s.get("name"),
                    "description": s.get("description"),
                    "tags": s.get("tags", []),
                    "examples": s.get("examples", []),
                    # Per-skill MIME modes override the card defaults (A2A wire JSON is
                    # camelCase, same as defaultInputModes above). Whitelisted, not copied
                    # wholesale, so unknown card keys still don't leak into the summary.
                    "inputModes": s.get("inputModes", []),
                    "outputModes": s.get("outputModes", []),
                }
                for s in agent_card.get("skills", [])
            ],
            "capabilities": agent_card.get("capabilities", {})
        }

    async def update_a2a_server_cache(
        self,
        server_id: str,
        tenant_id: str,
        agent_card: Dict[str, Any],
        validated_at: datetime,
        contract: Optional[Dict[str, Any]] = None,
        expected_updated_at: Optional[datetime] = None,
        refresh_token: Optional[str] = None,
        use_a2a_streaming: Optional[bool] = None,
    ) -> Dict[str, Any] | None:
        """Update cached agent card and validation timestamp."""
        summary = self.build_a2a_card_summary(agent_card)

        fields = {
            "cached_agent_card": agent_card,
            "cached_agent_card_summary": summary,
            "cached_at": validated_at,
            "last_validated_at": validated_at,
            "updated_at": datetime.utcnow(),
        }
        if contract is not None:
            fields["a2a_contract"] = contract
        if refresh_token is not None:
            fields["auth.refresh_token"] = refresh_token
        if use_a2a_streaming is not None:
            fields["use_a2a_streaming"] = use_a2a_streaming

        query = {"_id": server_id, "tenant_id": tenant_id}
        if expected_updated_at is not None:
            query["updated_at"] = expected_updated_at

        result = await self.a2a_collection.find_one_and_update(
            query,
            {"$set": fields},
            return_document=True
        )
        return result

    async def delete_a2a_server(
        self,
        server_id: str,
        tenant_id: str,
    ) -> bool:
        """Delete A2A server."""
        result = await self.a2a_collection.delete_one({"_id": server_id, "tenant_id": tenant_id})
        deleted = result.deleted_count > 0
        if deleted:
            await self.delete_a2a_task_contracts_by_server(tenant_id, server_id)
        return deleted

    async def update_a2a_server_cache_with_endpoint(
        self,
        server_id: str,
        tenant_id: str,
        agent_card: Dict[str, Any],
        validated_at: datetime,
        endpoint: str,
    ) -> Dict[str, Any] | None:
        """Update cached agent card for specific endpoint."""
        def sanitize_key(key: str) -> str:
            return key.replace('.', '_').replace('$', '_').replace('/', '_')

        endpoint_safe = sanitize_key(endpoint)
        cache_key = f"cached_agent_card_{endpoint_safe}"
        cache_key_summary = f"{cache_key}_summary"
        cached_at_key = f"cached_at_{endpoint_safe}"

        summary = self.build_a2a_card_summary(agent_card)
        summary["endpoint"] = endpoint

        update_data = {
            cache_key: agent_card,
            cache_key_summary: summary,
            cached_at_key: validated_at,
            "last_validated_at": validated_at,
            "updated_at": datetime.utcnow()
        }

        result = await self.a2a_collection.find_one_and_update(
            {"_id": server_id, "tenant_id": tenant_id},
            {"$set": update_data},
            return_document=True
        )
        return result
