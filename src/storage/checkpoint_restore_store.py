"""Durable per-project CAS journal for non-idempotent checkpoint restore."""

from datetime import datetime, timezone
from uuid import uuid4
import logging

from pymongo import ReturnDocument

from telemetry.run_cache import remember_run_context
from telemetry.run_diagnostics import report_trace_issue
from telemetry.run_identity import finish_run_trace, prepare_run_trace

logger = logging.getLogger(__name__)
BLOCKING_STATES = ("request-started", "external-restored", "ready", "outcome-unknown")


class RestoreError(Exception):
    def __init__(self, code, status_code=409, run_id=None):
        super().__init__(code)
        self.code, self.status_code, self.run_id = code, status_code, run_id


class CheckpointRestoreStore:
    def __init__(self, storage):
        self.storage = storage

    @property
    def projects(self):
        return self.storage.db.projects

    @property
    def runs(self):
        return self.storage.db.runs

    async def get(self, project_id):
        doc = await self.storage.load_project(project_id)
        return doc.get("checkpoint_restore") if isinstance(doc, dict) else None

    async def claim(
        self, project_id, source_run_id, point_id, *, previous_operation=None
    ):
        op = {
            "run_id": str(uuid4()),
            "restored_from": {"run_id": source_run_id, "point_id": point_id},
            "state": "request-started",
            "created_at": datetime.now(timezone.utc),
        }
        allowed = [
            {"checkpoint_restore": {"$exists": False}},
            {"checkpoint_restore.state": "failed"},
        ]
        if previous_operation:
            allowed.append(
                {
                    "checkpoint_restore.run_id": previous_operation,
                    "checkpoint_restore.state": "dispatched",
                }
            )
        doc = await self.projects.find_one_and_update(
            {"project_id": project_id, "$or": allowed},
            {"$set": {"checkpoint_restore": op}},
            return_document=ReturnDocument.AFTER,
        )
        if not doc:
            current = await self.get(project_id)
            raise RestoreError(
                "checkpoint_restore_pending", run_id=(current or {}).get("run_id")
            )
        # The project claim intentionally precedes run creation: a failed write
        # leaves a durable closed gate and cannot cause a second remote restore.
        telemetry, root_span = prepare_run_trace(project_id, op["run_id"])
        try:
            await self.runs.insert_one(
                {
                    "project_id": project_id,
                    "run_id": op["run_id"],
                    "parent_run_id": source_run_id,
                    "restored_from": op["restored_from"],
                    "restore_state": op["state"],
                    "active": False,
                    "run_status": "initialized",
                    "created_at": op["created_at"],
                    "deleted_at": None,
                    "telemetry": telemetry,
                }
            )
            if telemetry.get("status") != "degraded":
                remember_run_context(
                    self.storage, project_id, op["run_id"], telemetry["traceparent"]
                )
            else:
                await report_trace_issue(
                    self.storage, project_id, op["run_id"], "root_span_failed"
                )
        finally:
            finish_run_trace(root_span, project_id=project_id, run_id=op["run_id"])
        return op

    async def transition(self, project_id, op, expected, state, **fields):
        updates = {"state": state, **fields}
        doc = await self.projects.find_one_and_update(
            {
                "project_id": project_id,
                "checkpoint_restore.run_id": op["run_id"],
                "checkpoint_restore.state": expected,
            },
            {"$set": {f"checkpoint_restore.{k}": v for k, v in updates.items()}},
            return_document=ReturnDocument.AFTER,
        )
        if not doc:
            raise RestoreError("checkpoint_restore_state_conflict", run_id=op["run_id"])
        await self.runs.update_one(
            {"run_id": op["run_id"]}, {"$set": {"restore_state": state}}
        )
        logger.info(
            "[CHECKPOINT] project_id=%s run_id=%s state=%s — restore transition",
            project_id,
            op["run_id"],
            state,
        )
        return doc["checkpoint_restore"]

    async def blocked_reason(self, project_id):
        op = await self.get(project_id)
        if not op:
            return None
        if op["state"] in BLOCKING_STATES:
            return "checkpoint_restore_pending"
        if op["state"] == "dispatched":
            run = await self.storage.get_run(op["run_id"])
            if run and run.get("run_status") not in (
                "completed",
                "failed",
                "cancelled",
            ):
                cursor = await self.storage.get_open_a2a_task_state(project_id)
                if not cursor or cursor.get("run_id") != op["run_id"]:
                    if await self.has_continuation(project_id, op["run_id"]):
                        return None
                    return "checkpoint_dispatch_outcome_unknown"
        return None

    async def has_continuation(self, project_id, run_id):
        # New restored Runs start at the bound A2A node with an empty journal.
        # A downstream tool/gate therefore proves traversal passed that node.
        row = await self.storage.db.messages.find_one(
            {
                "project_id": project_id,
                "run_id": run_id,
                "$or": [
                    {"type": "approval", "status": "pending"},
                    {
                        "type": "tool_call",
                        "data.workflow_node_id": {"$exists": True, "$nin": [None, ""]},
                    },
                ],
            },
            {"_id": 1},
        )
        if row:
            return True
        parked = await self.storage.db.a2a_task_state.find_one(
            {
                "project_id": project_id,
                "run_id": run_id,
                "status": "awaiting_human",
            },
            {"_id": 1},
        )
        return bool(parked)

    async def assert_mutation_allowed(self, project_id):
        op = await self.get(project_id)
        if op and op["state"] == "dispatched":
            run = await self.storage.get_run(op["run_id"])
            if not run or run.get("run_status") not in (
                "completed",
                "failed",
                "cancelled",
            ):
                raise RestoreError("checkpoint_restore_running", run_id=op["run_id"])
        if await self.blocked_reason(project_id):
            raise RestoreError("checkpoint_restore_pending", run_id=op["run_id"])
