"""Checkpoint bindings and append-only point journal."""

from __future__ import annotations

from copy import deepcopy

from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from storage.message_store import _bson_text_safe


class CheckpointConflict(ValueError):
    pass


class CheckpointNotFound(LookupError):
    pass


class CheckpointStore:
    BINDING_FIELDS = (
        "run_id",
        "project_id",
        "tenant_id",
        "server_id",
        "node_id",
        "context_id",
        "workflow_def",
        "message",
        "inputs",
    )

    def __init__(self, storage, message_store):
        self.storage = storage
        self.message_store = message_store
        self.runs = storage.db.runs
        self.messages = message_store.messages

    async def initialize(self) -> None:
        await self.messages.create_index(
            [("run_id", 1), ("data.point_id", 1)],
            unique=True,
            name="checkpoint_point_identity",
            partialFilterExpression={"subtype": "checkpoint_saved"},
        )

    async def bind_run(self, binding: dict) -> dict:
        value = {key: deepcopy(binding.get(key)) for key in self.BINDING_FIELDS}
        run = await self.runs.find_one_and_update(
            {
                "run_id": value["run_id"],
                "project_id": value["project_id"],
                "deleted_at": None,
                "checkpoint": {"$exists": False},
            },
            {"$set": {"checkpoint": value}},
            return_document=ReturnDocument.AFTER,
        )
        if run:
            return run["checkpoint"]
        live = await self.runs.find_one(
            {
                "run_id": value["run_id"],
                "project_id": value["project_id"],
                "deleted_at": None,
            },
            {"checkpoint": 1},
        )
        if not live:
            raise CheckpointNotFound("run not found")
        if live.get("checkpoint") == value:
            return live["checkpoint"]
        raise CheckpointConflict("run already has a different checkpoint binding")

    async def get_binding(self, run_id: str) -> dict | None:
        run = await self.runs.find_one(
            {"run_id": run_id, "deleted_at": None}, {"_id": 0, "checkpoint": 1}
        )
        return (
            deepcopy(run.get("checkpoint")) if run and run.get("checkpoint") else None
        )

    @staticmethod
    def _project(message: dict) -> dict:
        return {
            **deepcopy(message.get("data") or {}),
            "number": message["sequence"],
            "message_id": message["id"],
        }

    async def get_point(
        self, project_id: str, run_id: str, point_id: str
    ) -> dict | None:
        message = await self.messages.find_one(
            {
                "project_id": project_id,
                "run_id": run_id,
                "subtype": "checkpoint_saved",
                "data.point_id": point_id,
            },
            {"_id": 0},
        )
        return self._project(message) if message else None

    async def record_point(self, binding: dict, payload: dict) -> tuple[dict, bool]:
        data = _bson_text_safe({**deepcopy(payload), "server_id": binding["server_id"]})
        try:
            message = await self.message_store.append(
                binding["project_id"],
                {
                    "type": "event",
                    "subtype": "checkpoint_saved",
                    "content": payload.get("label")
                    or f"Checkpoint {payload['point_id']}",
                    "data": data,
                },
                run_id=binding["run_id"],
            )
            return message, True
        except DuplicateKeyError as exc:
            pattern = (exc.details or {}).get("keyPattern", {})
            attributable = {"run_id", "data.point_id"}.issubset(pattern) or (
                "checkpoint_point_identity" in str(exc)
            )
            if not attributable:
                raise
            existing = await self.messages.find_one(
                {
                    "project_id": binding["project_id"],
                    "run_id": binding["run_id"],
                    "subtype": "checkpoint_saved",
                    "data.point_id": data["point_id"],
                },
                {"_id": 0},
            )
            if not existing:
                raise
            # Compare the canonical storage form that MessageStore persisted.
            if existing.get("data") != data:
                raise CheckpointConflict(
                    "checkpoint point already exists with different payload"
                ) from exc
            return existing, False

    async def list_points(
        self, project_id: str, run_id: str, limit: int = 100
    ) -> list[dict]:
        cursor = (
            self.messages.find(
                {
                    "project_id": project_id,
                    "run_id": run_id,
                    "subtype": "checkpoint_saved",
                },
                {"_id": 0},
            )
            .sort("sequence", 1)
            .limit(limit)
        )
        return [self._project(row) for row in await cursor.to_list(length=limit)]
