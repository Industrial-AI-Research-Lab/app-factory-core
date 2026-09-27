"""Atomic persistence for a Run's root trace context."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pymongo import ReturnDocument

if TYPE_CHECKING:
    from storage.mongo_backend import MongoStorageBackend


logger = logging.getLogger(__name__)


class RunTraceStore:
    """Read and claim the immutable root trace context for a live Run."""

    def __init__(self, storage: MongoStorageBackend):
        self.runs = storage.db.runs

    async def read(self, project_id: str, run_id: str) -> str | None:
        selector = {"run_id": run_id, "project_id": project_id, "deleted_at": None}
        doc = await self.runs.find_one(selector, {"_id": 0, "telemetry": 1})
        if doc is None:
            logger.warning(
                "[OTEL] action=read_run_context project_id=%s run_id=%s — live Run not found",
                project_id,
                run_id,
            )
            raise LookupError("Run not found")

        telemetry = doc.get("telemetry")
        self.telemetry_status = (
            telemetry.get("status") if isinstance(telemetry, dict) else None
        )
        self.telemetry_error = (
            telemetry.get("error") if isinstance(telemetry, dict) else None
        )
        if telemetry is None:
            return None
        if not isinstance(telemetry, dict):
            logger.warning(
                "[OTEL] action=read_run_context project_id=%s run_id=%s — telemetry is not an object",
                project_id,
                run_id,
            )
            raise ValueError("Run telemetry must be an object or null")

        traceparent = telemetry.get("traceparent")
        if traceparent is None:
            return None
        if not isinstance(traceparent, str):
            logger.warning(
                "[OTEL] action=read_run_context project_id=%s run_id=%s — traceparent is not a string",
                project_id,
                run_id,
            )
            raise ValueError("Run traceparent must be a string or null")
        return traceparent

    async def claim(self, project_id: str, run_id: str, candidate: str) -> str:
        selector = {"run_id": run_id, "project_id": project_id, "deleted_at": None}
        doc = await self.runs.find_one_and_update(
            {
                **selector,
                "$expr": {
                    "$and": [
                        {
                            "$in": [
                                {"$type": "$telemetry"},
                                ["missing", "null", "object"],
                            ]
                        },
                        {
                            "$in": [
                                {"$type": "$telemetry.traceparent"},
                                ["missing", "null"],
                            ]
                        },
                    ]
                },
            },
            [
                {
                    "$set": {
                        "telemetry": {
                            "$mergeObjects": [
                                {"$ifNull": ["$telemetry", {}]},
                                {"traceparent": candidate},
                            ]
                        }
                    }
                }
            ],
            return_document=ReturnDocument.AFTER,
            projection={"telemetry": 1},
        )
        winner = (
            doc["telemetry"]["traceparent"]
            if doc
            else await self.read(project_id, run_id)
        )
        if winner is None:
            logger.warning(
                "[OTEL] action=claim_run_context project_id=%s run_id=%s — trace context was not initialized",
                project_id,
                run_id,
            )
            raise LookupError("Run trace context was not initialized")
        return winner
