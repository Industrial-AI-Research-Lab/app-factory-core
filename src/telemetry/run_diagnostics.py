"""Persist tracing faults through the existing Run and chat write paths."""

import logging

from storage.message_store import MessageStore
from telemetry.run_cache import get_run_slot

logger = logging.getLogger(__name__)


async def report_trace_issue(storage, project_id, run_id, reason):
    logger.error(
        "[OTEL] project_id=%s run_id=%s reason=%s — Run tracing is degraded",
        project_id,
        run_id,
        reason,
    )
    slot = get_run_slot(storage, run_id)
    issue = (project_id, reason)
    if issue in slot.reported:
        return
    slot.reported.add(issue)
    status = (
        "legacy_initialized" if reason == "legacy_trace_initialized" else "degraded"
    )
    try:
        result = await storage.db.runs.update_one(
            {"project_id": project_id, "run_id": run_id, "deleted_at": None},
            [
                {
                    "$set": {
                        "telemetry": {
                            "$mergeObjects": [
                                {
                                    "$cond": [
                                        {"$eq": [{"$type": "$telemetry"}, "object"]},
                                        "$telemetry",
                                        {"invalid_value": "$telemetry"},
                                    ]
                                },
                                {"$literal": {"status": status, "error": reason}},
                            ]
                        }
                    }
                }
            ],
        )
        if not result.matched_count:
            logger.error(
                "[OTEL] project_id=%s run_id=%s — status Run not found",
                project_id,
                run_id,
            )
    except Exception:
        slot.reported.discard(issue)
        logger.exception(
            "[OTEL] project_id=%s run_id=%s — cannot persist trace status",
            project_id,
            run_id,
        )
    try:
        await MessageStore(storage).append_assistant_message(
            project_id,
            "Не удалось штатно подключить трассировку запуска. "
            "Выполнение продолжается; диагностические записи могут быть разделены. "
            f"Причина: {reason}.",
            run_id=run_id,
            metadata={"error": True, "telemetry": True, "reason": reason},
        )
    except Exception:
        slot.reported.discard(issue)
        logger.exception(
            "[OTEL] project_id=%s run_id=%s — cannot append trace warning",
            project_id,
            run_id,
        )
