"""
Agent LLM Calls Store

Per-invocation capture of what the LLM actually saw and returned.
Backs the Agent Invocation Inspector.

Storage contract: this MUST be a real MongoDB collection so operators and
debugging agents can query it directly with `db.agent_llm_calls.find(...)`.
The HTTP API in api/routes/agent_llm_calls.py is the UI surface, not the
only surface.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from storage.llm_call_tool_results import cap_tool_results

logger = logging.getLogger(__name__)


# Soft cap for the per-invocation document body (excluding _id / created_at
# / metadata fields). Fields are progressively truncated, largest first,
# until the body fits. See spec section "Storage cost".
DOC_SOFT_CAP_BYTES = 200_000

# Mongo TTL for unflagged docs (30 days). Partial index — flagged docs are
# retained indefinitely.
TTL_SECONDS = 30 * 24 * 60 * 60


def _field_bytes(value: Any) -> int:
    """Estimate JSON-encoded size of a single field value."""
    if value is None:
        return 0
    try:
        return len(json.dumps(value, default=str).encode("utf-8"))
    except Exception:
        return 0


def _doc_body_bytes(doc: Dict[str, Any]) -> int:
    """Estimate total JSON-encoded size of the doc body."""
    try:
        return len(json.dumps(doc, default=str).encode("utf-8"))
    except Exception:
        return 0


def _truncate_placeholder(field: str, original_size: int) -> str:
    return f"<truncated: original {original_size} bytes — see truncated=true>"


def _list_item_names(items: List[Any], limit: int = 200) -> List[str]:
    """Best-effort identifying names for items in a truncated list.

    Tool schemas are the dominant cause of truncation (one capture can carry
    23 schemas / 150KB). Dropping the heavy bodies but keeping the names lets
    the Inspector still answer "which tools did this invocation have?".
    Handles both Responses-API ({"name": ...}) and Chat-API
    ({"function": {"name": ...}}) tool shapes.
    """
    names: List[str] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        name = it.get("name")
        if name is None and isinstance(it.get("function"), dict):
            name = it["function"].get("name")
        if isinstance(name, str) and name:
            names.append(name)
        if len(names) >= limit:
            break
    return names


def apply_truncation(doc: Dict[str, Any], cap_bytes: int = DOC_SOFT_CAP_BYTES) -> Dict[str, Any]:
    """Truncate large fields in-place until the doc fits the cap.

    Mutates and returns `doc`. Sets `doc["truncated"] = True` and
    `doc["original_sizes"]` if any field had to be truncated.
    """
    if _doc_body_bytes(doc) <= cap_bytes:
        doc.setdefault("truncated", False)
        doc.setdefault("original_sizes", None)
        return doc

    req = doc.setdefault("request", {})
    resp = doc.setdefault("response", {})

    candidates = [
        ("request.system", req, "system", req.get("system")),
        ("request.messages", req, "messages", req.get("messages")),
        ("request.tools", req, "tools", req.get("tools")),
        ("response.text", resp, "text", resp.get("text")),
        ("response.thinking", resp, "thinking", resp.get("thinking")),
        ("response.tool_uses", resp, "tool_uses", resp.get("tool_uses")),
    ]

    original_sizes: Dict[str, int] = {}
    candidates.sort(key=lambda c: _field_bytes(c[3]), reverse=True)

    for path, parent, key, value in candidates:
        if _doc_body_bytes(doc) <= cap_bytes:
            break
        if value is None:
            continue
        original = _field_bytes(value)
        if original == 0:
            continue
        original_sizes[path] = original
        if isinstance(value, str):
            parent[key] = _truncate_placeholder(path, original)
        elif isinstance(value, list):
            placeholder = {"_truncated": True, "_original_count": len(value), "_original_bytes": original}
            names = _list_item_names(value)
            if names:
                placeholder["_names"] = names
            parent[key] = [placeholder]
        else:
            parent[key] = {"_truncated": True, "_original_bytes": original}

    doc["truncated"] = True
    doc["original_sizes"] = original_sizes
    return doc


class AgentLLMCallsStore:
    """Async accessor for the `agent_llm_calls` Mongo collection."""

    def __init__(self, db):
        """
        Args:
            db: MongoStorageBackend instance (has `.db` motor handle).
        """
        self.db = db
        self._collection = None

    @property
    def collection(self):
        if self._collection is None:
            self._collection = self.db.db.agent_llm_calls
        return self._collection

    async def initialize(self):
        """Create indexes. Safe to call repeatedly."""
        logger.info("Creating agent_llm_calls indexes...")
        await self.collection.create_index(
            [("project_id", 1), ("run_id", 1), ("started_at", -1)],
        )
        await self.collection.create_index(
            [("agent_id", 1), ("started_at", -1)],
        )
        # Partial TTL: only unflagged docs expire after 30 days.
        await self.collection.create_index(
            "created_at",
            expireAfterSeconds=TTL_SECONDS,
            partialFilterExpression={"flagged": False},
        )
        logger.info("✅ agent_llm_calls indexes created")

    @staticmethod
    def _coerce_created_at(value: Any) -> datetime:
        """Return a timezone-aware datetime for the TTL-indexed created_at.

        Stored as a BSON Date — never an ISO string — because Mongo's TTL
        monitor only expires Date-typed fields; a string is silently ignored
        and the partial TTL index would never reap unflagged docs. Mirrors the
        date normalization in MongoStorageBackend.save_project.
        """
        if isinstance(value, str):
            try:
                value = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                value = None
        if not isinstance(value, datetime):
            return datetime.now(timezone.utc)
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    async def insert_call(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Insert a capture doc. Applies truncation if oversized.

        Returns the (possibly mutated) doc that was written. Defensive:
        write failures log and return the doc anyway — the capture path
        must not break the agent invocation it was observing.

        Runs the body through json.dumps(default=str) before inserting so
        provider-specific objects (e.g. openai's ChatCompletionMessageToolCall
        that the SimpleAgentRunner threads back into the next turn's
        `messages`) survive the BSON encoder as strings instead of being
        dropped by `insert_one`.
        """
        doc.setdefault("flagged", False)
        created_at = self._coerce_created_at(doc.get("created_at"))
        doc["created_at"] = created_at
        doc.update(cap_tool_results(doc))
        apply_truncation(doc)
        try:
            clean = json.loads(json.dumps(doc, default=str))
        except Exception:
            clean = doc
        # The json round-trip above stringifies datetimes (default=str); put
        # created_at back as a real datetime so it persists as a BSON Date and
        # the partial TTL index can actually expire it.
        clean["created_at"] = created_at
        try:
            await self.collection.insert_one(clean)
        except Exception as exc:
            logger.warning(
                "agent_llm_calls.insert_call failed (call_id=%s): %s",
                doc.get("_id"),
                exc,
            )
        return doc

    async def get_call(self, call_id: str) -> Optional[Dict[str, Any]]:
        return await self.collection.find_one({"_id": call_id})

    async def list_calls(
        self,
        project_id: str,
        agent_id: Optional[str] = None,
        run_id: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        query: Dict[str, Any] = {"project_id": project_id}
        if agent_id:
            query["agent_id"] = agent_id
        if run_id:
            query["run_id"] = run_id
        projection = {
            "_id": 1,
            "agent_id": 1,
            "run_id": 1,
            "project_id": 1,
            "task_id": 1,
            "turn_index": 1,
            "started_at": 1,
            "completed_at": 1,
            "model": 1,
            "response.usage": 1,
            "response.stop_reason": 1,
            "response.error": 1,
            "truncated": 1,
            "flagged": 1,
        }
        cursor = (
            self.collection.find(query, projection)
            .sort("started_at", -1)
            .limit(max(1, min(int(limit or 50), 500)))
        )
        return [doc async for doc in cursor]

    async def list_trace_summaries(
        self,
        project_id: str,
        run_id: Optional[str] = None,
        limit: int = 5000,
        include_unscoped: bool = False,
    ) -> Dict[str, Any]:
        query: Dict[str, Any] = {"project_id": project_id}
        if run_id and include_unscoped:
            query["$or"] = [
                {"run_id": run_id},
                {"run_id": None},
                {"run_id": {"$exists": False}},
            ]
        elif run_id:
            query["run_id"] = run_id
        projection = {"_id": 1, "project_id": 1, "run_id": 1, "task_id": 1, "agent_id": 1,
                      "turn_index": 1, "started_at": 1, "completed_at": 1, "model": 1,
                      "status": 1, "truncated": 1, "response.usage": 1,
                      "response.stop_reason": 1, "response.error": 1,
                      "response.tool_uses.id": 1, "response.tool_uses.name": 1}
        cursor = self.collection.find(query, projection).sort([("started_at", -1), ("_id", -1)]).limit(limit + 1)
        rows = await cursor.to_list(length=limit + 1)
        capped = len(rows) > limit
        rows = rows[:limit]
        rows.reverse()
        return {"items": rows, "limit_reached": capped}

    async def flag_call(self, call_id: str) -> bool:
        """Mark a call as flagged (exempt from TTL)."""
        result = await self.collection.update_one(
            {"_id": call_id},
            {"$set": {"flagged": True}},
        )
        return result.matched_count > 0
