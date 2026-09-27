"""
Message Store - Unified Message System

Append-only message log with atomic sequence numbers.
Single source of truth for all project communication.

Message types:
- user: User sent a message
- assistant: AI response
- approval: Approval cards (requirements, plan, output, deploy)
- system: System notifications (phase changes, approval results)
- event: Lightweight events (task progress, tool execution)
- tool_call: LLM requested a tool call (ledger; persisted BEFORE execution)
- tool_result: outcome of a tool_call, paired by data.tool_call_id (ledger)

A tool_call with no paired tool_result is a "hanging call" — the point where
the agent stopped and is waiting. Pair presence is the single source of
truth for that state; tool_call records carry no status field to flip.
See docs/adr/0008-tool-ledger-first-class-records.md.
"""

from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
import json
import uuid
import logging

logger = logging.getLogger(__name__)

# For get_messages(exclude_types=...): readers that want conversation, not
# journal volume, exclude these so the fixed read window (default limit=1000,
# oldest-first) isn't spent on records they'd throw away — tool activity
# writes two of these per call, far faster than conversation grows.
TOOL_LEDGER_TYPES = ("tool_call", "tool_result")


# Journal payloads are stored whole (up to the 512KB spill threshold) but
# RENDERED as bounded previews — into agent prompts and over the messages
# API alike — so one fat result can't dominate a context window or a page
# load. Consumers that need the full body read the record itself.
TOOL_JOURNAL_PREVIEW_CHARS = 2000


def tool_payload_preview(value: Any) -> str:
    """Bounded text form of a journal payload (arguments or result)."""
    if not isinstance(value, str):
        try:
            value = json.dumps(value, ensure_ascii=False, default=str)
        except (TypeError, ValueError):
            value = str(value)
    if len(value) > TOOL_JOURNAL_PREVIEW_CHARS:
        return (
            value[:TOOL_JOURNAL_PREVIEW_CHARS]
            + f"…(+{len(value) - TOOL_JOURNAL_PREVIEW_CHARS} chars)"
        )
    return value


def pair_tool_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Pair journal records into [{"call": rec|None, "result": rec|None}].

    Matching is (run_id, tool_call_id) plus sequence adjacency — a result
    closes the EARLIEST still-open call with its key — because providers may
    reuse a call id across rounds within one run (ADR-0008), so the id alone
    is ambiguous. `records` must be ascending by sequence (get_messages
    order). A call with no result stays hanging (result None); a result
    whose call fell outside the fetched window becomes an orphan (call
    None), anchored at its own position.
    """
    pairs: List[Dict[str, Any]] = []
    open_by_key: Dict[Any, List[Dict[str, Any]]] = {}
    for rec in records:
        data = rec.get("data") or {}
        key = (rec.get("run_id"), data.get("tool_call_id"))
        if rec.get("type") == "tool_call":
            pair = {"call": rec, "result": None}
            pairs.append(pair)
            if data.get("tool_call_id"):
                open_by_key.setdefault(key, []).append(pair)
        elif rec.get("type") == "tool_result":
            waiting = open_by_key.get(key)
            if waiting:
                waiting.pop(0)["result"] = rec
            else:
                pairs.append({"call": None, "result": rec})
    return pairs


# BSON caps integers at 8 bytes; JSON has no width limit.
_BSON_INT64_MIN = -(2**63)
_BSON_INT64_MAX = 2**63 - 1


def _bson_text_safe(value: Any) -> Any:
    """BSON rejects shapes JSON allows; store the closest form Mongo can hold
    instead of letting the loud persist crash AFTER a tool already ran (the
    loud-fail stays reserved for real database problems). A lone UTF-16
    surrogate (half an emoji clipped by truncation) becomes visible
    '\\ud83d' text via backslashreplace, not '?'; an int outside int64
    (uint64 ids from Go/Rust services are routine) becomes its decimal
    string; dict keys additionally go through _bson_key_safe. Anything else
    that isn't BSON-native — set/Decimal/UUID/arbitrary objects, which only
    Python-side tool handlers can produce — falls back to str(value), the
    same when-in-doubt rule the conversation and archive-measure paths
    already apply via json.dumps(default=str)."""
    if isinstance(value, str):
        try:
            value.encode("utf-8")
            return value
        except UnicodeEncodeError:
            return value.encode("utf-8", "backslashreplace").decode("utf-8")
    if isinstance(value, int) and not (_BSON_INT64_MIN <= value <= _BSON_INT64_MAX):
        return str(value)
    if isinstance(value, dict):
        return {_bson_key_safe(k): _bson_text_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_bson_text_safe(v) for v in value]
    # bool is an int subclass; in-range ints fall through the branch above.
    if isinstance(value, (int, float, bytes, datetime)) or value is None:
        return value
    return str(value)


def _bson_key_safe(key: Any) -> str:
    """BSON field names must be NUL-free strings. json.loads always yields
    str keys but lets a "\\u0000" escape through, and Python-side tool
    handlers can key dicts by int/None — either shape crashes insert_one."""
    key = _bson_text_safe(key)
    if not isinstance(key, str):
        key = str(key)
    return key.replace("\x00", "\\x00")


class MessageStore:
    """
    Append-only message log with atomic sequence numbers.
    Single source of truth for all project communication.
    """
    
    def __init__(self, db):
        """
        Args:
            db: MongoStorageBackend instance
        """
        self.db = db
        self._messages_collection = None
        self._sequences_collection = None
    
    @property
    def messages(self):
        """Lazy access to messages collection."""
        if self._messages_collection is None:
            self._messages_collection = self.db.db.messages
        return self._messages_collection
    
    @property
    def sequences(self):
        """Lazy access to message_sequences collection."""
        if self._sequences_collection is None:
            self._sequences_collection = self.db.db.message_sequences
        return self._sequences_collection
    
    async def initialize(self):
        """Create indexes for the messages collection."""
        logger.info("Creating message store indexes...")
        
        # Messages indexes
        await self.messages.create_index(
            [("project_id", 1), ("sequence", 1)],
            unique=True
        )
        await self.messages.create_index([("project_id", 1), ("created_at", -1)])
        await self.messages.create_index([("project_id", 1), ("type", 1)])
        await self.messages.create_index("id", unique=True)
        
        # Sequences indexes
        await self.sequences.create_index("project_id", unique=True)
        
        logger.info("✅ Message store indexes created")
    
    async def append(
        self,
        project_id: str,
        message: Dict[str, Any],
        run_id: Optional[str] = None,
        session=None
    ) -> Dict[str, Any]:
        """
        Append message with atomic sequence increment.
        
        Args:
            project_id: Project ID
            message: Message data (type, content, subtype, data, metadata)
            run_id: Optional run ID for scoping
        
        Returns:
            The complete message with id, sequence, and created_at
        """
        # Atomic increment using MongoDB $inc
        seq_doc = await self.sequences.find_one_and_update(
            {"project_id": project_id},
            {"$inc": {"sequence": 1}},
            upsert=True,
            return_document=True,
            session=session
        )
        
        sequence = seq_doc["sequence"]
        message_id = str(uuid.uuid4())
        # Emit a TZ-aware ISO string (e.g. "2026-05-06T14:38:05.123+00:00").
        # JS `new Date(s)` parses naive ISO strings as LOCAL time, which made
        # the chat order user messages before earlier agent thoughts and
        # display them at the wrong time-of-day for users not in UTC. With an
        # explicit offset, browsers parse correctly and `toLocaleTimeString`
        # converts to the viewer's TZ.
        created_at = datetime.now(timezone.utc).isoformat()
        
        full_message = {
            "id": message_id,
            "project_id": project_id,
            "sequence": sequence,
            "created_at": created_at,
            "type": message.get("type", "system"),
            "subtype": message.get("subtype"),
            "content": _bson_text_safe(message.get("content")),
            "data": _bson_text_safe({
                **(message.get("data") or {}),
                **(message.get("metadata") or {})  # Merge metadata into data
            }) if message.get("data") or message.get("metadata") else None,
            "status": message.get("status"),
        }
        
        # Add run_id if provided
        if run_id:
            full_message["run_id"] = run_id
        
        await self.messages.insert_one(full_message, session=session)
        
        # Remove MongoDB _id before returning
        full_message.pop("_id", None)
        
        logger.debug(f"Appended message {message_id} (seq={sequence}) to project {project_id}")
        return full_message
    
    async def get_messages(
        self,
        project_id: str,
        after_sequence: int = 0,
        run_id: Optional[str] = None,
        limit: int = 1000,
        exclude_types: Optional[List[str]] = None,
        only_types: Optional[List[str]] = None,
        tail: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Get messages in sequence order, optionally after a sequence number.

        Args:
            project_id: Project ID
            after_sequence: Only return messages after this sequence number
            run_id: Optional run ID filter
            limit: Maximum messages to return
            exclude_types: Message types to filter out server-side (e.g.
                TOOL_LEDGER_TYPES), so they don't consume the limit window
            only_types: Return only these message types — the deliberate
                journal fetch (ADR-0008); mutually exclusive with
                exclude_types
            tail: Return the newest `limit` records instead of the oldest
                (result stays ascending) — for recent-conversation readers,
                where oldest-first limiting returns the stale head of a long
                history

        Returns:
            List of messages sorted by sequence
        """
        if exclude_types and only_types:
            raise ValueError("exclude_types and only_types are mutually exclusive")

        query = {
            "project_id": project_id,
            "sequence": {"$gt": after_sequence}
        }

        if exclude_types:
            query["type"] = {"$nin": list(exclude_types)}
        elif only_types:
            query["type"] = {"$in": list(only_types)}

        if run_id:
            # Project-lineage messages without run_id (initial user prompt written
            # before any run is active, gate-response user messages) belong to the
            # whole project, not a specific run — include them in every run's view.
            # See docs/bug-chat-first-message-hidden.md.
            query["$or"] = [{"run_id": run_id}, {"run_id": {"$exists": False}}]
        
        cursor = self.messages.find(
            query,
            {"_id": 0}  # Exclude MongoDB _id
        ).sort("sequence", -1 if tail else 1).limit(limit)

        docs = await cursor.to_list(length=limit)
        if tail:
            docs.reverse()
        return docs

    async def list_trace_messages(
        self,
        project_id: str,
        run_id: Optional[str] = None,
        limit: int = 10000,
        include_payloads: bool = False,
        include_unscoped: bool = False,
    ) -> Dict[str, Any]:
        query: Dict[str, Any] = {"project_id": project_id, "type": {"$in": list(TOOL_LEDGER_TYPES) + ["approval"]}}
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
        projection = {"_id": 0, "id": 1, "project_id": 1, "run_id": 1, "sequence": 1, "created_at": 1, "type": 1, "subtype": 1, "status": 1,
                      "data.run_id": 1, "data.tool_call_id": 1, "data.task_id": 1, "data.agent_id": 1, "data.workflow_node_id": 1,
                      "data.agent_display_name": 1,
                      "data.name": 1, "data.approval_id": 1, "data.approval_status": 1,
                      "data.gate_node_id": 1, "data.refine_target_node_id": 1, "data.phase": 1}
        cursor = self.messages.find(query, projection).sort([("sequence", -1), ("id", -1)]).limit(limit + 1)
        rows = await cursor.to_list(length=limit + 1)
        capped = len(rows) > limit
        rows = rows[:limit]
        rows.reverse()
        return {"items": rows, "limit_reached": capped}

    async def list_trace_agent_results(
        self,
        project_id: str,
        run_id: Optional[str] = None,
        limit: int = 5000,
        include_unscoped: bool = False,
    ) -> Dict[str, Any]:
        """Return compact assistant-result metadata without its content."""
        query: Dict[str, Any] = {"project_id": project_id, "type": "assistant"}
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
        projection = {
            "_id": 0, "id": 1, "project_id": 1, "run_id": 1, "sequence": 1,
            "created_at": 1, "timestamp": 1, "data.run_id": 1, "data.task_id": 1,
            "data.agent_id": 1, "data.attempt": 1,
        }
        cursor = self.messages.find(query, projection).sort([("sequence", -1), ("id", -1)]).limit(limit + 1)
        rows = await cursor.to_list(length=limit + 1)
        capped = len(rows) > limit
        rows = rows[:limit]
        rows.reverse()
        return {"items": rows, "limit_reached": capped}

    async def get_trace_input_message(self, project_id: str) -> Optional[Dict[str, Any]]:
        """Return the earliest project user message as a bounded-trace fallback."""
        return await self.messages.find_one(
            {"project_id": project_id, "type": "user"},
            {"_id": 0, "id": 1, "sequence": 1, "created_at": 1, "content": 1},
            sort=[("sequence", 1), ("id", 1)],
        )

    async def get_trace_agent_result(self, project_id: str, message_id: str) -> Optional[Dict[str, Any]]:
        """Read one assistant result after the project authorization boundary."""
        return await self.messages.find_one(
            {"project_id": project_id, "id": message_id, "type": "assistant"},
            {"_id": 0, "id": 1, "content": 1, "created_at": 1, "sequence": 1},
        )

    async def get_trace_payloads(self, project_id: str, message_ids: List[str]) -> List[Dict[str, Any]]:
        if not message_ids:
            return []
        projection = {"_id": 0, "id": 1, "type": 1, "run_id": 1, "sequence": 1, "data": 1}
        cursor = self.messages.find({"project_id": project_id, "id": {"$in": message_ids}, "type": {"$in": list(TOOL_LEDGER_TYPES)}}, projection).sort("sequence", 1)
        return await cursor.to_list(length=len(message_ids))
    
    async def get_tool_records_for_call(
        self, project_id: str, tool_call_id: str
    ) -> List[Dict[str, Any]]:
        """All journal records for one tool_call_id, ascending by sequence.

        The targeted read behind answering a question: the route needs one
        call's pair state, not a journal window (a provider may reuse a call
        id across rounds, so callers get every occurrence and apply the
        earliest-open pairing rule themselves).
        """
        cursor = self.messages.find(
            {
                "project_id": project_id,
                "type": {"$in": list(TOOL_LEDGER_TYPES)},
                "data.tool_call_id": tool_call_id,
            },
            {"_id": 0},
        ).sort("sequence", 1)
        return await cursor.to_list(length=None)

    async def get_latest_sequence(self, project_id: str) -> int:
        """Get the latest sequence number for a project."""
        doc = await self.sequences.find_one({"project_id": project_id})
        return doc["sequence"] if doc else 0

    async def claim_next_sequence(self, project_id: str) -> int:
        """
        Atomically claim the next sequence slot without appending a message.

        Mirrors append()'s $inc claim so file stamps share one coordinate
        space with message stamps. Returns a slot strictly greater than
        every record (message or file) that existed at the moment of the
        call; later message appends will receive higher slots.
        """
        seq_doc = await self.sequences.find_one_and_update(
            {"project_id": project_id},
            {"$inc": {"sequence": 1}},
            upsert=True,
            return_document=True,
        )
        return seq_doc["sequence"]

    async def get_message_by_id(self, message_id: str) -> Optional[Dict[str, Any]]:
        """Get a single message by ID."""
        doc = await self.messages.find_one(
            {"id": message_id},
            {"_id": 0}
        )
        return doc

    async def delete_message_in_project(self, project_id: str, message_id: str) -> bool:
        """Drop one message. Does not rewind the sequence counter (a hole is fine)."""
        if not project_id or not message_id:
            return False
        result = await self.messages.delete_one({"id": message_id, "project_id": project_id})
        return result.deleted_count > 0
    
    async def get_pending_approval(
        self,
        project_id: str,
        run_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get the current pending approval message for a project.
        
        Returns the most recent approval message with status='pending'.
        """
        query = {
            "project_id": project_id,
            "type": "approval",
            "status": "pending"
        }

        if run_id:
            query["$or"] = [{"run_id": run_id}, {"run_id": {"$exists": False}}]
        
        cursor = self.messages.find(
            query,
            {"_id": 0}
        ).sort("sequence", -1).limit(1)
        
        results = await cursor.to_list(length=1)
        return results[0] if results else None
    
    async def update_message_status(
        self,
        message_id: str,
        status: str,
        metadata_updates: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Update the status of a message (e.g., approval status).
        
        Note: This doesn't break append-only semantics as it only updates
        status fields, not content. For full audit trail, append a new
        system message recording the status change.
        
        Args:
            message_id: Message ID
            status: New status
            metadata_updates: Optional metadata to merge
        
        Returns:
            True if message was found and updated
        """
        update = {"$set": {"status": status}}
        
        if metadata_updates:
            for key, value in metadata_updates.items():
                update["$set"][f"metadata.{key}"] = value
        
        result = await self.messages.update_one(
            {"id": message_id},
            update
        )
        
        return result.modified_count > 0

    async def update_approval_status_by_approval_id(
        self,
        approval_id: str,
        status: str
    ) -> Optional[Dict[str, Any]]:
        """
        Update the status of an approval message by its approval_id.
        
        NOTE: approval_id is stored in data.approval_id (single source of truth).
        
        Args:
            approval_id: The approval_id stored in data.approval_id
            status: New status (e.g., 'superseded', 'approved', 'rejected')
        
        Returns:
            The updated message document, or None if not found
        """
        doc = await self.messages.find_one_and_update(
            {"data.approval_id": approval_id, "type": "approval"},
            {"$set": {"status": status}},
            return_document=True  # Return the updated document
        )
        if doc:
            doc.pop("_id", None)
        return doc

    async def resolve_approval_by_approval_id(
        self,
        approval_id: str,
        status: str,
        resolution: Optional[Dict[str, Any]] = None,
        *,
        expected_data: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Atomically resolve a still-pending approval and store its decision."""
        updates: Dict[str, Any] = {"status": status}
        if isinstance(resolution, dict):
            updates["data.resolution"] = resolution
        query: Dict[str, Any] = {
            "data.approval_id": approval_id,
            "type": "approval",
            "status": "pending",
        }
        if expected_data is not None:
            query["data"] = expected_data
        doc = await self.messages.find_one_and_update(
            query,
            {"$set": updates},
            return_document=True,
        )
        if doc:
            doc.pop("_id", None)
        return doc

    async def update_approval_data_by_approval_id(
        self,
        approval_id: str,
        new_data: Dict[str, Any],
        *,
        project_id: str,
        run_id: Optional[str],
        gate_node_id: str,
        expected_data: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """
        Replace the `data` field of an approval message identified by its
        approval_id. Used by the refine flow (`_refine_approval_core`) when
        the user's feedback regenerates the approval payload but the
        approval_id (and the FSM polling it) must stay the same — see
        api/routes/approvals.py for the wider contract.

        approval_id is preserved inside data even if the caller forgot to
        include it: the FE keys off `data.approval_id` and dropping it would
        orphan the row from the resolve endpoint.
        """
        merged = dict(new_data or {})
        merged["approval_id"] = approval_id
        doc = await self.messages.find_one_and_update(
            {
                "data.approval_id": approval_id,
                "data.gate_node_id": gate_node_id,
                "type": "approval",
                "status": "pending",
                "project_id": project_id,
                "run_id": run_id,
                "data": expected_data,
            },
            {"$set": {"data": merged}},
            return_document=True,
        )
        if doc:
            doc.pop("_id", None)
        return doc

    async def get_approval_by_id(self, approval_id: str) -> Optional[Dict[str, Any]]:
        """
        Get an approval message by its approval_id (stored in data.approval_id).
        
        Returns the approval message dict or None if not found.
        """
        doc = await self.messages.find_one(
            {"data.approval_id": approval_id, "type": "approval"},
            {"_id": 0}
        )
        if not doc:
            return None
        # Add gate_type for UI compatibility (uses subtype as gate_type)
        if doc.get("subtype") and not doc.get("gate_type"):
            doc["gate_type"] = doc["subtype"]
        return doc

    async def get_pending_approvals_for_project(
        self,
        project_id: str,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get all pending approval messages for a project.
        
        Returns approval messages formatted for UI compatibility:
        - approval_id: from data.approval_id
        - gate_type: from subtype
        - data: approval data
        - status: approval status
        """
        cursor = self.messages.find(
            {"project_id": project_id, "type": "approval", "status": "pending"},
            {"_id": 0}
        ).sort("sequence", 1).limit(limit)
        docs = await cursor.to_list(length=limit)
        
        # Format for UI compatibility
        result = []
        for doc in docs:
            # Add gate_type for UI (uses subtype)
            if doc.get("subtype") and not doc.get("gate_type"):
                doc["gate_type"] = doc["subtype"]
            # Extract approval_id from data for top-level access
            if doc.get("data", {}).get("approval_id") and not doc.get("approval_id"):
                doc["approval_id"] = doc["data"]["approval_id"]
            result.append(doc)
        return result

    async def cancel_pending_approvals_for_project(
        self,
        project_id: str
    ) -> int:
        """
        Cancel all pending approvals for a project (set status to 'cancelled').
        Returns the number of approvals cancelled.
        """
        result = await self.messages.update_many(
            {"project_id": project_id, "type": "approval", "status": "pending"},
            {"$set": {"status": "cancelled"}}
        )
        return result.modified_count

    async def supersede_pending_approval(
        self,
        project_id: str,
        subtype: str
    ) -> bool:
        """
        Mark all pending approval messages of a given subtype as superseded.
        Used when a new approval replaces an old one (e.g., refined plan).
        
        Args:
            project_id: Project ID
            subtype: Approval subtype (requirements, plan, output, deploy)
        
        Returns:
            True if any messages were updated
        """
        result = await self.messages.update_many(
            {
                "project_id": project_id,
                "type": "approval",
                "subtype": subtype,
                "status": "pending"
            },
            {"$set": {"status": "superseded"}}
        )
        return result.modified_count > 0
    
    async def append_user_message(
        self,
        project_id: str,
        content: str,
        run_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Helper to append a user message."""
        return await self.append(project_id, {
            "type": "user",
            "content": content,
            "metadata": metadata or {}
        }, run_id=run_id)
    
    async def append_assistant_message(
        self,
        project_id: str,
        content: str,
        run_id: Optional[str] = None,
        phase: Optional[str] = None,
        agent_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Helper to append an assistant message."""
        msg_metadata = metadata or {}
        if phase:
            msg_metadata["phase"] = phase
        if agent_id:
            msg_metadata["agent_id"] = agent_id
        
        return await self.append(project_id, {
            "type": "assistant",
            "content": content,
            "metadata": msg_metadata
        }, run_id=run_id)
    
    async def append_approval(
        self,
        project_id: str,
        subtype: str,
        data: Dict[str, Any],
        run_id: Optional[str] = None,
        content: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Helper to append an approval message.
        
        Args:
            project_id: Project ID
            subtype: Approval type (requirements, plan, output, deploy)
            data: The approval data (requirements dict, plan dict, etc.)
            run_id: Optional run ID
            content: Optional description
            metadata: Optional metadata
        
        Returns:
            The created approval message
        """
        return await self.append(project_id, {
            "type": "approval",
            "subtype": subtype,
            "status": "pending",
            "data": data,
            "content": content,
            "metadata": metadata or {}
        }, run_id=run_id)
    
    async def append_system_message(
        self,
        project_id: str,
        subtype: str,
        content: str,
        run_id: Optional[str] = None,
        data: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Helper to append a system message.
        
        Args:
            subtype: System message type (phase_started, phase_completed, 
                     approval_result, error, etc.)
        """
        return await self.append(project_id, {
            "type": "system",
            "subtype": subtype,
            "content": content,
            "data": data,
            "metadata": metadata or {}
        }, run_id=run_id)
    
    async def append_event(
        self,
        project_id: str,
        subtype: str,
        content: str,
        data: Dict[str, Any],
        run_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Helper to append an event message (task progress, tool execution)."""
        return await self.append(project_id, {
            "type": "event",
            "subtype": subtype,
            "content": content,
            "data": data,
            "metadata": metadata or {}
        }, run_id=run_id)
    
    async def append_tool_call(
        self,
        project_id: str,
        tool_call_id: str,
        name: str,
        arguments: str,
        run_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        task_id: Optional[str] = None,
        pending_input: Optional[Dict[str, Any]] = None,
        workflow_node_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Persist an LLM tool call before it executes (tool ledger).

        ``arguments`` is the raw string exactly as the LLM produced it (it may
        not even be valid JSON) — replaying a run rebuilds the LLM's
        function_call input items from this field verbatim, so parsing here
        would lose fidelity. ``pending_input`` is a forward-compat slot
        ({origin, routing_key}) for direct answer routing; nothing populates
        it yet. ``workflow_node_id`` lets restart recovery map an interrupted
        attempt back to the workflow node to re-enter; records written before
        this field existed simply can't auto-resume.
        """
        return await self.append(project_id, {
            "type": "tool_call",
            "data": {
                "tool_call_id": tool_call_id,
                "name": name,
                "arguments": arguments,
                "agent_id": agent_id,
                "task_id": task_id,
                "pending_input": pending_input,
                "workflow_node_id": workflow_node_id,
            },
        }, run_id=run_id)

    async def append_tool_result(
        self,
        project_id: str,
        tool_call_id: str,
        name: str,
        result: Any,
        status: str,
        run_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        task_id: Optional[str] = None,
        session=None
    ) -> Dict[str, Any]:
        """Persist a tool outcome, closing the pair opened by append_tool_call.

        ``result`` is expected to be pre-bounded by the caller's tool boundary
        (oversized payloads were already swapped for an archive_ref placeholder
        by ArchiveStore.maybe_spill) so the document stays far below Mongo's
        16 MB cap. ``status`` is "ok" or "error".
        """
        return await self.append(project_id, {
            "type": "tool_result",
            "status": status,
            "data": {
                "tool_call_id": tool_call_id,
                "name": name,
                "result": result,
                "agent_id": agent_id,
                "task_id": task_id,
            },
        }, run_id=run_id, session=session)

    async def reset_sequence(self, project_id: str, new_sequence: int = 0) -> None:
        """
        Reset sequence number for a project (used in revert scenarios).

        WARNING: This should only be used during revert operations.
        """
        await self.sequences.update_one(
            {"project_id": project_id},
            {"$set": {"sequence": new_sequence}},
            upsert=True
        )
    
    async def delete_messages_after_sequence(
        self,
        project_id: str,
        after_sequence: int,
        run_id: Optional[str] = None,
        include_target: bool = False
    ) -> int:
        """
        Delete messages after a given sequence number (for revert).
        
        Args:
            project_id: Project ID
            after_sequence: Delete messages with sequence > this value (or >= if include_target)
            run_id: Optional run ID filter
            include_target: If True, delete messages with sequence >= after_sequence
        
        Returns:
            Number of messages deleted
        """
        seq_op = "$gte" if include_target else "$gt"
        query = {
            "project_id": project_id,
            "sequence": {seq_op: after_sequence}
        }
        
        if run_id:
            query["run_id"] = run_id

        await self._sweep_reverted_attachments(project_id, dict(query), run_id=run_id)

        result = await self.messages.delete_many(query)
        
        # Update sequence counter (if including target, reset to target-1)
        new_seq = after_sequence - 1 if include_target else after_sequence
        await self.reset_sequence(project_id, new_seq)

        # Revert sweep (AppFactory-149, ADR-0014): drop rolling-summary versions that
        # cover now-deleted messages. new_seq is the surviving max sequence, so a
        # version whose covered range runs past it is stale. This lives at the one
        # delete choke point every revert path funnels through (revert_manager and
        # shared_context.truncate_conversation both land here), so no path can
        # rewind messages yet leave a summary of them behind. The store is a
        # sibling on the backend; if it is absent (older fakes) or the sweep fails,
        # the revert still succeeds — a stale summary is recoverable and gets
        # re-folded, a failed revert is not.
        rolling_summary_store = getattr(self.db, "rolling_summary_store", None)
        if rolling_summary_store is not None:
            try:
                await rolling_summary_store.delete_versions_after_sequence(project_id, new_seq)
            except Exception as exc:
                logger.warning(
                    "[REVERT] rolling-summary sweep failed for %s at seq %s: %s",
                    project_id, new_seq, exc,
                )

        logger.info(f"Deleted {result.deleted_count} messages {'from' if include_target else 'after'} sequence {after_sequence}")
        return result.deleted_count

    async def _sweep_reverted_attachments(
        self,
        project_id: str,
        query: dict,
        run_id: Optional[str] = None,
    ) -> None:
        """Drop user attachments for messages about to be deleted on revert."""
        user_attachments = getattr(self.db, "user_attachments", None)
        if user_attachments is None:
            return
        try:
            cursor = self.messages.find(query, {"id": 1})
            message_ids = [doc.get("id") async for doc in cursor if doc.get("id")]
            if not message_ids:
                return
            tenant_id = None
            if hasattr(self.db, "load_project"):
                project = await self.db.load_project(project_id)
                tenant_id = (project or {}).get("tenant_id")
            if not tenant_id:
                logger.warning("[ATTACH] revert sweep skipped project=%s — no tenant_id", project_id)
                return
            from storage.file_attachment_store import FileAttachmentStore
            from storage.file_blob_store import FileBlobStore
            from api.routes.file_attachment_events import emit_attachment_deleted

            removed = await FileAttachmentStore(self.db, FileBlobStore.from_env()).delete_user_attachments_for_message_ids(
                tenant_id=str(tenant_id),
                project_id=project_id,
                message_ids=message_ids,
            )
            for doc in removed:
                await emit_attachment_deleted(
                    project_id=project_id,
                    tenant_id=str(tenant_id),
                    doc=doc,
                    run_id=run_id,
                    reason="revert",
                )
        except Exception as exc:
            logger.warning("[ATTACH] revert attachment sweep failed project=%s: %s", project_id, exc)
