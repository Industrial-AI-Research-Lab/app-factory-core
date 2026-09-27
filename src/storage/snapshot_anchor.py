"""Resolve a user_message snapshot's true checkpoint sequence from the messages.

Shared by the backfill migration (config.migrate_snapshot_conversation_index)
and the revert guard (orchestration.revert_manager): both need the same answer
to "which message sequence is this snapshot's checkpoint?" — the highest
type="user" message at or before the snapshot's created_at. Pure, no I/O, so
both the one-off migration and the runtime revert path can depend on it.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, Optional


def to_naive_utc(value: Any) -> Optional[datetime]:
    """Coerce a stored timestamp to a naive-UTC datetime for comparison.

    Messages store created_at as an ISO string ("...+00:00"), snapshots as a
    BSON date (decoded to a datetime, naive or aware depending on the client).
    Returns None for anything unparseable so the caller can skip it.
    """
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def snapshot_boundary(snapshot: dict) -> Any:
    """The moment a snapshot's checkpoint was taken, for anchoring the revert.

    Prefer meta.checkpoint_at over created_at. created_at is stamped when the
    snapshot row is saved, which is AFTER SnapshotManager.create_snapshot runs a
    git commit that can take tens of seconds; a user message typed during that
    window lands before created_at and inflates the anchor past the real
    checkpoint (false stale-index refuse; migration over-repair). checkpoint_at
    is captured before the commit, so it bounds the anchor at the checkpoint.
    Snapshots written before checkpoint_at existed fall back to created_at.
    """
    meta = snapshot.get("meta") or {}
    return meta.get("checkpoint_at") or snapshot.get("created_at")


def compute_target_sequence(
    boundary_ts: Any, messages: Iterable[dict]
) -> Optional[int]:
    """Highest user-message sequence at or before the boundary, or 0 if none.

    boundary_ts is the snapshot's checkpoint moment — pass snapshot_boundary(),
    not created_at directly, so a slow-commit gap doesn't inflate the result.
    Only type=="user" messages are candidates: the producer records the
    sequence of the user/approval message appended just before the snapshot,
    while the approval_result receipt lands after it (higher sequence, later
    timestamp). Restricting to user messages is what keeps a wiped run — where
    the anchor is gone and only orphan receipts survive — from resolving to a
    receipt's sequence; it yields 0 there, which the caller treats as
    unrepairable. Returns None when the boundary timestamp can't be parsed.
    """
    boundary_dt = to_naive_utc(boundary_ts)
    if boundary_dt is None:
        return None
    best = 0
    for msg in messages:
        if msg.get("type") != "user":
            continue
        seq = msg.get("sequence")
        if not isinstance(seq, int):
            continue
        msg_dt = to_naive_utc(msg.get("created_at"))
        if msg_dt is None or msg_dt > boundary_dt:
            continue
        if seq > best:
            best = seq
    return best
