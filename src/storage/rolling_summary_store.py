"""Rolling Summary store — versioned, derived compaction state (AppFactory-149, ADR-0014).

A separate collection, deliberately NOT message documents: the message log stays
the pure source of truth, and the ~8 conversation readers are spared a
summary-typed record each would have to learn to exclude. Every version covers a
contiguous message-sequence range and is produced by folding the previous version
forward (Decision 3) — this store keeps no opinion on how the text is produced, only
on how versions are ordered, retrieved, and swept.

Revert (ADR-0007) drops versions whose covered range runs past the rewind point;
``delete_versions_after_sequence`` is that sweep and is project-scoped on purpose —
revert deletes messages across every run from the target sequence, so the summaries
covering them must go too, regardless of which run produced them.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class RollingSummaryStore:
    def __init__(self, db):
        """Args: db — MongoStorageBackend (same handle MessageStore takes)."""
        self.db = db
        self._collection = None

    @property
    def summaries(self):
        if self._collection is None:
            self._collection = self.db.db.rolling_summaries
        return self._collection

    async def initialize(self) -> None:
        # Unique per (project, version): the fold derives the next version from
        # the current latest, so two runs folding the same project at once must
        # collide here rather than both store a "version 3" the reader can't
        # order. run_id is deliberately NOT in this key — a version's identity is
        # the project and its number (ADR-0014 Decision 4); run_id is provenance
        # only, and keying on it would let concurrent runs write rival version-3
        # rows, the exact ambiguity this unique index exists to prevent.
        await self.summaries.create_index(
            [("project_id", 1), ("version", 1)], unique=True
        )
        # The revert sweep filters on covers_to_sequence within a project.
        await self.summaries.create_index(
            [("project_id", 1), ("covers_to_sequence", 1)]
        )

    async def get_latest(self, project_id: str) -> Optional[Dict[str, Any]]:
        """Highest-version summary for the project — the one the fold extends.

        A project is a single linear message stream: revert hard-deletes the tail
        (ADR-0007), it never branches, so one project has exactly one summary
        chain. run_id is not a filter here on purpose — a later run must fold
        forward from the prior run's summary, not start a fresh version 1 and
        re-read the whole past.
        """
        cursor = (
            self.summaries.find({"project_id": project_id}, {"_id": 0})
            .sort("version", -1)
            .limit(1)
        )
        docs = await cursor.to_list(length=1)
        return docs[0] if docs else None

    async def append_version(
        self,
        project_id: str,
        *,
        summary: str,
        covers_from_sequence: int,
        covers_to_sequence: int,
        conversation_covers_to_sequence: int = 0,
        run_id: Optional[str] = None,
        meta: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Persist the next fold version for the project; version = prev + 1.

        The version is derived from the current latest for the project, so after a
        revert has dropped newer versions the counter naturally resumes from the
        surviving max — no reset needed. ``run_id`` is stored for provenance (which
        run produced this fold) but is not part of version identity.

        Two frontiers, deliberately separate (AppFactory-149 slice 5):
          - ``covers_to_sequence`` — highest MESSAGE sequence this version's content
            represents (conversation + trajectory). The revert sweep keys on it, so
            it must move to the true high-water whenever trajectory is folded, or a
            revert past that trajectory would keep a now-stale summary.
          - ``conversation_covers_to_sequence`` — highest CONVERSATION sequence folded
            in. The seeder skips conversation at or below it and re-seeds everything
            above it verbatim, so it must NOT over-claim: a trajectory-only fold
            (the plugin) carries the prior value forward rather than advancing it to
            the high-water, otherwise recent verbatim turns would be filtered out
            next run yet never made it into the summary — silent loss.
        """
        latest = await self.get_latest(project_id)
        version = (latest["version"] + 1) if latest else 1
        doc = {
            "id": str(uuid.uuid4()),
            "project_id": project_id,
            "run_id": run_id,
            "version": version,
            "covers_from_sequence": covers_from_sequence,
            "covers_to_sequence": covers_to_sequence,
            "conversation_covers_to_sequence": conversation_covers_to_sequence,
            "summary": summary,
            "meta": meta or {},
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        # Copy so the driver stamping _id onto the inserted dict doesn't leak a
        # BSON ObjectId back to the caller.
        await self.summaries.insert_one(dict(doc))
        return doc

    async def delete_versions_after_sequence(
        self, project_id: str, sequence: int
    ) -> int:
        """Drop every version whose covered range extends past ``sequence``.

        Project-scoped, not run-scoped: revert removes messages from every run at
        or after the target, so a summary covering any of them is now stale. A
        version whose covers_to_sequence is exactly ``sequence`` still holds (it
        summarizes only surviving messages) and is kept.
        """
        result = await self.summaries.delete_many(
            {"project_id": project_id, "covers_to_sequence": {"$gt": sequence}}
        )
        return getattr(result, "deleted_count", 0)
