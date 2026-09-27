"""
Artifact Store - Full File Content Persistence

Stores all files created by agents in MongoDB for container recovery.
Every file write → immediate DB sync. Zero data loss.

Schema:
{
    "project_id": "uuid",
    "run_id": "uuid" (optional),
    "path": "src/main.py",
    "content": "...",
    "version": 3,                   # in-place counter on overwrite; NOT history
    "checkpoint_sequence": 42,      # atomic slot claimed from MessageStore — see save_file
    "created_at": datetime,
    "updated_at": datetime,
    "deleted_at": datetime,         # soft-delete marker — set by delete_file / delete_all_files
}

Delete paths are asymmetric:
- delete_file / delete_all_files: soft-delete ($set deleted_at).
- restore_files_to_checkpoint: HARD delete (delete_many) on records with
  checkpoint_sequence > target. No reader queries soft-deleted file records,
  so on revert the soft-delete marker is pure data ballast.
"""

from typing import Dict, Any, Optional, List
from datetime import datetime
import logging

from storage.archive_store import HARD_SPILL_CEILING_BYTES

logger = logging.getLogger(__name__)


def _spill_fields(doc: Dict[str, Any]) -> Dict[str, Any]:
    """The locator view of a possibly-spilled artifact. `spilled` is False and the
    rest None for a normal inline record — and for legacy records that predate
    spilling — so every reader exposes the same shape and recovery can tell which
    files it must hydrate from object storage."""
    return {
        "spilled": bool(doc.get("spilled")),
        "archive_ref_id": doc.get("archive_ref_id"),
        "size_bytes": doc.get("size_bytes"),
        "content_type": doc.get("content_type"),
    }


class ArtifactStore:
    """
    Manages file artifact persistence in MongoDB.
    
    DB-first: Every write goes to DB immediately.
    Enables container recovery by restoring all files from DB.
    """
    
    def __init__(self, storage_backend, message_store=None, archive_store=None):
        """
        Args:
            storage_backend: MongoStorageBackend instance
            message_store: MessageStore for atomically claiming the next
                position slot on save_file writes. Required for any
                construction that intends to call save_file — read-only
                constructions (deploy build, stack detection, dispatcher
                inspection) may omit it. save_file refuses to stamp the
                pre-fix `0` sentinel at runtime; that value is reserved
                for legacy records produced by the migration backfill.
            archive_store: ArchiveStore used to spill a file whose content would
                approach Mongo's 16 MB per-document cap into object storage.
                Omit it and every file is stored inline (the pre-spill behaviour).
        """
        self.storage = storage_backend
        self.message_store = message_store
        self.archive_store = archive_store
        self.db = None
        self.collection = None
        # A project's tenant never changes, so this needs no expiry; it keeps the
        # per-save spill path from re-reading the project just to scope the blob.
        self._project_tenant_cache: Dict[str, Optional[str]] = {}
    
    async def initialize(self):
        """Initialize the artifact store and create indexes."""
        if self.storage is None or self.storage.db is None:
            logger.warning("Storage backend not initialized")
            return
        
        self.db = self.storage.db
        self.collection = self.db.file_artifacts
        
        # Create indexes for efficient queries
        await self.collection.create_index(
            [("project_id", 1), ("path", 1)],
            unique=False  # Allow multiple versions
        )
        await self.collection.create_index(
            [("project_id", 1), ("path", 1), ("deleted_at", 1)],
        )
        await self.collection.create_index(
            [("project_id", 1), ("run_id", 1)],
        )
        await self.collection.create_index("updated_at")
        
        logger.info("✅ ArtifactStore initialized")
    
    async def _resolve_tenant(self, project_id: str) -> Optional[str]:
        """The project's tenant_id (cached), for scoping a spilled file's blob and
        resolving that tenant's spill threshold. None when it can't be read — the
        spill still works, it just lands in the shared key-space."""
        if project_id in self._project_tenant_cache:
            return self._project_tenant_cache[project_id]
        tenant_id = None
        try:
            proj = await self.storage.projects.find_one(
                {"project_id": project_id}, {"tenant_id": 1}
            )
            tenant_id = (proj or {}).get("tenant_id")
        except Exception as exc:
            logger.warning("Could not resolve tenant for %s: %s", project_id, exc)
        self._project_tenant_cache[project_id] = tenant_id
        return tenant_id

    async def save_file(
        self,
        project_id: str,
        path: str,
        content: str,
        run_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Save or update a file artifact.
        
        Uses upsert to create or update the file.
        Increments version on each update.
        
        Args:
            project_id: Project identifier
            path: File path (relative to workdir)
            content: Full file content
            run_id: Optional run identifier
            
        Returns:
            Dict with artifact info including version
        """
        if self.collection is None:
            logger.warning("ArtifactStore not initialized, skipping save")
            return {"status": "skipped", "reason": "not_initialized"}

        now = datetime.utcnow()

        # Inline vs spill, decided by byte size. A file over the tenant's
        # threshold goes to object storage as an archive_ref and the document
        # keeps only a locator — otherwise a large .geojson/.html would breach
        # Mongo's 16 MB per-document cap and fail the write outright. Spill fields
        # are set in BOTH branches so overwriting a spilled file with a small one
        # (or vice versa) leaves no stale locator behind.
        content_bytes = (
            content.encode("utf-8", errors="replace")
            if isinstance(content, str) else bytes(content or b"")
        )
        size_bytes = len(content_bytes)
        spill_meta = None
        if self.archive_store is not None and size_bytes > 0:
            tenant_id = await self._resolve_tenant(project_id)
            if tenant_id in (None, "__default__"):
                tenant_id = "__root__"
            threshold = await self.archive_store.resolve_threshold_bytes(tenant_id)
            if size_bytes > threshold:
                spill_meta = await self.archive_store.spill_file(
                    content_bytes, path=path, project_id=project_id,
                    run_id=run_id, tenant_id=tenant_id,
                )
                if spill_meta is None:
                    if size_bytes > HARD_SPILL_CEILING_BYTES:
                        # Spill failed and the file is too big to inline without
                        # breaching the 16 MiB document cap — inlining anyway is
                        # rejected, swallowed by the caller, and the file is lost.
                        logger.error(
                            "[ARTIFACT] %s/%s is %d B and spill failed — refusing inline write over the document cap",
                            project_id, path, size_bytes,
                        )
                        return {
                            "status": "error",
                            "reason": "spill_failed_oversized",
                            "path": path,
                            "size_bytes": size_bytes,
                        }
                    logger.warning(
                        "[ARTIFACT] %s/%s is %d B over threshold %d but spill failed — storing inline",
                        project_id, path, size_bytes, threshold,
                    )
        if spill_meta is not None:
            content_fields: Dict[str, Any] = {
                "content": None,
                "spilled": True,
                "archive_ref_id": spill_meta["ref_id"],
                "size_bytes": spill_meta["size_bytes"],
                "content_type": spill_meta.get("content_type"),
            }
        else:
            content_fields = {
                "content": content,
                "spilled": False,
                "archive_ref_id": None,
                "size_bytes": size_bytes,
                "content_type": None,
            }

        # Atomically claim the next position from the project's coordinate
        # (single counter shared with MessageStore.append). The claimed slot
        # is strictly greater than every record that existed at call time —
        # restore_files_to_checkpoint(target) with target < claimed slot
        # will sweep this file; revert symmetry holds even when other
        # coroutines are appending messages concurrently. The legacy read-
        # then-stamp pattern raced under cooperative scheduling and landed
        # files at stale positions. Sentinel `0` is no longer produced at
        # runtime; it remains reserved for legacy records written by the
        # migration backfill, preserved by any positive-target restore.
        if self.message_store is None:
            raise RuntimeError(
                f"ArtifactStore.save_file requires message_store to claim a "
                f"position slot (project_id={project_id} path={path}). "
                f"Refusing to stamp the pre-fix `0` sentinel at runtime."
            )
        checkpoint_sequence = await self.message_store.claim_next_sequence(project_id)

        # Find existing (non-deleted) artifact
        query = {
            "project_id": project_id,
            "path": path,
            "deleted_at": None,
        }

        existing = await self.collection.find_one(query)

        if existing:
            # Update existing artifact
            new_version = (existing.get("version") or 0) + 1
            await self.collection.update_one(
                {"_id": existing["_id"]},
                {
                    "$set": {
                        **content_fields,
                        "version": new_version,
                        "updated_at": now,
                        "run_id": run_id or existing.get("run_id"),
                        "checkpoint_sequence": checkpoint_sequence,
                    }
                }
            )
            logger.debug(f"Updated artifact: {project_id}/{path} v{new_version} seq={checkpoint_sequence}")
            return {
                "status": "updated",
                "project_id": project_id,
                "path": path,
                "version": new_version,
                "checkpoint_sequence": checkpoint_sequence,
                "spilled": spill_meta is not None,
            }
        else:
            # Create new artifact
            doc = {
                "project_id": project_id,
                "path": path,
                **content_fields,
                "version": 1,
                "run_id": run_id,
                "created_at": now,
                "updated_at": now,
                "deleted_at": None,
                "checkpoint_sequence": checkpoint_sequence,
            }
            await self.collection.insert_one(doc)
            logger.debug(f"Created artifact: {project_id}/{path} v1 seq={checkpoint_sequence}")
            return {
                "status": "created",
                "project_id": project_id,
                "path": path,
                "version": 1,
                "checkpoint_sequence": checkpoint_sequence,
                "spilled": spill_meta is not None,
            }
    
    async def get_file(
        self,
        project_id: str,
        path: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Get a file artifact by path.
        
        Returns the latest non-deleted version.
        """
        if self.collection is None:
            return None
        
        doc = await self.collection.find_one({
            "project_id": project_id,
            "path": path,
            "deleted_at": None,
        })
        
        if not doc:
            return None
        
        return {
            "project_id": doc["project_id"],
            "path": doc["path"],
            "content": doc.get("content"),
            "version": doc.get("version", 1),
            "run_id": doc.get("run_id"),
            "created_at": doc.get("created_at"),
            "updated_at": doc.get("updated_at"),
            **_spill_fields(doc),
        }

    async def get_all_files(
        self,
        project_id: str,
        run_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get all file artifacts for a project, deduplicated by path.
        
        Returns only the LATEST version of each file (by updated_at).
        Used for container recovery and deployment.
        
        Args:
            project_id: Project identifier
            run_id: Optional filter by run
            
        Returns:
            List of artifact dicts with path and content (latest version per path)
        """
        if self.collection is None:
            return []
        
        query = {
            "project_id": project_id,
            "deleted_at": None,
        }
        if run_id:
            query["run_id"] = run_id
        
        # Sort by updated_at descending so we can dedupe keeping the latest
        cursor = self.collection.find(query).sort("updated_at", -1)
        docs = await cursor.to_list(length=10000)
        
        # Deduplicate by path - keep only the LATEST version (first seen due to sort)
        seen_paths: set = set()
        result = []
        for doc in docs:
            path = doc.get("path", "")
            if path and path not in seen_paths:
                seen_paths.add(path)
                result.append({
                    "project_id": doc["project_id"],
                    "path": path,
                    "content": doc.get("content"),
                    "version": doc.get("version", 1),
                    "run_id": doc.get("run_id"),
                    "updated_at": doc.get("updated_at"),
                    **_spill_fields(doc),
                })

        return result

    async def list_trace_metadata(self, project_id: str, limit: int = 1000) -> Dict[str, Any]:
        """Return a bounded, content-free artifact view for Execution Trace.

        Trace needs to identify produced files, never their source content.
        Keeping this projection here prevents an aggregate read-model from
        accidentally fetching file bodies solely to render a filename.
        """
        if self.collection is None:
            return {"items": [], "limit_reached": False}
        cursor = self.collection.find(
            {"project_id": project_id, "deleted_at": None},
            {"_id": 0, "project_id": 1, "run_id": 1, "path": 1, "version": 1, "updated_at": 1},
        ).sort([("updated_at", -1), ("path", 1)]).limit(limit + 1)
        docs = await cursor.to_list(length=limit + 1)
        capped = len(docs) > limit
        return {
            "items": [{
                "project_id": doc.get("project_id"), "run_id": doc.get("run_id"), "path": doc.get("path"),
                "version": doc.get("version"), "updated_at": doc.get("updated_at"),
            } for doc in docs[:limit]],
            "limit_reached": capped,
        }
    
    async def delete_file(
        self,
        project_id: str,
        path: str,
    ) -> bool:
        """
        Soft-delete a file artifact.
        
        Returns True if file was deleted, False if not found.
        """
        if self.collection is None:
            return False
        
        result = await self.collection.update_one(
            {
                "project_id": project_id,
                "path": path,
                "deleted_at": None,
            },
            {
                "$set": {"deleted_at": datetime.utcnow()}
            }
        )
        
        return result.modified_count > 0
    
    async def delete_all_files(
        self,
        project_id: str,
        run_id: Optional[str] = None,
    ) -> int:
        """
        Soft-delete all file artifacts for a project.
        
        Returns count of deleted files.
        """
        if self.collection is None:
            return 0
        
        query = {
            "project_id": project_id,
            "deleted_at": None,
        }
        if run_id:
            query["run_id"] = run_id
        
        result = await self.collection.update_many(
            query,
            {"$set": {"deleted_at": datetime.utcnow()}}
        )
        
        return result.modified_count
    
    async def count_files(self, project_id: str) -> int:
        """Count non-deleted files for a project."""
        if self.collection is None:
            return 0
        
        return await self.collection.count_documents({
            "project_id": project_id,
            "deleted_at": None,
        })
    
    async def get_total_size(self, project_id: str) -> int:
        """Get total size of all file contents for a project."""
        if self.collection is None:
            return 0
        
        # A spilled file has no inline content; its byte size lives in size_bytes.
        pipeline = [
            {"$match": {"project_id": project_id, "deleted_at": None}},
            {"$project": {"size": {"$cond": [
                {"$eq": ["$spilled", True]},
                {"$ifNull": ["$size_bytes", 0]},
                {"$strLenBytes": {"$ifNull": ["$content", ""]}},
            ]}}},
            {"$group": {"_id": None, "total": {"$sum": "$size"}}},
        ]
        
        result = await self.collection.aggregate(pipeline).to_list(length=1)
        return result[0]["total"] if result else 0
    
    async def snapshot_from_artifacts(
        self,
        project_id: str,
        artifacts: List[Dict[str, Any]],
        run_id: Optional[str] = None,
        force: bool = False,
    ) -> Dict[str, Any]:
        """
        Snapshot files from a list of artifacts (from container/repo) to DB.
        
        Called when user input is awaited (approval gates) to persist
        current file state for recovery and history display.
        
        Args:
            project_id: Project identifier
            artifacts: List of {"path": str, "content": str} dicts
            run_id: Optional run identifier
            force: Skip confirmation check for large saves
            
        Returns:
            Dict with status, saved count, and confirmation_required flag
        """
        from config.artifacts import (
            should_include_file, 
            MAX_FILES_WITHOUT_CONFIRMATION,
            MAX_FILES_HARD_LIMIT,
        )
        
        if self.collection is None or not artifacts:
            return {"status": "skipped", "saved": 0, "reason": "no_artifacts"}
        
        # Filter artifacts using centralized config
        filtered = []
        for artifact in artifacts:
            path = artifact.get("path")
            content = artifact.get("content")
            if not path or content is None:
                continue
            if should_include_file(path):
                filtered.append(artifact)
        
        # Check if confirmation is needed for large saves
        if len(filtered) > MAX_FILES_WITHOUT_CONFIRMATION and not force:
            logger.warning(
                f"Large artifact save requested: {len(filtered)} files for {project_id}. "
                f"Threshold is {MAX_FILES_WITHOUT_CONFIRMATION}. Requires confirmation."
            )
            return {
                "status": "confirmation_required",
                "saved": 0,
                "file_count": len(filtered),
                "threshold": MAX_FILES_WITHOUT_CONFIRMATION,
                "files": [a.get("path") for a in filtered[:50]],  # Preview first 50
            }
        
        # Enforce hard limit
        if len(filtered) > MAX_FILES_HARD_LIMIT:
            logger.warning(
                f"Artifact save exceeds hard limit: {len(filtered)} > {MAX_FILES_HARD_LIMIT}. "
                f"Truncating to {MAX_FILES_HARD_LIMIT} files."
            )
            filtered = filtered[:MAX_FILES_HARD_LIMIT]
        
        saved = 0
        for artifact in filtered:
            path = artifact.get("path")
            content = artifact.get("content")
            try:
                result = await self.save_file(project_id, path, content, run_id)
                if result.get("status") in {"created", "updated"}:
                    saved += 1
                else:
                    # save_file refuses some writes (e.g. an oversized file whose
                    # spill failed) by return value, not by raising.
                    logger.warning(
                        "[ARTIFACT] batch did not save %s: %s",
                        path, result.get("reason") or result.get("status"),
                    )
            except Exception as e:
                logger.warning(f"Failed to snapshot {path}: {e}")
        
        logger.info(f"Snapshotted {saved} files for project {project_id}")
        return {"status": "saved", "saved": saved}
    
    # =========================================================================
    # File Checkpoints (stamp-on-write — see save_file)
    # =========================================================================

    async def get_files_at_checkpoint(
        self,
        project_id: str,
        checkpoint_sequence: int,
        run_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get all files that existed at a specific checkpoint.
        
        Returns files where checkpoint_sequence <= target sequence.
        Used for restore operations.
        """
        if self.collection is None:
            return []
        
        query = {
            "project_id": project_id,
            "checkpoint_sequence": {"$lte": checkpoint_sequence},
            "deleted_at": None,
        }
        if run_id:
            query["run_id"] = run_id
        
        cursor = self.collection.find(query).sort("path", 1)
        docs = await cursor.to_list(length=10000)
        
        return [
            {
                "path": doc["path"],
                "content": doc.get("content"),
                "version": doc.get("version", 1),
                "checkpoint_sequence": doc.get("checkpoint_sequence"),
                **_spill_fields(doc),
            }
            for doc in docs
        ]
    
    async def restore_files_to_checkpoint(
        self,
        project_id: str,
        checkpoint_sequence: int,
        run_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Restore file state to a specific checkpoint.

        - Hard-deletes files stamped strictly after the checkpoint
        - Keeps files at or before the checkpoint
        - Preserves legacy `0`-stamped records (migration backfill sentinel)

        Args:
            project_id: Project identifier
            checkpoint_sequence: Target message sequence to restore to
            run_id: Optional run identifier

        Returns:
            Dict with status and counts
        """
        if self.collection is None:
            return {"status": "skipped", "reason": "not_initialized"}

        # Hard-delete files stamped strictly after the checkpoint. Under
        # atomic-claim save_file, every runtime stamp is > the user message
        # that triggered the write, so $gt is the correct boundary. Records
        # with sentinel `0` (legacy migration backfill) are preserved by any
        # positive-target restore. Soft-delete was previously used but no
        # reader queries soft-deleted file records — the marker produced
        # only data ballast.
        delete_query = {
            "project_id": project_id,
            "deleted_at": None,
            "checkpoint_sequence": {"$gt": checkpoint_sequence},
        }
        if run_id:
            delete_query["run_id"] = run_id

        delete_result = await self.collection.delete_many(delete_query)

        # Count remaining files
        remaining = await self.count_files(project_id)

        logger.info(
            f"Restored files to checkpoint: project={project_id} sequence={checkpoint_sequence} "
            f"deleted={delete_result.deleted_count} remaining={remaining}"
        )

        return {
            "status": "restored",
            "checkpoint_sequence": checkpoint_sequence,
            "deleted_count": delete_result.deleted_count,
            "remaining_count": remaining,
        }
