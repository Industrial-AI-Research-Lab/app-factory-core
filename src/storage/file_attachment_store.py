"""Mongo metadata for user attachments and tenant artifacts.

Blobs go through FileBlobStore first; a failed insert rolls back those keys.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from storage.file_blob_store import (
    FileBlobStore,
    attachment_object_key,
    tenant_artifact_object_key,
)
from storage.file_upload_validation import FileUploadError, FileUploadLimits, load_limits, validate_batch

logger = logging.getLogger(__name__)


class FileAttachmentStore:
    def __init__(self, db, blob_store: FileBlobStore):
        self.db = db
        self.blob_store = blob_store
        inner = getattr(db, "db", db)
        self.user_attachments = inner.user_attachments
        self.tenant_artifacts = inner.tenant_artifacts

    async def ensure_indexes(self) -> None:
        await self.user_attachments.create_index(
            [("tenant_id", 1), ("project_id", 1)],
            name="tenant_project",
        )
        await self.user_attachments.create_index("message_id")
        await self.user_attachments.create_index(
            [("project_id", 1), ("message_sequence", 1)],
            name="project_message_sequence",
        )
        await self.tenant_artifacts.create_index("tenant_id")

    async def save_user_attachments(
        self,
        *,
        tenant_id: str,
        project_id: str,
        files: list[dict],
        message_id: Optional[str] = None,
        message_sequence: Optional[int] = None,
        run_id: Optional[str] = None,
        created_by: Optional[str] = None,
        limits: FileUploadLimits | None = None,
    ) -> list[dict[str, Any]]:
        if not self.blob_store.is_configured():
            raise FileUploadError("object storage not configured", status_code=503)
        # Revert can delete the message between append and this save — refuse
        # orphan blobs tied to a gone message_id (send+revert race).
        if message_id and not await self._message_alive(project_id, message_id):
            logger.warning(
                "[ATTACH] refuse save — message gone project=%s message=%s",
                project_id,
                message_id,
            )
            raise FileUploadError("message no longer exists", status_code=409)
        prepared = validate_batch(files, limits or load_limits())
        now = datetime.now(timezone.utc)
        docs: list[dict[str, Any]] = []
        keys: list[str] = []
        try:
            for item in prepared:
                attachment_id = uuid.uuid4().hex
                key = attachment_object_key(tenant_id, project_id, attachment_id)
                await self.blob_store.put_blob(
                    key, bytes(item["data"]), item["content_type"], filename=item["filename"]
                )
                keys.append(key)
                docs.append(
                    {
                        "_id": attachment_id,
                        "tenant_id": tenant_id,
                        "project_id": project_id,
                        "message_id": message_id,
                        "message_sequence": message_sequence,
                        "run_id": run_id,
                        "filename": item["filename"],
                        "content_type": item["content_type"],
                        "size_bytes": item["size_bytes"],
                        "object_key": key,
                        "created_by": created_by,
                        "created_at": now,
                    }
                )
            if message_id and not await self._message_alive(project_id, message_id):
                await self.blob_store.delete_keys(keys)
                logger.warning(
                    "[ATTACH] abort insert — message gone after blob put project=%s message=%s",
                    project_id,
                    message_id,
                )
                raise FileUploadError("message no longer exists", status_code=409)
            await self.user_attachments.insert_many(docs)
        except FileUploadError:
            raise
        except Exception as exc:
            logger.error("[ATTACH] save_user_attachments failed project=%s: %s", project_id, exc)
            await self.blob_store.delete_keys(keys)
            if docs:
                await self.user_attachments.delete_many({"_id": {"$in": [d["_id"] for d in docs]}})
            raise FileUploadError("failed to store attachments", status_code=500) from exc
        logger.info(
            "[ATTACH] saved count=%d project=%s tenant=%s",
            len(docs),
            project_id,
            tenant_id,
        )
        return docs

    async def create_user_attachment(
            self,
            *,
            tenant_id: str,
            project_id: str,
            filename: str,
            content_type: str,
            object_key: str,
            created_by: Optional[str] = None,
    ) -> dict[str, Any]:
        if not self.blob_store.is_configured():
            raise FileUploadError("object storage not configured", status_code=503)

        attachment_id = uuid.uuid4().hex

        now = datetime.now(timezone.utc)

        doc = {
            "_id": attachment_id,
            "tenant_id": tenant_id,
            "project_id": project_id,
            "message_id": None,
            "message_sequence": None,
            "run_id": None,
            "filename": filename,
            "content_type": content_type,
            "size_bytes": 0,
            "object_key": object_key,
            "created_by": created_by,
            "created_at": now,
        }

        try:
            await self.user_attachments.insert_one(doc)
        except Exception as exc:
            logger.error(
                "[ATTACH] create_user_attachment failed project=%s: %s",
                project_id,
                exc,
            )
            raise FileUploadError(
                "failed to create attachment",
                status_code=500,
            ) from exc

        logger.info(
            "[ATTACH] created attachment=%s project=%s tenant=%s",
            attachment_id,
            project_id,
            tenant_id,
        )

        return doc

    async def _message_alive(self, project_id: str, message_id: str) -> bool:
        """True if message still exists, or messages collection is unavailable (tests)."""
        inner = getattr(self.db, "db", self.db)
        messages = getattr(inner, "messages", None)
        if messages is None:
            return True
        doc = await messages.find_one({"id": message_id, "project_id": project_id})
        return doc is not None
    async def save_tenant_artifact(
        self,
        *,
        tenant_id: str,
        files: list[dict],
        title: Optional[str] = None,
        description: Optional[str] = None,
        created_by: Optional[str] = None,
        limits: FileUploadLimits | None = None,
    ) -> list[dict[str, Any]]:
        if not self.blob_store.is_configured():
            raise FileUploadError("object storage not configured", status_code=503)
        prepared = validate_batch(files, limits or load_limits())
        now = datetime.now(timezone.utc)
        docs: list[dict[str, Any]] = []
        keys: list[str] = []
        one = len(prepared) == 1
        try:
            for item in prepared:
                artifact_id = uuid.uuid4().hex
                key = tenant_artifact_object_key(tenant_id, artifact_id)
                await self.blob_store.put_blob(
                    key, bytes(item["data"]), item["content_type"], filename=item["filename"]
                )
                keys.append(key)
                docs.append(
                    {
                        "_id": artifact_id,
                        "tenant_id": tenant_id,
                        "filename": item["filename"],
                        "content_type": item["content_type"],
                        "size_bytes": item["size_bytes"],
                        "object_key": key,
                        "title": title if (one and title) else item["filename"],
                        "description": description if one else None,
                        "created_by": created_by,
                        "created_at": now,
                        "updated_at": now,
                        "updated_by": created_by,
                    }
                )
            await self.tenant_artifacts.insert_many(docs)
        except FileUploadError:
            raise
        except Exception as exc:
            logger.error("[TENANT_ARTIFACT] save failed tenant=%s: %s", tenant_id, exc)
            await self.blob_store.delete_keys(keys)
            if docs:
                await self.tenant_artifacts.delete_many({"_id": {"$in": [d["_id"] for d in docs]}})
            raise FileUploadError("failed to store tenant artifact", status_code=500) from exc
        logger.info("[TENANT_ARTIFACT] saved count=%d tenant=%s", len(docs), tenant_id)
        return docs

    def _user_attachment_scope(self, tenant_id: str, project_id: str) -> dict | None:
        """Every user-attachment read includes both ids. No find-by-_id-alone."""
        if not tenant_id or not project_id:
            return None
        return {"tenant_id": tenant_id, "project_id": project_id}

    async def get_user_attachment(
        self, *, tenant_id: str, project_id: str, attachment_id: str
    ) -> dict[str, Any] | None:
        scope = self._user_attachment_scope(tenant_id, project_id)
        if not scope or not attachment_id:
            return None
        return await self.user_attachments.find_one({**scope, "_id": attachment_id})

    async def list_user_attachments_for_messages(
        self, *, tenant_id: str, project_id: str, message_ids: list[str]
    ) -> list[dict[str, Any]]:
        scope = self._user_attachment_scope(tenant_id, project_id)
        ids = [mid for mid in message_ids if mid]
        if not scope or not ids:
            return []
        cursor = self.user_attachments.find({**scope, "message_id": {"$in": ids}})
        return await cursor.to_list(length=self._message_attachment_fetch_cap(len(ids)))

    async def list_user_attachments(
        self, *, tenant_id: str, project_id: str, limit: int = 200, skip: int = 0
    ) -> tuple[list[dict[str, Any]], bool]:
        """Newest-first list. Returns (rows, truncated) when more than ``limit`` exist."""
        scope = self._user_attachment_scope(tenant_id, project_id)
        if not scope:
            return [], False
        cap = max(1, min(int(limit), 500))
        offset = max(0, int(skip))
        cursor = (
            self.user_attachments.find(scope)
            .sort([("created_at", -1)])
            .skip(offset)
        )
        rows = await cursor.to_list(length=cap + 1)
        truncated = len(rows) > cap
        if truncated:
            logger.warning(
                "[ATTACH] list truncated tenant=%s project=%s cap=%d skip=%d",
                tenant_id,
                project_id,
                cap,
                offset,
            )
            rows = rows[:cap]
        return rows, truncated

    @staticmethod
    def _message_attachment_fetch_cap(message_count: int) -> int:
        """Cap for keys/meta fetched per message batch — follows FILE_UPLOAD_MAX_FILES."""
        per = max(1, int(load_limits().max_files))
        n = max(1, int(message_count))
        return max(n * per, per)

    def _tenant_artifact_scope(self, tenant_id: str) -> dict | None:
        """Every tenant-artifact read includes tenant_id. No find-by-_id-alone."""
        if not tenant_id:
            return None
        return {"tenant_id": tenant_id}

    async def get_tenant_artifact(self, *, tenant_id: str, artifact_id: str) -> dict[str, Any] | None:
        scope = self._tenant_artifact_scope(tenant_id)
        if not scope or not artifact_id:
            return None
        return await self.tenant_artifacts.find_one({**scope, "_id": artifact_id})

    async def list_tenant_artifacts(
        self, *, tenant_id: str, limit: int = 200
    ) -> tuple[list[dict[str, Any]], bool]:
        """Newest-first list. Returns (rows, truncated) when more than ``limit`` exist."""
        scope = self._tenant_artifact_scope(tenant_id)
        if not scope:
            return [], False
        cap = max(1, min(int(limit), 500))
        cursor = self.tenant_artifacts.find(scope).sort([("created_at", -1)])
        rows = await cursor.to_list(length=cap + 1)
        truncated = len(rows) > cap
        if truncated:
            logger.warning(
                "[TENANT_ARTIFACT] list truncated tenant=%s cap=%d",
                tenant_id,
                cap,
            )
            rows = rows[:cap]
        return rows, truncated

    async def update_tenant_artifact(
        self,
        *,
        tenant_id: str,
        artifact_id: str,
        title: Optional[str] = None,
        description: Any = ...,
        files: list[dict] | None = None,
        updated_by: Optional[str] = None,
        limits: FileUploadLimits | None = None,
    ) -> dict[str, Any] | None:
        doc = await self.get_tenant_artifact(tenant_id=tenant_id, artifact_id=artifact_id)
        if not doc:
            return None
        now = datetime.now(timezone.utc)
        fields: dict[str, Any] = {"updated_at": now, "updated_by": updated_by}
        if title is not None:
            fields["title"] = title
        if description is not ...:
            fields["description"] = description

        old_key = None
        new_key = None
        if files:
            if not self.blob_store.is_configured():
                raise FileUploadError("object storage not configured", status_code=503)
            prepared = validate_batch(files, limits or load_limits())
            if len(prepared) != 1:
                raise FileUploadError("tenant artifact requires exactly one file")
            item = prepared[0]
            new_key = f"{tenant_artifact_object_key(tenant_id, artifact_id)}.{uuid.uuid4().hex}"
            await self.blob_store.put_blob(
                new_key, bytes(item["data"]), item["content_type"], filename=item["filename"]
            )
            old_key = doc.get("object_key")
            fields.update(
                {
                    "filename": item["filename"],
                    "content_type": item["content_type"],
                    "size_bytes": item["size_bytes"],
                    "object_key": new_key,
                }
            )
        try:
            await self.tenant_artifacts.update_one(
                {"tenant_id": tenant_id, "_id": artifact_id}, {"$set": fields}
            )
        except Exception as exc:
            logger.error("[TENANT_ARTIFACT] update failed id=%s tenant=%s: %s", artifact_id, tenant_id, exc)
            if new_key:
                await self.blob_store.delete_keys([new_key])
            raise FileUploadError("failed to update tenant artifact", status_code=500) from exc
        if old_key and old_key != new_key:
            await self.blob_store.delete_keys([old_key])
        logger.info("[TENANT_ARTIFACT] updated id=%s tenant=%s", artifact_id, tenant_id)
        return await self.get_tenant_artifact(tenant_id=tenant_id, artifact_id=artifact_id)

    async def delete_tenant_artifact(self, *, tenant_id: str, artifact_id: str) -> dict[str, Any] | None:
        doc = await self.get_tenant_artifact(tenant_id=tenant_id, artifact_id=artifact_id)
        if not doc:
            return None
        await self.tenant_artifacts.delete_one({"tenant_id": tenant_id, "_id": artifact_id})
        key = doc.get("object_key")
        if key:
            deleted, failed = await self.blob_store.delete_keys([key])
            if failed:
                logger.warning(
                    "[TENANT_ARTIFACT] blob leftover id=%s tenant=%s key=%s",
                    artifact_id,
                    tenant_id,
                    key,
                )
        logger.info("[TENANT_ARTIFACT] deleted id=%s tenant=%s", artifact_id, tenant_id)
        return doc

    async def delete_user_attachments_for_message_ids(
        self,
        *,
        tenant_id: str,
        project_id: str,
        message_ids: list[str],
    ) -> list[dict[str, Any]]:
        """Drop attachments tied to reverted messages. Best-effort blob delete."""
        scope = self._user_attachment_scope(tenant_id, project_id)
        ids = [mid for mid in message_ids if mid]
        if not scope or not ids:
            return []
        cursor = self.user_attachments.find({**scope, "message_id": {"$in": ids}})
        # Collect every matching meta row before delete_many — do not cap below real count
        # (FILE_UPLOAD_MAX_FILES can be raised; hardcoded *10 left orphan S3 keys).
        docs: list[dict[str, Any]] = []
        async for doc in cursor:
            docs.append(doc)
        if not docs:
            return []
        keys = [str(doc.get("object_key")) for doc in docs if doc.get("object_key")]
        result = await self.user_attachments.delete_many({**scope, "message_id": {"$in": ids}})
        if keys:
            _, failed = await self.blob_store.delete_keys(keys)
            if failed:
                logger.warning(
                    "[ATTACH] revert blob leftover count=%d project=%s",
                    failed,
                    project_id,
                )
        deleted = int(result.deleted_count)
        if deleted:
            logger.info("[ATTACH] revert deleted count=%d project=%s", deleted, project_id)
        return docs

    async def update_user_attachment_size(
            self,
            *,
            tenant_id: str,
            project_id: str,
            attachment_id: str,
            size_bytes: int,
    ) -> dict[str, Any] | None:
        """Update a user attachment's size from object storage metadata."""
        scope = self._user_attachment_scope(tenant_id, project_id)

        if (
                not scope
                or not attachment_id
                or not isinstance(size_bytes, int)
                or isinstance(size_bytes, bool)
                or size_bytes < 0
        ):
            return None

        result = await self.user_attachments.update_one(
            {
                **scope,
                "_id": attachment_id,
                "size_bytes": 0,
            },
            {
                "$set": {
                    "size_bytes": size_bytes,
                }
            },
        )

        if result.modified_count:
            logger.info(
                "[ATTACH] updated size attachment=%s project=%s size=%d",
                attachment_id,
                project_id,
                size_bytes,
            )

        return await self.get_user_attachment(
            tenant_id=tenant_id,
            project_id=project_id,
            attachment_id=attachment_id,
        )

    async def refresh_user_attachment_size(
            self,
            *,
            tenant_id: str,
            project_id: str,
            attachment_id: str,
            doc: dict[str, Any],
    ) -> dict[str, Any]:
        """Refresh size_bytes from object storage when MongoDB has zero."""
        if doc.get("size_bytes") != 0:
            return doc

        object_key = doc.get("object_key")
        if not object_key:
            return doc

        if not self.blob_store.is_configured():
            return doc

        head = await self.blob_store.head_blob(str(object_key))

        if not isinstance(head, dict):
            return doc

        actual_size = head.get("ContentLength")

        if (
                not isinstance(actual_size, int)
                or isinstance(actual_size, bool)
                or actual_size < 0
        ):
            return doc

        updated_doc = await self.update_user_attachment_size(
            tenant_id=tenant_id,
            project_id=project_id,
            attachment_id=attachment_id,
            size_bytes=actual_size,
        )

        return updated_doc or doc
