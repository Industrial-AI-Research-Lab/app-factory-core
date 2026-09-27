"""S3 blob store for user attachments and tenant artifacts.

Reuses ARCHIVE_S3_* credentials. Bucket is FILE_S3_BUCKET, else ARCHIVE_S3_BUCKET
(same lifecycle as run archives — attachments go away with the rest).
Keys stay off the archive layout ``{tenant}/{project}/{run}/{ref}``.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

logger = logging.getLogger(__name__)

ATTACHMENTS_SEGMENT = "attachments"
TENANT_ARTIFACTS_SEGMENT = "tenant-artifacts"

# Reuse one store per process+env so preview does not rebuild clients / re-log.
_FROM_ENV: FileBlobStore | None = None
_FROM_ENV_KEY: tuple | None = None


def _env_fingerprint() -> tuple:
    return (
        (os.getenv("FILE_S3_BUCKET") or "").strip(),
        (os.getenv("ARCHIVE_S3_BUCKET") or "").strip(),
        (os.getenv("ARCHIVE_S3_ENDPOINT") or "").strip(),
        (os.getenv("ARCHIVE_S3_REGION") or "").strip(),
        (os.getenv("ARCHIVE_S3_ACCESS_KEY") or "").strip(),
        (os.getenv("ARCHIVE_S3_SECRET_KEY") or "").strip(),
        os.getenv("ARCHIVE_S3_ADDRESSING_STYLE") or "path",
    )


def attachment_object_key(tenant_id: str, project_id: str, attachment_id: str) -> str:
    return f"{tenant_id}/{ATTACHMENTS_SEGMENT}/{project_id}/{attachment_id}"


def tenant_artifact_object_key(tenant_id: str, artifact_id: str) -> str:
    return f"{tenant_id}/{TENANT_ARTIFACTS_SEGMENT}/{artifact_id}"


def attachments_project_prefix(tenant_id: str, project_id: str) -> str:
    return f"{tenant_id}/{ATTACHMENTS_SEGMENT}/{project_id}/"


def attachments_tenant_prefix(tenant_id: str) -> str:
    return f"{tenant_id}/{ATTACHMENTS_SEGMENT}/"


def tenant_artifacts_prefix(tenant_id: str) -> str:
    return f"{tenant_id}/{TENANT_ARTIFACTS_SEGMENT}/"


async def delete_project_attachment_prefixes(
    tenant_id: str,
    project_ids: list[str],
) -> tuple[int, int]:
    """Best-effort S3 cleanup after Mongo meta is gone.

    Call only post-commit (e.g. tenant_routes after cascade succeeds). Never from
    inside a Mongo transaction / compensating rollback — S3 cannot be undone.
    """
    if not tenant_id or not project_ids:
        return (0, 0)
    store = FileBlobStore.from_env()
    deleted = 0
    failed = 0
    for project_id in project_ids:
        if not project_id:
            continue
        prefix = attachments_project_prefix(tenant_id, project_id)
        d, f = await store.delete_prefix(prefix)
        deleted += d
        failed += f
        if d or f:
            logger.info(
                "[ATTACH] project prefix deleted tenant=%s project=%s blobs_deleted=%d blobs_failed=%d",
                tenant_id,
                project_id,
                d,
                f,
            )
    return deleted, failed


class FileBlobStore:
    """PUT/HEAD/DELETE/presign for user-file bytes. Seams are overridable in tests."""

    def __init__(
        self,
        *,
        endpoint: Optional[str] = None,
        region: Optional[str] = None,
        bucket: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        addressing_style: str = "path",
        using_archive_bucket: bool = False,
    ):
        self.endpoint = endpoint
        self.region = region
        self.bucket = bucket
        self._access_key = access_key
        self._secret_key = secret_key
        self.addressing_style = addressing_style
        self.using_archive_bucket = using_archive_bucket
        self._session = None

    @classmethod
    def from_env(cls) -> "FileBlobStore":
        global _FROM_ENV, _FROM_ENV_KEY
        key = _env_fingerprint()
        if _FROM_ENV is not None and _FROM_ENV_KEY == key:
            return _FROM_ENV
        file_bucket = (os.getenv("FILE_S3_BUCKET") or "").strip()
        archive_bucket = (os.getenv("ARCHIVE_S3_BUCKET") or "").strip()
        using_archive = False
        if file_bucket:
            bucket = file_bucket
        elif archive_bucket:
            bucket = archive_bucket
            using_archive = True
        else:
            bucket = ""
        store = cls(
            endpoint=(os.getenv("ARCHIVE_S3_ENDPOINT") or "").strip() or None,
            region=(os.getenv("ARCHIVE_S3_REGION") or "").strip() or None,
            bucket=bucket or None,
            access_key=(os.getenv("ARCHIVE_S3_ACCESS_KEY") or "").strip() or None,
            secret_key=(os.getenv("ARCHIVE_S3_SECRET_KEY") or "").strip() or None,
            addressing_style=os.getenv("ARCHIVE_S3_ADDRESSING_STYLE") or "path",
            using_archive_bucket=using_archive,
        )
        if store.is_configured():
            logger.info(
                "[ATTACH] blob store ready bucket=%s using_archive_bucket=%s prefix=%s|%s",
                store.bucket,
                store.using_archive_bucket,
                ATTACHMENTS_SEGMENT,
                TENANT_ARTIFACTS_SEGMENT,
            )
            if store.using_archive_bucket:
                logger.info(
                    "[ATTACH] using ARCHIVE_S3_BUCKET — attachments share archive lifecycle"
                )
        else:
            logger.warning("[ATTACH] object storage not configured")
        _FROM_ENV = store
        _FROM_ENV_KEY = key
        return store

    def is_configured(self) -> bool:
        return bool(self.endpoint and self.bucket and self._access_key and self._secret_key)

    def _s3_client(self):
        import aioboto3
        from botocore.config import Config

        if self._session is None:
            self._session = aioboto3.Session()
        return self._session.client(
            "s3",
            endpoint_url=self.endpoint,
            region_name=self.region,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
            config=Config(s3={"addressing_style": self.addressing_style}),
        )

    async def put_blob(
        self,
        key: str,
        data: bytes,
        content_type: str,
        *,
        filename: str | None = None,
    ) -> None:
        await self._put_blob(key, data, content_type, filename=filename)

    async def _put_blob(
        self,
        key: str,
        data: bytes,
        content_type: str,
        *,
        filename: str | None = None,
    ) -> None:
        # Force download UX: user-controlled MIME must not inline-render as a page.
        safe_name = (filename or "").replace('"', "").replace("\r", "").replace("\n", "").strip()
        disposition = (
            f'attachment; filename="{safe_name}"' if safe_name else "attachment"
        )
        async with self._s3_client() as s3:
            await s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=data,
                ContentType=content_type,
                ContentDisposition=disposition,
            )

    async def head_blob(self, object_key: str) -> Optional[dict[str, Any]]:
        try:
            return await self._head_blob(object_key)
        except Exception as exc:
            resp = getattr(exc, "response", None)
            code = ""
            if isinstance(resp, dict):
                code = str(((resp.get("Error") or {}).get("Code")) or "")
            if code in ("404", "NoSuchKey", "NotFound"):
                return None
            raise

    async def _head_blob(self, object_key: str) -> dict[str, Any]:
        async with self._s3_client() as s3:
            return await s3.head_object(Bucket=self.bucket, Key=object_key)

    async def get_blob(self, object_key: str, *, max_bytes: int | None = None) -> bytes | None:
        """Read object bytes from storage. Returns None if missing or unreadable."""
        if not self.is_configured():
            return None
        try:
            return await self._get_blob(object_key, max_bytes=max_bytes)
        except Exception as exc:
            resp = getattr(exc, "response", None)
            code = ""
            if isinstance(resp, dict):
                code = str(((resp.get("Error") or {}).get("Code")) or "")
            if code in ("404", "NoSuchKey", "NotFound"):
                return None
            logger.warning("[ATTACH] get_blob failed key=%s: %s", object_key, exc)
            return None

    async def _get_blob(self, object_key: str, *, max_bytes: int | None = None) -> bytes:
        async with self._s3_client() as s3:
            kwargs: dict[str, Any] = {"Bucket": self.bucket, "Key": object_key}
            if max_bytes is not None and max_bytes >= 0:
                end = 0 if max_bytes == 0 else max_bytes - 1
                kwargs["Range"] = f"bytes=0-{end}"
            resp = await s3.get_object(**kwargs)
            return await resp["Body"].read()

    async def presign_get(self, object_key: str, ttl_seconds: int) -> str:
        return await self._presign(object_key, int(ttl_seconds))

    async def _presign(self, object_key: str, ttl_seconds: int) -> str:
        async with self._s3_client() as s3:
            return await s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket, "Key": object_key},
                ExpiresIn=ttl_seconds,
            )

    async def presign_put(
            self,
            object_key: str,
            ttl_seconds: int,
            *,
            content_type: str | None = None,
    ) -> str:
        return await self._presign_put(
            object_key,
            int(ttl_seconds),
            content_type=content_type,
        )

    async def _presign_put(
            self,
            object_key: str,
            ttl_seconds: int,
            *,
            content_type: str | None = None,
    ) -> str:
        params: dict[str, Any] = {
            "Bucket": self.bucket,
            "Key": object_key,
        }

        if content_type:
            params["ContentType"] = content_type

        async with self._s3_client() as s3:
            return await s3.generate_presigned_url(
                "put_object",
                Params=params,
                ExpiresIn=ttl_seconds,
            )

    async def delete_keys(self, keys: list[str]) -> tuple[int, int]:
        """Best-effort delete of explicit keys. Never raises."""

        if not keys:
            return (0, 0)
        if not self.is_configured():
            logger.warning("[ATTACH] keys not deleted — object storage not configured")
            return (0, 0)
        try:
            return await self._delete_keys(keys)
        except Exception as exc:
            logger.error("[ATTACH] delete_keys failed count=%d: %s", len(keys), exc)
            return (0, len(keys))

    async def _delete_keys(self, keys: list[str]) -> tuple[int, int]:
        deleted = 0
        failed = 0
        async with self._s3_client() as s3:
            for i in range(0, len(keys), 1000):
                batch = [{"Key": k} for k in keys[i : i + 1000]]
                resp = await s3.delete_objects(
                    Bucket=self.bucket, Delete={"Objects": batch, "Quiet": True}
                )
                errors = len(resp.get("Errors", []))
                failed += errors
                deleted += len(batch) - errors
        return (deleted, failed)

    async def delete_prefix(self, prefix: str) -> tuple[int, int]:
        if not prefix:
            return (0, 0)
        if not self.is_configured():
            logger.warning("[ATTACH] prefix %r not deleted — object storage not configured", prefix)
            return (0, 0)
        try:
            return await self._delete_prefix(prefix)
        except Exception as exc:
            logger.error("[ATTACH] prefix delete failed (%r): %s", prefix, exc)
            return (0, 0)

    async def _delete_prefix(self, prefix: str) -> tuple[int, int]:
        deleted = 0
        failed = 0
        async with self._s3_client() as s3:
            continuation: Optional[str] = None
            while True:
                kwargs: dict[str, Any] = {"Bucket": self.bucket, "Prefix": prefix, "MaxKeys": 1000}
                if continuation:
                    kwargs["ContinuationToken"] = continuation
                listing = await s3.list_objects_v2(**kwargs)
                keys = [obj["Key"] for obj in listing.get("Contents", [])]
                if keys:
                    d, f = await self._delete_keys(keys)
                    deleted += d
                    failed += f
                if not listing.get("IsTruncated"):
                    break
                continuation = listing.get("NextContinuationToken")
        return (deleted, failed)
