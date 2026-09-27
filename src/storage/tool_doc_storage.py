"""Route tool documents between builtin and MCP Mongo collections."""

from __future__ import annotations

import uuid
from typing import Any


class McpToolInsertConflict(Exception):
    """(tenant, wire) or identity already occupied; insert must not overwrite."""

    def __init__(
        self,
        message: str = "MCP tool already exists",
        *,
        reason: str = "id_conflict",
        public_id: str = "",
        rpc_name: str = "",
    ):
        super().__init__(message)
        self.reason = reason
        self.public_id = public_id
        self.rpc_name = rpc_name


def is_mcp_tool_doc(doc: dict | None) -> bool:
    return isinstance(doc, dict) and doc.get("source") == "mcp_server"


async def save_tool_document(storage: Any, doc: dict, *, actor_id: str | None = None) -> None:
    if is_mcp_tool_doc(doc):
        lease_doc_id, lease_id = await _claim_mcp_image_reference_write(storage, doc)
        try:
            await storage.save_mcp_tool_configuration(doc, actor_id=actor_id)
        finally:
            await _release_mcp_image_reference_write(
                storage, lease_doc_id=lease_doc_id, lease_id=lease_id
            )
    else:
        await storage.save_tool_configuration(doc, actor_id=actor_id)


async def insert_mcp_tool_document(
    storage: Any,
    doc: dict,
    *,
    actor_id: str | None = None,
) -> dict:
    """Insert an MCP tool row without updating an existing (tenant, wire) document.

    Raises ``McpToolInsertConflict`` when the wire/identity is already taken.
    """
    if not is_mcp_tool_doc(doc):
        raise ValueError("insert_mcp_tool_document requires source=mcp_server")
    inserter = getattr(storage, "insert_mcp_tool_configuration", None)
    if not callable(inserter):
        raise RuntimeError("storage does not support insert_mcp_tool_configuration")
    lease_doc_id, lease_id = await _claim_mcp_image_reference_write(storage, doc)
    try:
        return await inserter(doc, actor_id=actor_id)
    finally:
        await _release_mcp_image_reference_write(
            storage, lease_doc_id=lease_doc_id, lease_id=lease_id
        )


def _external_mcp_image(doc: dict) -> str:
    metadata = doc.get("metadata") if isinstance(doc.get("metadata"), dict) else {}
    runtime = metadata.get("external_mcp") if isinstance(metadata.get("external_mcp"), dict) else {}
    return str(runtime.get("image") or "").strip()


async def _claim_mcp_image_reference_write(storage: Any, doc: dict) -> tuple[str | None, str | None]:
    from fastapi import HTTPException

    from tools.mcp_package_storage import (
        McpBuiltImageLeaseConflict,
        claim_mcp_built_image_reference_write_for_tag,
    )

    image_tag = _external_mcp_image(doc)
    if not image_tag:
        return None, None
    tenant_id = str(doc.get("tenant_id") or "__root__")
    reservation_id = uuid.uuid4().hex
    try:
        doc_id = await claim_mcp_built_image_reference_write_for_tag(
            storage,
            tenant_id,
            image_tag,
            reservation_id=reservation_id,
        )
    except McpBuiltImageLeaseConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return doc_id, reservation_id if doc_id else None


async def _release_mcp_image_reference_write(
    storage: Any, *, lease_doc_id: str | None, lease_id: str | None
) -> None:
    if not lease_doc_id or not lease_id:
        return
    from tools.mcp_package_storage import release_mcp_built_image_reference_write

    await release_mcp_built_image_reference_write(
        storage, lease_doc_id, reservation_id=lease_id
    )


async def delete_tool_document(storage: Any, doc: dict) -> bool:
    doc_id = str(doc.get("_id") or "")
    if not doc_id:
        return False
    if is_mcp_tool_doc(doc):
        if hasattr(storage, "delete_mcp_tool_configuration"):
            return bool(await storage.delete_mcp_tool_configuration(doc_id))
        return bool(await storage.delete_tool_configuration(doc_id))
    return bool(await storage.delete_tool_configuration(doc_id))


async def find_tools_by_name(
    storage: Any,
    name: str,
    tenant_id: str,
    *,
    mcp: bool,
    mcp_server: str | None = None,
) -> list:
    if mcp:
        if hasattr(storage, "find_mcp_tool_configurations_by_name"):
            return await storage.find_mcp_tool_configurations_by_name(
                name,
                tenant_id,
                mcp_server=mcp_server,
            )
        return []
    return await storage.find_tool_configurations_by_name(name, tenant_id)


async def count_mcp_tools_on_server(
    storage: Any,
    tenant_id: str,
    mcp_server_id: str,
    *,
    enabled_only: bool = False,
) -> int:
    """Count MCP tool docs for ``(tenant_id, mcp_server)`` (all statuses by default)."""
    server_key = str(mcp_server_id or "").strip()
    if not server_key:
        return 0
    docs = await get_mcp_tool_configurations(
        storage,
        enabled_only=enabled_only,
        tenant_id=tenant_id,
    )
    tid = str(tenant_id or "__root__")
    return sum(
        1
        for d in docs
        if str(d.get("mcp_server") or "") == server_key
        and str(d.get("tenant_id") or "__root__") == tid
    )


async def get_mcp_tool_configurations(
    storage: Any,
    *,
    enabled_only: bool = True,
    tenant_id: str | None = None,
) -> list:
    if hasattr(storage, "get_mcp_tool_configurations"):
        return await storage.get_mcp_tool_configurations(
            enabled_only=enabled_only,
            tenant_id=tenant_id,
        )
    docs = await storage.get_tool_configurations(enabled_only=enabled_only, tenant_id=tenant_id)
    out = [d for d in docs if is_mcp_tool_doc(d)]
    return out


async def get_all_tool_configurations_for_registry(
    storage: Any,
    *,
    enabled_only: bool = True,
    tenant_id: str | None = None,
) -> list:
    # MCP docs always come back unfiltered: a disabled tenant fork must survive
    # into ranking so it keeps shadowing the shared __system__ doc it forked
    # (ADR-0013). Callers re-apply the enabled filter to the EFFECTIVE doc via
    # config.tool_configuration_schema.drop_disabled_effective_docs.
    if hasattr(storage, "get_mcp_tool_configurations"):
        builtin = await storage.get_tool_configurations(
            enabled_only=enabled_only,
            tenant_id=tenant_id,
        )
        mcp = await storage.get_mcp_tool_configurations(
            enabled_only=False,
            tenant_id=tenant_id,
        )
        return list(builtin) + list(mcp)
    docs = await storage.get_tool_configurations(
        enabled_only=False,
        tenant_id=tenant_id,
    )
    if enabled_only:
        docs = [d for d in docs if is_mcp_tool_doc(d) or d.get("enabled", True)]
    return list(docs)
