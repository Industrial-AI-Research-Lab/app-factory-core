"""Persistence helpers for MCP ZIP-wizard state."""

from __future__ import annotations

from typing import Any


def _collection(storage: Any, name: str) -> Any:
    value = getattr(storage, name, None)
    if value is None:
        raise RuntimeError(f"Storage collection is not initialized: {name}")
    return value


async def _find_one(collection: Any, query: dict) -> dict | None:
    if isinstance(collection, list):
        return next(
            (
                dict(doc)
                for doc in collection
                if all(doc.get(k) == v for k, v in query.items())
            ),
            None,
        )
    return await collection.find_one(query)


async def _replace_one(collection: Any, query: dict, document: dict) -> None:
    if isinstance(collection, list):
        for index, current in enumerate(collection):
            if all(current.get(k) == v for k, v in query.items()):
                collection[index] = dict(document)
                return
        collection.append(dict(document))
        return
    await collection.replace_one(query, document, upsert=True)


async def _delete_one(collection: Any, query: dict) -> bool:
    if isinstance(collection, list):
        for index, current in enumerate(collection):
            if all(current.get(k) == v for k, v in query.items()):
                collection.pop(index)
                return True
        return False
    result = await collection.delete_one(query)
    return bool(result.deleted_count)


async def _find_many(collection: Any, query: dict) -> list[dict]:
    if isinstance(collection, list):
        return [
            dict(doc)
            for doc in collection
            if all(doc.get(k) == v for k, v in query.items())
        ]
    return await collection.find(query).to_list(length=None)


async def get_package(storage: Any, tenant_id: str, server_id: str) -> dict | None:
    return await _find_one(
        _collection(storage, "mcp_packages"),
        {"tenant_id": tenant_id, "server_id": server_id},
    )


async def list_packages(storage: Any, tenant_id: str | None = None) -> list[dict]:
    query = {} if tenant_id is None else {"tenant_id": tenant_id}
    return await _find_many(_collection(storage, "mcp_packages"), query)


async def save_package(storage: Any, document: dict) -> dict:
    await _replace_one(
        _collection(storage, "mcp_packages"),
        {"tenant_id": document["tenant_id"], "server_id": document["server_id"]},
        document,
    )
    return document


async def patch_package(
    storage: Any,
    tenant_id: str,
    server_id: str,
    patch: dict,
) -> bool:
    """Atomically set package fields without replacing concurrent build updates."""
    collection = _collection(storage, "mcp_packages")
    query = {"tenant_id": tenant_id, "server_id": server_id}
    if isinstance(collection, list):
        for current in collection:
            if all(current.get(key) == value for key, value in query.items()):
                current.update(patch)
                return True
        return False
    result = await collection.update_one(query, {"$set": dict(patch)})
    return bool(getattr(result, "matched_count", 0))


async def update_package(
    storage: Any, tenant_id: str, server_id: str, patch: dict
) -> dict | None:
    if not await patch_package(storage, tenant_id, server_id, patch):
        return None
    return await get_package(storage, tenant_id, server_id)


async def delete_package(storage: Any, tenant_id: str, server_id: str) -> bool:
    return await _delete_one(
        _collection(storage, "mcp_packages"),
        {"tenant_id": tenant_id, "server_id": server_id},
    )


async def get_built_image(storage: Any, doc_id: str) -> dict | None:
    return await _find_one(_collection(storage, "mcp_built_images"), {"_id": doc_id})


async def save_built_image(storage: Any, document: dict) -> dict:
    await _replace_one(
        _collection(storage, "mcp_built_images"), {"_id": document["_id"]}, document
    )
    return document


async def list_built_images(
    storage: Any,
    tenant_id: str | None = None,
    server_id: str | None = None,
) -> list[dict]:
    query = {}
    if tenant_id is not None:
        query["tenant_id"] = tenant_id
    if server_id is not None:
        query["server_id"] = server_id
    return await _find_many(_collection(storage, "mcp_built_images"), query)


async def delete_built_image(storage: Any, doc_id: str) -> bool:
    return await _delete_one(_collection(storage, "mcp_built_images"), {"_id": doc_id})
