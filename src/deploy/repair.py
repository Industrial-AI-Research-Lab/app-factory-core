"""Deploy Repair Module

LLM-based repair logic for deployment failures.
Analyzes build/deploy errors and generates structured repair actions.
"""

from typing import Any, Dict, List, Optional, Tuple
import json
import re
import logging

from storage.artifact_store import ArtifactStore

logger = logging.getLogger(__name__)


async def analyze_and_repair(
    llm_client,
    shared_context,
    diagnostics: Dict[str, Any],
    attempt: int,
    storage=None,
) -> Tuple[bool, bool]:
    """Use LLM to analyze failure and generate repair actions.
    
    Returns (applied, needs_delegation) tuple.
    """
    if not diagnostics or not llm_client or not shared_context:
        logger.warning("[REPAIR] Missing required params: diagnostics=%s llm=%s ctx=%s",
                       bool(diagnostics), bool(llm_client), bool(shared_context))
        return False, False

    project_id = getattr(shared_context, "project_id", None)
    if not isinstance(project_id, str) or not project_id.strip():
        logger.warning("[REPAIR] Missing project_id on shared_context - cannot load artifacts")
        return False, False
    message_store = getattr(shared_context, "message_store", None)
    artifact_store = await _initialize_artifact_store(storage, project_id, message_store=message_store)
    artifacts = await _load_artifacts_from_store(artifact_store, project_id)

    artifact_map = {a.get("path"): a for a in artifacts if isinstance(a, dict) and a.get("path")}
    artifact_paths = list(artifact_map.keys())

    # Extract mentioned files from error output to include their content
    stdout = diagnostics.get("stdout_tail", "") or ""
    stderr = diagnostics.get("stderr_tail", "") or ""
    error_text = stdout + stderr
    
    logger.info("[REPAIR] attempt=%d artifacts=%d error_len=%d", attempt, len(artifacts), len(error_text))
    
    relevant_files = _extract_error_files(error_text, artifact_paths)
    logger.info("[REPAIR] relevant_files=%s", relevant_files[:5])
    
    file_contents = {}
    for fpath in relevant_files[:3]:  # Limit to 3 files to avoid token overflow
        art = artifact_map.get(fpath)
        if art and art.get("content"):
            file_contents[fpath] = art["content"][:8000]  # Truncate large files
            logger.info("[REPAIR] included file %s (%d chars)", fpath, len(art["content"]))

    repair_prompt = {
        "role": "system",
        "content": (
            "You are a deployment repair agent. Analyze the build/deploy failure and return a JSON repair plan.\n\n"
            "Available repair actions:\n"
            "- {\"action\": \"replace_artifact\", \"path\": \"<path>\", \"content\": \"<new content>\"} - Replace artifact with COMPLETE fixed file content\n"
            "- {\"action\": \"remove_artifact\", \"path\": \"<path>\"} - Remove a problematic artifact file\n"
            "- {\"action\": \"add_artifact\", \"path\": \"<path>\", \"content\": \"<content>\"} - Add a new artifact file\n"
            "- {\"action\": \"use_template_dockerfile\"} - Remove any artifact Dockerfile and use auto-generated template\n"
            "- {\"action\": \"delegate_to_coding_agent\"} - Complex fix requiring full coding tools (use for intricate bugs)\n"
            "- {\"action\": \"no_fix_possible\", \"reason\": \"<explanation>\"} - Cannot fix automatically\n\n"
            "Return JSON: {\"analysis\": \"<what went wrong>\", \"repairs\": [<list of actions>], \"needs_coding_agent\": false}\n"
            "For source code syntax errors, FIX THEM with replace_artifact - provide the COMPLETE corrected file.\n"
            "Only use delegate_to_coding_agent for complex multi-file refactors or when you need to run tests.\n"
            "Focus on the root cause. Prefer using template Dockerfile over custom ones."
        ),
    }

    user_payload = {
        "attempt": attempt,
        "last_step": diagnostics.get("last_step"),
        "failed_command": diagnostics.get("failed_command"),
        "stderr": stderr[:3000],
        "stdout": stdout[:3000],
        "artifact_files": artifact_paths[:50],
    }
    
    if file_contents:
        user_payload["relevant_file_contents"] = file_contents

    user_msg = {
        "role": "user",
        "content": json.dumps(user_payload),
    }

    try:
        model = "gpt-5.2" #TODO add deploy_repair in config models
        logger.info(f"[REPAIR] Calling LLM model={model} temp=1")
        response = await llm_client.chat_completion_with_json(
            messages=[repair_prompt, user_msg],
            model=model,
            temperature=1,
        )
        logger.info("[REPAIR] LLM response type=%s", type(response).__name__)
    except Exception as e:
        logger.error("[REPAIR] LLM call failed: %s", e)
        return False, False

    if not isinstance(response, dict):
        logger.warning("[REPAIR] LLM response not a dict: %s", str(response)[:200])
        return False, False

    repairs = response.get("repairs") or []
    needs_coding_agent = response.get("needs_coding_agent", False)
    analysis = response.get("analysis", "")
    logger.info("[REPAIR] analysis=%s repairs=%d needs_delegation=%s", 
                analysis[:200] if analysis else "none", len(repairs), needs_coding_agent)
    
    if not repairs:
        logger.warning("[REPAIR] No repairs suggested")
        return False, needs_coding_agent

    for r in repairs:
        logger.info("[REPAIR] action=%s path=%s content_len=%d", 
                    r.get("action"), r.get("path", "n/a"), 
                    len(r.get("content", "")) if r.get("content") else 0)

    applied, delegate = await apply_repairs(project_id, artifact_store, artifacts, repairs)
    # When repairs actually changed artifacts, force needs_delegation=True so the
    # orchestrator's retry loop at orchestrator.py:1118-1129 re-enters
    # deploy_agent.execute_task() and rebuilds/redeploys with the new artifacts.
    # Without this, the loop short-circuits on `not needs_delegation` and the
    # repaired artifacts are stranded — verified on project 21b8c9e1 where
    # repair correctly added Dockerfile + nginx/default.conf (backend.log:307-309
    # `applied=True delegate=False`) but no redeploy ever ran. Overloading
    # `needs_delegation` here also triggers _run_coding_fix, which is more work
    # than strictly necessary, but ensures the deploy actually retries with the
    # new artifacts; coding_agent will see the artifacts already exist and
    # converge quickly.
    needs_delegation = delegate or needs_coding_agent or applied
    logger.info(
        "[REPAIR] applied=%s delegate=%s llm_needs_coding=%s -> needs_delegation=%s",
        applied, delegate, needs_coding_agent, needs_delegation,
    )
    return applied, needs_delegation


async def apply_repairs(
    project_id: str,
    artifact_store: Optional[ArtifactStore],
    artifacts: List[Dict],
    repairs: List[Dict],
) -> tuple:
    """Apply repair actions. Returns (applied_any, needs_delegation)."""
    applied_any = False
    needs_delegation = False

    for repair in repairs:
        if not isinstance(repair, dict):
            continue
        action = repair.get("action")

        if action == "no_fix_possible":
            return False, False

        if action == "delegate_to_coding_agent":
            needs_delegation = True
            continue

        if action == "use_template_dockerfile":
            applied_any = await _remove_dockerfile(project_id, artifact_store, artifacts) or applied_any

        elif action == "remove_artifact":
            path = repair.get("path")
            if path:
                applied_any = await _remove_artifact(project_id, artifact_store, artifacts, path) or applied_any

        elif action == "replace_artifact":
            path = repair.get("path")
            content = repair.get("content")
            if path and content:
                applied_any = await _replace_artifact(
                    project_id, artifact_store, artifacts, path, content
                ) or applied_any

        elif action == "add_artifact":
            path = repair.get("path")
            content = repair.get("content")
            if path and content:
                applied_any = await _add_artifact(
                    project_id, artifact_store, artifacts, path, content
                ) or applied_any

    return applied_any, needs_delegation


async def _initialize_artifact_store(
    storage,
    project_id: str,
    message_store=None,
) -> Optional[ArtifactStore]:
    """Create and initialize ArtifactStore if storage is available.

    `message_store` is required for repair-written artifacts to claim a
    position slot. save_file refuses to stamp the pre-fix `0` sentinel at
    runtime, so omitting `message_store` raises on the first repair write.
    """
    if storage is None:
        logger.warning(
            "[REPAIR] project_id=%s storage_available=false - ArtifactStore unavailable",
            project_id,
        )
        return None

    try:
        artifact_store = ArtifactStore(storage, message_store=message_store)
        await artifact_store.initialize()
    except Exception as exc:
        logger.warning(
            "[REPAIR] project_id=%s storage_init_failed=true error=%s - failed to initialize ArtifactStore",
            project_id,
            exc,
        )
        return None

    if artifact_store.collection is None:
        logger.warning(
            "[REPAIR] project_id=%s artifact_store_ready=false - ArtifactStore collection unavailable",
            project_id,
        )
        return None

    return artifact_store


async def _load_artifacts_from_store(
    artifact_store: Optional[ArtifactStore],
    project_id: str,
) -> List[Dict[str, Any]]:
    """Load normalized file artifacts from ArtifactStore."""
    if artifact_store is None:
        return []

    try:
        db_artifacts = await artifact_store.get_all_files(project_id)
    except Exception as exc:
        logger.warning(
            "[REPAIR] project_id=%s load_failed=true error=%s - failed to read file artifacts",
            project_id,
            exc,
        )
        return []

    artifacts = []
    for artifact in db_artifacts:
        path = artifact.get("path")
        content = artifact.get("content")
        if not path or not isinstance(content, str):
            continue
        artifacts.append({"type": "file", "path": path, "content": content})

    return artifacts


async def _remove_dockerfile(
    project_id: str,
    artifact_store: Optional[ArtifactStore],
    artifacts: list,
) -> bool:
    """Remove any Dockerfile from artifacts so template is used."""
    if artifact_store is None:
        logger.warning(
            "[REPAIR] project_id=%s action=use_template_dockerfile - ArtifactStore unavailable, cannot delete Dockerfile",
            project_id,
        )
        return False

    try:
        removed_any = False
        remaining_artifacts = []
        for artifact in artifacts:
            path = artifact.get("path") or ""
            if _is_dockerfile_path(path):
                removed_any = await artifact_store.delete_file(project_id, path) or removed_any
                continue
            remaining_artifacts.append(artifact)

        if removed_any:
            artifacts[:] = remaining_artifacts
            return True
    except Exception as exc:
        logger.warning(
            "[REPAIR] project_id=%s action=use_template_dockerfile error=%s - failed to remove Dockerfile",
            project_id,
            exc,
        )
    return False


async def _remove_artifact(
    project_id: str,
    artifact_store: Optional[ArtifactStore],
    artifacts: list,
    path: str,
) -> bool:
    """Remove a specific artifact by path."""
    if artifact_store is None:
        logger.warning(
            "[REPAIR] project_id=%s action=remove_artifact path=%s - ArtifactStore unavailable",
            project_id,
            path,
        )
        return False

    try:
        stored_path = _resolve_existing_path(artifacts, path)
        deleted = await artifact_store.delete_file(project_id, stored_path)
        if deleted:
            norm_path = _normalize_path(path)
            artifacts[:] = [
                artifact
                for artifact in artifacts
                if _normalize_path(artifact.get("path") or "") != norm_path
            ]
            return True
    except Exception as exc:
        logger.warning(
            "[REPAIR] project_id=%s action=remove_artifact path=%s error=%s - failed to remove artifact",
            project_id,
            path,
            exc,
        )
    return False


async def _replace_artifact(
    project_id: str,
    artifact_store: Optional[ArtifactStore],
    artifacts: list,
    path: str,
    content: str,
) -> bool:
    """Replace content of an existing artifact."""
    if artifact_store is None:
        logger.warning(
            "[REPAIR] project_id=%s action=replace_artifact path=%s - ArtifactStore unavailable",
            project_id,
            path,
        )
        return False

    try:
        stored_path = _resolve_existing_path(artifacts, path)
        result = await artifact_store.save_file(project_id, stored_path, content)
        if result.get("status") in {"created", "updated"}:
            _upsert_local_artifact(artifacts, stored_path, content)
            return True
    except Exception as exc:
        logger.warning(
            "[REPAIR] project_id=%s action=replace_artifact path=%s error=%s - failed to persist repaired artifact",
            project_id,
            path,
            exc,
        )
    return False


async def _add_artifact(
    project_id: str,
    artifact_store: Optional[ArtifactStore],
    artifacts: list,
    path: str,
    content: str,
) -> bool:
    """Add a new artifact."""
    if artifact_store is None:
        logger.warning(
            "[REPAIR] project_id=%s action=add_artifact path=%s - ArtifactStore unavailable",
            project_id,
            path,
        )
        return False

    try:
        stored_path = _resolve_existing_path(artifacts, path)
        result = await artifact_store.save_file(project_id, stored_path, content)
        if result.get("status") in {"created", "updated"}:
            _upsert_local_artifact(artifacts, stored_path, content)
            return True
    except Exception as exc:
        logger.warning(
            "[REPAIR] project_id=%s action=add_artifact path=%s error=%s - failed to add artifact",
            project_id,
            path,
            exc,
        )
    return False


def _normalize_path(path: str) -> str:
    """Normalize artifact paths for matching."""
    return (path or "").replace("\\", "/").lower().lstrip("./")


def _is_dockerfile_path(path: str) -> bool:
    """Check whether a path points to a Dockerfile."""
    norm_path = _normalize_path(path)
    return norm_path in {"dockerfile"} or norm_path.endswith("/dockerfile")


def _upsert_local_artifact(artifacts: list, path: str, content: str) -> None:
    """Keep the in-memory artifact list in sync across multiple repair actions."""
    norm_path = _normalize_path(path)
    for artifact in artifacts:
        if _normalize_path(artifact.get("path") or "") == norm_path:
            artifact["content"] = content
            artifact["type"] = artifact.get("type") or "file"
            return
    artifacts.append({"path": path, "content": content, "type": "file"})


def _resolve_existing_path(artifacts: list, path: str) -> str:
    """Prefer the stored artifact path so replacements don't create case-variant duplicates."""
    norm_path = _normalize_path(path)
    for artifact in artifacts:
        artifact_path = artifact.get("path")
        if _normalize_path(artifact_path or "") == norm_path:
            return artifact_path or path
    return path


def _extract_error_files(error_text: str, artifact_paths: List[str]) -> List[str]:
    """Extract file paths mentioned in error output that match known artifacts."""
    found = []
    
    # Common patterns for file paths in build errors
    # e.g., "/app/src/App.tsx:34:35" or "file: /app/src/App.tsx"
    patterns = [
        r'/app/([^\s:]+\.[a-zA-Z]+)',  # Docker container paths like /app/src/foo.tsx
        r'file:\s*([^\s:]+\.[a-zA-Z]+)',  # "file: path" format
        r"'([^']+\.[a-zA-Z]+)'",  # Single-quoted paths
        r'"([^"]+\.[a-zA-Z]+)"',  # Double-quoted paths
    ]
    
    for pattern in patterns:
        for match in re.finditer(pattern, error_text):
            path = match.group(1)
            # Normalize and try to match against artifacts
            norm = path.replace("\\", "/").lstrip("./")
            for ap in artifact_paths:
                ap_norm = ap.replace("\\", "/").lstrip("./")
                if ap_norm == norm or ap_norm.endswith("/" + norm) or norm.endswith("/" + ap_norm):
                    if ap not in found:
                        found.append(ap)
                    break
    
    return found
