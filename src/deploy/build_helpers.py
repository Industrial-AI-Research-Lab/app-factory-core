"""Image build and push helpers for deployments."""

from __future__ import annotations

from typing import Any, Dict, Optional
from pathlib import Path
import os
import logging
import tempfile

from deploy.helpers.deploy_helper import DeployHelper, maybe_registry_login
from sandbox.host_cli import run_host_cli

from deploy.stack_detector import detect_stack
from deploy.dockerfile_templates import dockerfile_for_stack
from deploy.deploy_spec import normalize_deploy_spec
from storage.artifact_store import ArtifactStore
from storage.archive_store import ArchiveStore
from tools.archive_tools import _trailing_byte_count

logger = logging.getLogger(__name__)


def _deployable_artifact_kind(art: Any) -> Optional[str]:
    """Classify a stored artifact for the build context: "inline" (bytes live in
    the doc), "spilled" (a locator hydrated from object storage at build time),
    or None (not deployable). One classifier so the precheck and the build never
    disagree on which artifacts are deployable."""
    if not isinstance(art, dict) or not (art.get("path") or "").strip():
        return None
    content = art.get("content")
    if isinstance(content, str) and content:
        return "inline"
    if art.get("spilled") and art.get("archive_ref_id"):
        return "spilled"
    return None


async def check_artifacts_ready(
    project_id: str,
    artifact_store,
) -> Dict[str, Any]:
    """Validate artifacts readiness for deploy.

    Returns {ready, count, reason}. Used by both the orchestrator precheck
    and the build path so readiness logic lives in one place.

    Criteria for ready=True:
    - At least one artifact in the store (count > 0)
    - At least one deployable artifact (inline content or a spilled locator)
    """
    if artifact_store is None:
        return {"ready": False, "count": 0, "reason": "artifact_store_unavailable"}

    if artifact_store.collection is None:
        return {"ready": False, "count": 0, "reason": "artifact_store_not_initialized"}

    try:
        raw_artifacts = await artifact_store.get_all_files(project_id)
    except Exception as e:
        logger.warning(
            "[DEPLOY] artifact_store_read_failed project_id=%s error=%s",
            project_id,
            e,
        )
        return {"ready": False, "count": 0, "reason": "artifact_store_read_failed"}

    if not raw_artifacts:
        return {"ready": False, "count": 0, "reason": "no_artifacts"}

    count = sum(1 for art in raw_artifacts if _deployable_artifact_kind(art))
    if count == 0:
        return {"ready": False, "count": 0, "reason": "empty_content"}

    return {"ready": True, "count": count, "reason": "ok"}


def get_prod_registry_config() -> Dict[str, str]:
    """Return registry host/namespace for prod-capable deploys.

    Defaults are aligned with the in-cluster docker-registry service and
    the existing `imageNamespace: AppFactory` Helm value.
    """
    registry_host = os.getenv("DEPLOY_AGENT_REGISTRY_HOST") or "docker-registry:5000"
    image_namespace = os.getenv("DEPLOY_AGENT_IMAGE_NAMESPACE") or "AppFactory"
    return {
        "registry_host": registry_host,
        "image_namespace": image_namespace,
    }


def format_prod_image_ref(
        base_slug: Optional[str],
        deployment_id: str,
        project_id: str,
) -> str:
    """Compute a stable image_ref for prod-capable deploys.

    The ref follows:
    `{registry_host}/{image_namespace}/{slug}:{deployment_id}`
    """
    import re
    cfg = get_prod_registry_config()
    slug_src = (base_slug or project_id.split("-")[0] or project_id).lower()
    # Normalize to a DNS-ish slug component
    slug = re.sub(r"[^a-z0-9-]", "-", slug_src) or "app"
    return f"{cfg['registry_host'].rstrip('/')}/{cfg['image_namespace'].strip('/')}/{slug}:{deployment_id}"


async def build_and_push_static_demo_prod(
        project_id: str,
        base_slug: Optional[str],
        deployment_id: str,
        container_manager,
) -> Dict[str, Any]:
    """Build and push a trivial static app image to the prod registry.

    This uses the same static app assets as the local prototype but targets
    the configured registry host/namespace. It does **not** create any K8s
    resources; callers are expected to use the resulting image_ref when
    creating DeploymentSummary + Kubernetes manifests.
    """
    if os.getenv("DEPLOY_AGENT_PROD_ENABLED", "false").lower() != "true":
        raise RuntimeError("Deploy Agent prod pipeline disabled")

    image_ref = format_prod_image_ref(base_slug, deployment_id, project_id)
    helper = DeployHelper(container_manager)
    res = await helper.build_and_push_static_app(project_id, image_ref)
    # Ensure image_ref is always present in the result for downstream callers
    if not res.get("image_ref"):
        res["image_ref"] = image_ref
    return res


async def _hydrate_spilled_artifact_to_host(
    archive_store,
    archive_ref_id: str,
    target_path: Path,
    *,
    budget_seconds: int = 3600,
) -> bool:
    """Stream a spilled file artifact from object storage to ``target_path`` with a
    host-side curl, so a many-hundred-MB deliverable reaches the build context
    without its bytes ever passing through this process or the LLM context.
    Returns True only on a byte-count-verified download."""
    if not archive_store or not archive_store.is_configured():
        logger.warning("[DEPLOY_BUILD] cannot hydrate %s: object storage not configured", target_path)
        return False
    record = await archive_store.get_ref(archive_ref_id)
    object_key = (record or {}).get("object_key")
    if not object_key:
        logger.warning("[DEPLOY_BUILD] cannot hydrate %s: ref %s has no stored object", target_path, archive_ref_id)
        return False
    try:
        head = await archive_store.head_blob(object_key)
    except Exception as exc:
        logger.error("[DEPLOY_BUILD] head failed hydrating %s (ref=%s): %s", target_path, archive_ref_id, exc)
        return False
    if head is None:
        logger.warning("[DEPLOY_BUILD] object gone for %s (ref=%s) — cannot hydrate", target_path, archive_ref_id)
        return False
    # curl can run ~2x budget: a retry starts just under --retry-max-time, then
    # runs a full --max-time (see docs/adr/0005). The URL TTL and the subprocess
    # ceiling below both clear that, nesting curl < signature < subprocess.
    ttl = int(budget_seconds) * 2 + 300
    try:
        url = await archive_store.presign_get(object_key, ttl)
    except Exception as exc:
        logger.error("[DEPLOY_BUILD] presign failed hydrating %s: %s", target_path, exc)
        return False

    import shlex
    q = shlex.quote
    dest = str(target_path)
    # `;` not `&&` before wc: the byte count must run even when curl exits
    # non-zero, because a missing file printing 0 IS the failure signal.
    command = (
        f"curl -fsSL --retry 3 --retry-max-time {int(budget_seconds)} "
        f'--max-time {int(budget_seconds)} -o {q(dest)} "$AppFactory_SPILL_URL"; '
        f"wc -c < {q(dest)} 2>/dev/null || echo 0"
    )
    # URL via env, never argv: run_host_cli logs the full command at INFO, where
    # a presigned URL would sit in Loki as a live bearer token for its whole TTL.
    res = await run_host_cli(
        ["bash", "-c", command],
        timeout=int(budget_seconds) * 2 + 360,
        env={"AppFactory_SPILL_URL": url},
    )
    downloaded = _trailing_byte_count(str((res or {}).get("stdout") or ""))
    if not downloaded:
        stderr = str((res or {}).get("stderr") or "").replace(url, "<presigned-url>")[:500]
        logger.error("[DEPLOY_BUILD] hydrate wrote no bytes for %s (ref=%s): %s", target_path, archive_ref_id, stderr)
        return False
    # The object's own length wins; stored size_bytes is an advisory index.
    expected = head.get("ContentLength") if isinstance(head, dict) else None
    if not isinstance(expected, int) or isinstance(expected, bool) or expected <= 0:
        expected = (record or {}).get("size_bytes")
    if isinstance(expected, int) and expected > 0 and downloaded != expected:
        logger.error("[DEPLOY_BUILD] hydrate short read for %s: got %d expected %d", target_path, downloaded, expected)
        return False
    return True


async def docker_build_and_push(host_dir: Path, image_ref: str) -> Dict[str, Any]:
    """Build ``host_dir`` as ``image_ref`` and push it, logging in first when
    registry credentials are configured. Returns {ok, steps, diagnostics};
    diagnostics names the failed step and is None on success."""
    steps: list[Dict[str, Any]] = []

    def _failure(
        last_step: str, command: Optional[str], res: Dict[str, Any]
    ) -> Dict[str, Any]:
        diagnostics = {
            "last_step": last_step,
            "failed_command": command,
            "stderr_tail": (res.get("stderr") or "")[:400],
            "stdout_tail": (res.get("stdout") or "")[:200],
        }
        return {"ok": False, "steps": steps, "diagnostics": diagnostics}

    docker_tmp = tempfile.TemporaryDirectory(prefix="AppFactory-deploy-docker-")
    login_env = {"DOCKER_CONFIG": docker_tmp.name}
    docker_env: Optional[Dict[str, str]] = None
    try:
        login_step = await maybe_registry_login(image_ref, docker_env=login_env)
        if login_step is not None:
            docker_env = login_env
            steps.append(login_step)
            if int(login_step.get("exit_code", 1)) != 0:
                return _failure("login", login_step.get("command"), login_step)

        for step, cmd in (
            ("build", ["docker", "build", "-t", image_ref, str(host_dir)]),
            ("push", ["docker", "push", image_ref]),
        ):
            res = await run_host_cli(cmd, cwd=None, env=docker_env)
            steps.append({"step": f"{step}_host", "command": " ".join(cmd), **res})
            if int(res.get("exit_code", 1)) != 0:
                return _failure(step, " ".join(cmd), res)
    finally:
        docker_tmp.cleanup()

    return {"ok": True, "steps": steps, "diagnostics": None}


async def build_and_push_from_artifacts(
        project_id: str,
        deployment_id: str,
        container_manager,
        deploy_spec: Optional[Dict[str, Any]] = None,
        storage=None,
) -> Dict[str, Any]:
    """D-19: artifact-to-image pipeline (v1: generic artifact dump).

    This helper takes whatever textual file artifacts are present in
    ArtifactStore and turns them into a simple container image by:
    - Reading `file_artifacts` via ArtifactStore
    - Writing file-like artifacts into a per-deployment app directory inside
      the container-use environment
    - Writing a simple Dockerfile that serves the app via python -m
      http.server
    - Building and pushing an image using DeployHelper.build_and_push_app_dir

    It does not attempt stack detection (Node/Flask/etc.) and does not
    enforce any particular file extensions. Deploy (GenericAgent) logic can
    choose when this generic http.server image is appropriate.

    On any pre-build error (no artifacts, no textual files, missing
    container manager, or write failures) this returns ok=False with
    diagnostics and steps instead of raising.
    """
    steps: list[Dict[str, Any]] = []

    def _artifact_load_failure(stderr_tail: str) -> Dict[str, Any]:
        diagnostics: Dict[str, Any] = {
            "last_step": "artifact_inspection",
            "failed_command": None,
            "stderr_tail": stderr_tail,
            "stdout_tail": "",
            "artifact_count": 0,
        }
        return {
            "ok": False,
            "image_ref": None,
            "diagnostics": diagnostics,
            "steps": steps,
        }

    # 1) Load artifacts from ArtifactStore (source of truth)
    raw_artifacts = []

    if storage is None:
        logger.warning(
            "[DEPLOY_BUILD] project_id=%s storage_available=false - cannot load file artifacts",
            project_id,
        )
        return _artifact_load_failure("ArtifactStore unavailable for deploy.")

    try:
        artifact_store = ArtifactStore(storage)
        await artifact_store.initialize()
    except Exception as e:
        logger.warning(
            "[DEPLOY_BUILD] project_id=%s storage_init_failed=true error=%s - failed to initialize ArtifactStore",
            project_id,
            e,
        )
        return _artifact_load_failure("Failed to initialize ArtifactStore for deploy.")

    if artifact_store.collection is None:
        logger.warning(
            "[DEPLOY_BUILD] project_id=%s artifact_store_ready=false - file_artifacts collection unavailable",
            project_id,
        )
        return _artifact_load_failure("ArtifactStore collection unavailable for deploy.")

    try:
        db_artifacts = await artifact_store.get_all_files(project_id)
        if db_artifacts:
            for art in db_artifacts:
                path = art.get("path")
                if not path:
                    continue
                if art.get("content"):
                    raw_artifacts.append({
                        "type": "file",
                        "path": path,
                        "content": art.get("content"),
                        "metadata": {"source": "file_artifacts_db"},
                    })
                elif art.get("spilled") and art.get("archive_ref_id"):
                    # Large file spilled to object storage: no inline content here.
                    # Carry the locator so the write step streams the bytes into the
                    # build context, instead of the deploy silently shipping without it.
                    raw_artifacts.append({
                        "type": "file",
                        "path": path,
                        "spilled": True,
                        "archive_ref_id": art.get("archive_ref_id"),
                        "metadata": {"source": "file_artifacts_db_spilled"},
                    })
            logger.info(
                "[DEPLOY_BUILD] project_id=%s artifacts=%d - loaded artifacts from ArtifactStore",
                project_id,
                len(raw_artifacts),
            )
    except Exception as e:
        logger.warning(
            "[DEPLOY_BUILD] project_id=%s load_failed=true error=%s - failed to load file artifacts",
            project_id,
            e,
        )
        return _artifact_load_failure("Failed to load file artifacts for deploy.")

    # Build path can't call check_artifacts_ready (it re-fetches the artifacts);
    # it shares _deployable_artifact_kind so precheck and build never diverge.
    _file_arts_check = [a for a in raw_artifacts if _deployable_artifact_kind(a)]
    ready_check = {
        "ready": len(_file_arts_check) > 0,
        "count": len(_file_arts_check),
        "reason": "ok" if _file_arts_check else "no_artifacts",
    }
    
    if not ready_check["ready"]:
        reason_map = {
            "no_artifacts": "No artifacts available for deploy.",
            "empty_content": "No file-like artifacts with textual content found for deploy.",
            "artifact_store_unavailable": "ArtifactStore unavailable for deploy.",
            "artifact_store_not_initialized": "ArtifactStore collection unavailable for deploy.",
            "artifact_store_read_failed": "Failed to load file artifacts for deploy.",
        }
        diagnostics = {
            "last_step": "artifact_inspection",
            "failed_command": None,
            "stderr_tail": reason_map.get(ready_check["reason"], ready_check["reason"]),
            "stdout_tail": "",
            "artifact_count": ready_check["count"],
        }
        return {
            "ok": False,
            "image_ref": None,
            "diagnostics": diagnostics,
            "steps": steps,
        }

    file_artifacts: list[Dict[str, Any]] = []
    for art in raw_artifacts:
        kind = _deployable_artifact_kind(art)
        if kind is None:
            continue
        path = (art.get("path") or "").strip().replace("\\", "/")
        if kind == "inline":
            file_artifacts.append({
                "path": path,
                "content": art.get("content"),
                "type": art.get("type") or "file",
            })
        else:
            file_artifacts.append({
                "path": path,
                "spilled": True,
                "archive_ref_id": art.get("archive_ref_id"),
                "type": art.get("type") or "file",
            })

    if not container_manager:
        diagnostics = {
            "last_step": "artifact_inspection",
            "failed_command": None,
            "stderr_tail": "Container manager not available for artifact-based deploy.",
            "stdout_tail": "",
            "artifact_count": len(file_artifacts),
        }
        return {
            "ok": False,
            "image_ref": None,
            "diagnostics": diagnostics,
            "steps": steps,
        }

    cm = container_manager

    # 2) Locate host repo path for this project (via ContainerManager)
    try:
        container_info = await cm.get_or_create_container(project_id)
        repo_path = container_info.get("repo_path")
    except Exception:
        repo_path = None

    if not repo_path:
        diagnostics = {
            "last_step": "repo_resolution",
            "failed_command": None,
            "stderr_tail": "Host repository path not available for artifact-based deploy.",
            "stdout_tail": "",
            "artifact_count": len(file_artifacts),
        }
        return {
            "ok": False,
            "image_ref": None,
            "diagnostics": diagnostics,
            "steps": steps,
        }

    # 3) Write artifacts into a per-deployment app directory under the host repo
    app_dir = f"deploy_artifacts_{(deployment_id or '').replace('-', '')[:8] or 'latest'}"
    host_app_dir = Path(repo_path) / app_dir
    host_app_dir.mkdir(parents=True, exist_ok=True)

    # Limit to a reasonable number so we don't overwhelm the filesystem
    max_files = 100
    archive_store_for_hydration = None
    for art in file_artifacts[:max_files]:
        rel = art["path"].lstrip("./")
        target_path = host_app_dir / rel
        try:
            target_path.parent.mkdir(parents=True, exist_ok=True)
            if art.get("spilled"):
                if archive_store_for_hydration is None:
                    archive_store_for_hydration = ArchiveStore.from_env(storage)
                hydrated = await _hydrate_spilled_artifact_to_host(
                    archive_store_for_hydration, art["archive_ref_id"], target_path
                )
                if not hydrated:
                    # Fail the whole deploy rather than skip the file: a servable
                    # site silently missing a deliverable is worse than a loud stop.
                    diagnostics = {
                        "last_step": "hydrate_spilled_artifact_host",
                        "failed_command": f"hydrate:{target_path}",
                        "stderr_tail": f"Failed to hydrate spilled artifact {art['path']} from object storage.",
                        "stdout_tail": "",
                        "artifact_count": len(file_artifacts),
                    }
                    return {
                        "ok": False,
                        "image_ref": None,
                        "diagnostics": diagnostics,
                        "steps": steps,
                    }
                steps.append(
                    {
                        "step": "hydrate_spilled_artifact_host",
                        "path": str(target_path),
                    }
                )
            else:
                target_path.write_text(art["content"], encoding="utf-8")
                steps.append(
                    {
                        "step": "write_artifact_host",
                        "path": str(target_path),
                    }
                )
        except Exception as e:
            diagnostics = {
                "last_step": "write_artifact_host",
                "failed_command": f"write_file:{target_path}",
                "stderr_tail": str(e)[:400],
                "stdout_tail": "",
                "artifact_count": len(file_artifacts),
            }
            return {
                "ok": False,
                "image_ref": None,
                "diagnostics": diagnostics,
                "steps": steps,
            }

    # 4) Write a Dockerfile appropriate for the detected stack (or a DeploySpec override)
    detection = detect_stack(file_artifacts)
    normalized_spec = None
    try:
        normalized_spec = normalize_deploy_spec(deploy_spec)
    except Exception:
        normalized_spec = None

    effective_stack = detection.stack
    try:
        if normalized_spec and normalized_spec.get("stack"):
            effective_stack = normalized_spec.get("stack")
    except Exception:
        pass

    effective_port = 8000
    try:
        if normalized_spec and isinstance(normalized_spec.get("port"), int):
            effective_port = int(normalized_spec.get("port"))
    except Exception:
        pass

    dockerfile = None
    extra_files = None
    try:
        if normalized_spec and isinstance(normalized_spec.get("dockerfile"), str):
            dockerfile = normalized_spec.get("dockerfile")
        if normalized_spec and isinstance(normalized_spec.get("extra_files"), dict):
            extra_files = normalized_spec.get("extra_files")
    except Exception:
        dockerfile = None
        extra_files = None

    if not isinstance(dockerfile, str) or not dockerfile.strip():
        dockerfile, extra_files = dockerfile_for_stack(stack=effective_stack, details=detection.details,
                                                       port=effective_port)
    dockerfile_path = host_app_dir / "Dockerfile"
    try:
        dockerfile_path.write_text(dockerfile, encoding="utf-8")
        steps.append(
            {
                "step": "dockerfile_host",
                "path": str(dockerfile_path),
                "stack": effective_stack,
            }
        )
        for rel, content in (extra_files or {}).items():
            try:
                out_path = host_app_dir / rel
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_text(content, encoding="utf-8")
                steps.append({"step": "write_extra_file", "path": str(out_path)})
            except Exception:
                continue
    except Exception as e:
        diagnostics = {
            "last_step": "dockerfile_host",
            "failed_command": f"write_file:{dockerfile_path}",
            "stderr_tail": str(e)[:400],
            "stdout_tail": "",
            "artifact_count": len(file_artifacts),
        }
        return {
            "ok": False,
            "image_ref": None,
            "diagnostics": diagnostics,
            "steps": steps,
        }

    # 5) Build and push image using host Docker
    image_ref = format_prod_image_ref(None, deployment_id, project_id)
    pushed = await docker_build_and_push(host_app_dir, image_ref)
    steps.extend(pushed["steps"])
    if not pushed["ok"]:
        return {
            "ok": False,
            "image_ref": None,
            "diagnostics": {
                **pushed["diagnostics"],
                "artifact_count": len(file_artifacts),
            },
            "steps": steps,
        }

    return {
        "ok": True,
        "image_ref": image_ref,
        "diagnostics": None,
        "steps": steps,
    }
