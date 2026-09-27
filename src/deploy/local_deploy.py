"""Local deployment methods."""

from __future__ import annotations

from typing import Any, Dict, Optional
from datetime import datetime
import uuid
import re
import asyncio

from deploy.helpers.deploy_helper import DeployHelper
from deploy.helpers.k8s_deploy_helper import K8sDeployHelper
from sandbox.host_cli import run_host_cli
from deploy.health_checks import run_health_check_with_retry


async def deploy_static_demo_local(
    project_id: str,
    shared_context,
    event_emitter,
    container_manager,
    deploy_slug: Optional[str] = None,
    target_namespace: Optional[str] = None,
    cluster_target: Optional[str] = None,
) -> Dict[str, Any]:
    """Deploy a trivial static app for a project in local dev mode.

    This uses the existing DeployHelper + K8sDeployHelper hello-world
    pipeline but records a DeploymentSummary in SharedContext so that
    /api/projects/{id} can reflect deploy state.
    """
    namespace = target_namespace or "AppFactory-apps"

    # Base slug from project id or explicit deploy_slug
    base_slug = (deploy_slug or project_id.split("-")[0] or project_id).lower()

    # Derive a DNS-1035 compliant K8s name for Deployment/Service.
    k8s_name = re.sub(r"[^a-z0-9-]", "-", base_slug)
    if not k8s_name:
        k8s_name = "app"
    if not ("a" <= k8s_name[0] <= "z"):
        k8s_name = f"a-{k8s_name}"
    if len(k8s_name) > 63:
        k8s_name = k8s_name[:63]

    service_name = k8s_name
    image_ref = f"localhost:5000/{base_slug}:latest"

    now = datetime.utcnow().isoformat()
    deployment_id = str(uuid.uuid4())

    deployment: Dict[str, Any] = {
        "deployment_id": deployment_id,
        "project_id": project_id,
        "status": "running",
        "image_ref": image_ref,
        "namespace": namespace,
        "service_name": service_name,
        "ingress_host": "",
        "port": 8000,
        "created_at": now,
        "updated_at": now,
        "health": {
            "status": "unknown",
            "checked_at": None,
            "details": "",
        },
        "attempts": 1,
        "max_attempts": 1,
        "escalated": False,
        "diagnostics": None,
    }

    error_obj: Optional[Dict[str, Any]] = None

    # Seed initial running deployment entry + event
    await shared_context.record_deployment_result(deployment, None)
    await event_emitter.emit(
        "deploy_started",
        getattr(shared_context, "run_id", None),
        {
            "project_id": project_id,
            "deployment": deployment,
        },
    )

    try:
        helper = DeployHelper(container_manager)
        build_res = await helper.build_and_push_static_app(project_id, image_ref)
        try:
            if build_res.get("steps") is not None:
                deployment["build_steps"] = build_res.get("steps")
        except Exception:
            pass
        if not build_res.get("ok"):
            diag = build_res.get("diagnostics") or {}
            deployment["status"] = "failed"
            deployment["updated_at"] = datetime.utcnow().isoformat()
            deployment["diagnostics"] = {
                "last_step": diag.get("last_step"),
                "failed_command": diag.get("failed_command"),
                "stderr_tail": diag.get("stderr_tail"),
                "stdout_tail": diag.get("stdout_tail"),
            }
            error_obj = {
                "error": "build_or_push_failed",
                "message": "Image build/push failed for static demo app",
                "details": deployment["diagnostics"],
            }
            await shared_context.record_deployment_result(deployment, error_obj)
            await event_emitter.emit(
                "deploy_failed",
                getattr(shared_context, "run_id", None),
                {
                    "project_id": project_id,
                    "deployment": deployment,
                    "error": error_obj,
                },
            )
            return {
                "deployment": deployment,
                "deploy_status": deployment["status"],
                "error": error_obj,
            }

        try:
            target = (cluster_target or "minikube").lower()
        except Exception:
            target = "minikube"
        if target == "minikube":
            try:
                load_cmd = ["minikube", "image", "load", image_ref]
                await asyncio.wait_for(
                    run_host_cli(load_cmd, cwd=None),
                    timeout=120.0,
                )
            except Exception:
                pass

        k8s = K8sDeployHelper()
        k8s_res = await k8s.deploy_static_app(
            name=service_name,
            image_ref=image_ref,
            namespace=namespace,
            port=8000,
        )
        if not k8s_res.get("ok"):
            diag = k8s_res.get("diagnostics") or {}
            deployment["status"] = "failed"
            deployment["updated_at"] = datetime.utcnow().isoformat()
            deployment["diagnostics"] = {
                "last_step": diag.get("last_step"),
                "failed_command": diag.get("failed_command"),
                "stderr_tail": diag.get("stderr_tail"),
                "stdout_tail": diag.get("stdout_tail"),
            }
            error_obj = {
                "error": "k8s_deploy_failed",
                "message": "K8s deploy failed for static demo app",
                "details": deployment["diagnostics"],
            }
            await shared_context.record_deployment_result(deployment, error_obj)
            await event_emitter.emit(
                "deploy_failed",
                getattr(shared_context, "run_id", None),
                {
                    "project_id": project_id,
                    "deployment": deployment,
                    "error": error_obj,
                },
            )
            return {
                "deployment": deployment,
                "deploy_status": deployment["status"],
                "error": error_obj,
            }

        # Probe before deciding success/failure. Mirrors prod_deploy.py:261-316.
        # Local deploy is dev-only, but carries the same bug shape: marking
        # status="succeeded" before the probe + emitting deploy_succeeded
        # regardless of health produces a false-positive signal when the
        # static demo doesn't respond on port 8000 (build ok, K8s ok, but pod
        # never became ready or never listened on the right port).
        deployment["updated_at"] = datetime.utcnow().isoformat()
        health_result = await run_health_check_with_retry(
            namespace=namespace,
            service_name=service_name,
            port=8000,
            max_retries=3,
            retry_delay=5.0,
            pod_ready_timeout=30.0,
        )
        deployment["health"] = health_result
        deployment["updated_at"] = datetime.utcnow().isoformat()

        if deployment["health"].get("status") == "healthy":
            deployment["status"] = "succeeded"
            await shared_context.record_deployment_result(deployment, None)
            await event_emitter.emit(
                "deploy_succeeded",
                getattr(shared_context, "run_id", None),
                {
                    "project_id": project_id,
                    "deployment": deployment,
                },
            )
            return {
                "deployment": deployment,
                "deploy_status": deployment["status"],
                "error": None,
            }

        deployment["status"] = "failed"
        health_details = deployment["health"].get("details") or "health check did not pass"
        diagnostics = {
            "last_step": "health_check",
            "failed_command": "in-cluster probe on port 8000",
            "stderr_tail": health_details,
            "stdout_tail": "",
        }
        deployment["diagnostics"] = diagnostics
        error_obj = {
            "error": "health_check_failed",
            "message": f"Local deploy applied to K8s but health check failed: {health_details}",
            "details": diagnostics,
        }
        await shared_context.record_deployment_result(deployment, error_obj)
        await event_emitter.emit(
            "deploy_failed",
            getattr(shared_context, "run_id", None),
            {
                "project_id": project_id,
                "deployment": deployment,
                "error": error_obj,
            },
        )
        return {
            "deployment": deployment,
            "deploy_status": deployment["status"],
            "error": error_obj,
        }

    except Exception as e:
        deployment["status"] = "failed"
        deployment["updated_at"] = datetime.utcnow().isoformat()
        diagnostics = {
            "last_step": "exception",
            "failed_command": None,
            "stderr_tail": str(e),
        }
        deployment["diagnostics"] = diagnostics
        error_obj = {
            "error": "deploy_exception",
            "message": "Unexpected error during deploy",
            "details": diagnostics,
        }
        await shared_context.record_deployment_result(deployment, error_obj)
        await event_emitter.emit(
            "deploy_failed",
            getattr(shared_context, "run_id", None),
            {
                "project_id": project_id,
                "deployment": deployment,
                "error": error_obj,
            },
        )
        return {
            "deployment": deployment,
            "deploy_status": deployment["status"],
            "error": error_obj,
        }


async def teardown_deployment_local(
    project_id: str,
    shared_context,
    event_emitter,
    deployment_id: str,
) -> Dict[str, Any]:
    """Teardown a previously created local static demo deployment.

    Local-only helper that deletes the K8s Deployment/Service using
    K8sDeployHelper and updates SharedContext + deploy events.
    """
    deployments = shared_context.get("deployments", [])
    target: Optional[Dict[str, Any]] = None
    for d in deployments:
        if isinstance(d, dict) and d.get("deployment_id") == deployment_id:
            target = dict(d)
            break

    if not target:
        raise ValueError("Deployment not found for project")

    namespace = target.get("namespace") or "AppFactory-apps"
    service_name = target.get("service_name") or ""

    now = datetime.utcnow().isoformat()
    target["status"] = "tearing_down"
    target["updated_at"] = now
    await shared_context.record_deployment_result(target, None)
    await event_emitter.emit(
        "deploy_teardown_started",
        getattr(shared_context, "run_id", None),
        {
            "project_id": project_id,
            "deployment_id": deployment_id,
        },
    )

    k8s = K8sDeployHelper()
    k8s_res = await k8s.teardown_static_app(name=service_name, namespace=namespace)
    if not k8s_res.get("ok"):
        diag = k8s_res.get("diagnostics") or {}
        target["status"] = "failed"
        target["updated_at"] = datetime.utcnow().isoformat()
        target["diagnostics"] = {
            "last_step": diag.get("last_step"),
            "failed_command": diag.get("command"),
            "stderr_tail": diag.get("stderr_tail"),
        }
        error_obj = {
            "error": "teardown_failed",
            "message": "K8s teardown failed for static demo app",
            "details": target["diagnostics"],
        }
        await shared_context.record_deployment_result(target, error_obj)
        await event_emitter.emit(
            "deploy_failed",
            getattr(shared_context, "run_id", None),
            {
                "project_id": project_id,
                "deployment": target,
                "error": error_obj,
            },
        )
        return {
            "deployment": target,
            "deploy_status": target["status"],
            "error": error_obj,
        }

    # Success: mark as deleted
    target["status"] = "deleted"
    target["updated_at"] = datetime.utcnow().isoformat()
    target["diagnostics"] = None
    await shared_context.record_deployment_result(target, None)
    await event_emitter.emit(
        "deploy_teardown_completed",
        getattr(shared_context, "run_id", None),
        {
            "project_id": project_id,
            "deployment": target,
        },
    )
    return {
        "deployment": target,
        "deploy_status": target["status"],
        "error": None,
    }
