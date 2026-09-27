from __future__ import annotations

from typing import Any, Dict, Optional
from datetime import datetime
import uuid

from deploy.helpers.prod_k8s_deploy_helper import ProdK8sDeployHelper
from deploy.health_checks import (
    run_health_check_with_retry,
    run_external_url_health_check,
    combine_health_results,
)


async def redeploy_image_prod(
    *,
    project_id: str,
    shared_context,
    event_emitter,
    namespace: str,
    service_name: str,
    image_ref: str,
    port: int = 8000,
    rollback_of_deployment_id: Optional[str] = None,
    retry_of_deployment_id: Optional[str] = None,
) -> Dict[str, Any]:
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
        "port": int(port),
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

    if rollback_of_deployment_id:
        deployment["rollback_of_deployment_id"] = rollback_of_deployment_id
    if retry_of_deployment_id:
        deployment["retry_of_deployment_id"] = retry_of_deployment_id

    await shared_context.record_deployment_result(deployment, None)
    await event_emitter.emit(
        "deploy_started",
        getattr(shared_context, "run_id", None),
        {
            "project_id": project_id,
            "deployment": deployment,
        },
    )

    error_obj: Optional[Dict[str, Any]] = None

    try:
        k8s = ProdK8sDeployHelper()
        k8s_res = await k8s.deploy_app(
            name=service_name,
            image_ref=image_ref,
            namespace=namespace,
            port=int(port),
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
                "message": "K8s redeploy failed (prod)",
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

        ingress_host = k8s_res.get("ingress_host", "")
        deployment["ingress_host"] = ingress_host
        deployment["updated_at"] = datetime.utcnow().isoformat()

        in_cluster_health = await run_health_check_with_retry(
            namespace=namespace,
            service_name=service_name,
            port=int(port),
            max_retries=3,
            retry_delay=5.0,
            pod_ready_timeout=30.0,
        )

        external_health: Optional[Dict[str, Any]] = None
        if ingress_host:
            external_url = f"https://{ingress_host}/"
            external_health = await run_external_url_health_check(
                url=external_url,
                timeout=10.0,
                verify_ssl=False,
            )

        deployment["health"] = combine_health_results(in_cluster_health, external_health)
        deployment["updated_at"] = datetime.utcnow().isoformat()

        # Health gates the success event. Mirrors prod_deploy.py:261-316.
        # Previously status="succeeded" was set right after `kubectl apply`
        # returned 0 and deploy_succeeded fired regardless of probe outcome —
        # same anti-pattern that produced the false-positive on project
        # 21b8c9e1 on the initial-deploy path, just on the retry/rollback
        # entry. Unhealthy → status="failed" + deploy_failed with diagnostics,
        # so the orchestrator's retry loop and the UI's failure path both get
        # a real signal during the precise moment (retry / rollback) the user
        # is debugging an incident.
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
            "failed_command": f"in-cluster + external probe on port {int(port)}",
            "stderr_tail": health_details,
            "stdout_tail": "",
        }
        deployment["diagnostics"] = diagnostics
        error_obj = {
            "error": "health_check_failed",
            "message": f"Redeploy applied to K8s but health check failed: {health_details}",
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
            "message": "Unexpected error during redeploy (prod)",
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
