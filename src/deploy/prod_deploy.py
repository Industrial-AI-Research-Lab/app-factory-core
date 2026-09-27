"""Production deployment methods."""

from __future__ import annotations

from typing import Any, Dict, Optional
from datetime import datetime
import uuid
import re
import os

from deploy.helpers.prod_k8s_deploy_helper import ProdK8sDeployHelper
from deploy.deploy_spec import normalize_deploy_spec
from deploy.health_checks import (
    run_health_check_with_retry,
    run_external_url_health_check,
    combine_health_results,
)
from deploy.build_helpers import (
    build_and_push_static_demo_prod as _build_static_prod,
    build_and_push_from_artifacts as _build_from_artifacts,
)


async def deploy_from_artifacts_prod(
    project_id: str,
    shared_context,
    event_emitter,
    container_manager,
    deploy_slug: Optional[str] = None,
    target_namespace: Optional[str] = None,
    deploy_spec: Optional[Dict[str, Any]] = None,
    storage=None,
) -> Dict[str, Any]:
    """Deploy a project based on its artifacts into a prod-capable cluster.

    This uses the generic artifact-to-image pipeline (build_and_push_from_artifacts)
    to build an http.server-based image from SharedContext.artifacts, then
    deploys it via ProdK8sDeployHelper into the configured namespace.

    It reuses the same DeploymentSummary / event patterns as
    deploy_static_demo_prod so the UI can treat both paths uniformly.
    """

    if os.getenv("DEPLOY_AGENT_PROD_ENABLED", "false").lower() != "true":
        raise RuntimeError("Deploy Agent prod pipeline disabled")

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

    # Single source of truth for the container port. The Dockerfile EXPOSE
    # line, the K8s Service targetPort, and the in-cluster health probe
    # MUST all read the same value — otherwise the Service routes traffic
    # to a port nothing is listening on and the ingress returns 502
    # ("Bad Gateway") even though `kubectl apply` succeeded. Verified on
    # project 21b8c9e1 where the LLM put `deploy_spec.port=80`, the
    # Dockerfile rendered `CMD http.server 80`, but the K8s Service was
    # created with the previously-hardcoded port=8000 — pod up, ingress
    # 502, three health-check retries failed, and `deploy_succeeded` was
    # nevertheless emitted because the success path didn't gate on health.
    normalized_spec = normalize_deploy_spec(deploy_spec) or {}
    effective_port = normalized_spec.get("port") if isinstance(normalized_spec.get("port"), int) else 8000

    now = datetime.utcnow().isoformat()
    deployment_id = str(uuid.uuid4())

    deployment: Dict[str, Any] = {
        "deployment_id": deployment_id,
        "project_id": project_id,
        "status": "running",
        "image_ref": None,
        "namespace": namespace,
        "service_name": service_name,
        "ingress_host": "",
        "port": effective_port,
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
        build_res = await _build_from_artifacts(
            project_id=project_id,
            deployment_id=deployment_id,
            container_manager=container_manager,
            deploy_spec=deploy_spec,
            storage=storage,
        )
        image_ref = build_res.get("image_ref")
        deployment["image_ref"] = image_ref
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
                "message": "Image build/push failed for artifact-based app (prod)",
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

        if not image_ref:
            # Defensive: treat missing image_ref as a build failure.
            deployment["status"] = "failed"
            deployment["updated_at"] = datetime.utcnow().isoformat()
            diagnostics = {
                "last_step": "build",
                "failed_command": None,
                "stderr_tail": "build helper returned ok without image_ref",
            }
            deployment["diagnostics"] = diagnostics
            error_obj = {
                "error": "build_or_push_failed",
                "message": "Image build/push did not return image_ref",
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

        k8s = ProdK8sDeployHelper()
        k8s_res = await k8s.deploy_app(
            name=service_name,
            image_ref=image_ref,
            namespace=namespace,
            port=effective_port,
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
                "message": "K8s deploy failed for artifact-based app (prod)",
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

        # Capture ingress_host from k8s deploy result
        ingress_host = k8s_res.get("ingress_host", "")
        deployment["ingress_host"] = ingress_host

        # Run in-cluster health check with pod readiness wait and retry logic.
        # `port=effective_port` is intentional: this MUST match the Service
        # targetPort and the Dockerfile EXPOSE — see the comment above where
        # `effective_port` is derived. Hardcoding 8000 here while the Service
        # forwarded to a different port produced the false-success that hid
        # the 502 from project 21b8c9e1.
        in_cluster_health = await run_health_check_with_retry(
            namespace=namespace,
            service_name=service_name,
            port=effective_port,
            max_retries=3,
            retry_delay=5.0,
            pod_ready_timeout=30.0,
        )

        # Run external URL health check if ingress_host is available
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

        # Health gates the success event. Previously `status="succeeded"` was
        # set right after `kubectl apply` returned 0, BEFORE health checks ran,
        # and `deploy_succeeded` was emitted unconditionally — `deployment.health`
        # was just metadata, not a gate. That turned a real Bad-Gateway failure
        # into a fake success on project 21b8c9e1 (3 health retries failed,
        # event still fired). Now: unhealthy → status="failed" + deploy_failed,
        # which feeds the orchestrator's retry loop with a real signal.
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

        # Unhealthy or unknown after retries — surface as a real failure so
        # the deploy_agent's analyze_and_repair tool gets called with the
        # health diagnostics and the orchestrator's retry loop fires.
        deployment["status"] = "failed"
        health_details = deployment["health"].get("details") or "health check did not pass"
        diagnostics = {
            "last_step": "health_check",
            "failed_command": f"in-cluster + external probe on port {effective_port}",
            "stderr_tail": health_details,
            "stdout_tail": "",
        }
        deployment["diagnostics"] = diagnostics
        error_obj = {
            "error": "health_check_failed",
            "message": f"Deploy applied to K8s but health check failed: {health_details}",
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
            "message": "Unexpected error during deploy (prod, artifacts)",
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


async def deploy_static_demo_prod(
    project_id: str,
    shared_context,
    event_emitter,
    container_manager,
    deploy_slug: Optional[str] = None,
    target_namespace: Optional[str] = None,
) -> Dict[str, Any]:
    """Deploy a trivial static app for a project in a prod-capable cluster.

    This is an experimental, admin-only path intended to validate the
    prod plumbing end-to-end. It uses the same trivial static app and
    Dockerfile template as the local prototype, but targets the configured
    internal registry and `AppFactory-apps` namespace.
    """
    if os.getenv("DEPLOY_AGENT_PROD_ENABLED", "false").lower() != "true":
        raise RuntimeError("Deploy Agent prod pipeline disabled")

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

    now = datetime.utcnow().isoformat()
    deployment_id = str(uuid.uuid4())

    deployment: Dict[str, Any] = {
        "deployment_id": deployment_id,
        "project_id": project_id,
        "status": "running",
        "image_ref": None,
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
        build_res = await _build_static_prod(
            project_id=project_id,
            base_slug=base_slug,
            deployment_id=deployment_id,
            container_manager=container_manager,
        )
        image_ref = build_res.get("image_ref")
        deployment["image_ref"] = image_ref
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
                "message": "Image build/push failed for static demo app (prod)",
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

        if not image_ref:
            deployment["status"] = "failed"
            deployment["updated_at"] = datetime.utcnow().isoformat()
            diagnostics = {
                "last_step": "build",
                "failed_command": None,
                "stderr_tail": "build helper returned ok without image_ref",
            }
            deployment["diagnostics"] = diagnostics
            error_obj = {
                "error": "build_or_push_failed",
                "message": "Image build/push did not return image_ref",
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

        k8s = ProdK8sDeployHelper()
        k8s_res = await k8s.deploy_app(
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
                "message": "K8s deploy failed for static demo app (prod)",
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

        # Capture ingress_host from k8s deploy result
        ingress_host = k8s_res.get("ingress_host", "")
        deployment["ingress_host"] = ingress_host

        # Run in-cluster health check
        in_cluster_health = await run_health_check_with_retry(
            namespace=namespace,
            service_name=service_name,
            port=8000,
            max_retries=3,
            retry_delay=5.0,
            pod_ready_timeout=30.0,
        )

        # Run external URL health check if ingress_host is available
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

        # Health gates the success event — see deploy_from_artifacts_prod for
        # the rationale (project 21b8c9e1 had a real Bad-Gateway failure
        # masked by `status="succeeded"` set before health ran).
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
            "failed_command": "in-cluster + external probe on port 8000",
            "stderr_tail": health_details,
            "stdout_tail": "",
        }
        deployment["diagnostics"] = diagnostics
        error_obj = {
            "error": "health_check_failed",
            "message": f"Static demo applied to K8s but health check failed: {health_details}",
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
            "message": "Unexpected error during deploy (prod)",
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


async def teardown_deployment_prod(
    project_id: str,
    shared_context,
    event_emitter,
    deployment_id: str,
) -> Dict[str, Any]:
    """Teardown a previously created prod static demo deployment.

    Prod-capable helper that deletes the K8s Deployment/Service/Ingress
    using ProdK8sDeployHelper and updates SharedContext + deploy events.
    """
    if os.getenv("DEPLOY_AGENT_PROD_ENABLED", "false").lower() != "true":
        raise RuntimeError("Deploy Agent prod pipeline disabled")

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

    k8s = ProdK8sDeployHelper()
    k8s_res = await k8s.teardown_app(name=service_name, namespace=namespace)
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
            "message": "K8s teardown failed for static demo app (prod)",
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
