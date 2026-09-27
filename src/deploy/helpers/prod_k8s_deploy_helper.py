"""Kubernetes deploy helper for prod-capable deploy flows.

This helper mirrors the local K8sDeployHelper but is intended for use from the
backend pod in a real cluster. It is **not wired into any production path yet**
and all operations are hard-guarded behind DEPLOY_AGENT_PROD_ENABLED.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
import logging
from pathlib import Path
import tempfile
import textwrap
import asyncio
import os

from sandbox.host_cli import run_host_cli
from deploy.helpers.k8s_deploy_helper import _tail, _tail_from_steps


logger = logging.getLogger(__name__)


class ProdK8sDeployHelper:
    """Thin wrapper around in-cluster kubectl for prod-capable deploy/teardown.

    This helper:
    - Never auto-creates namespaces (it verifies they exist).
    - Is fully gated behind DEPLOY_AGENT_PROD_ENABLED to avoid accidental use.
    - Uses the same Deployment + Service + Ingress shape as the local helper so
      that DeploymentSummary mappings stay consistent.
    - Creates per-project Ingress resources for external URL access under
      DEPLOY_AGENT_APPS_DOMAIN (e.g., <slug>.apps.example.com).
    """

    def _check_enabled(self) -> None:
        if os.getenv("DEPLOY_AGENT_PROD_ENABLED", "false").lower() != "true":
            raise RuntimeError("Deploy Agent prod pipeline disabled")

    async def ensure_namespace_exists(self, namespace: str) -> Dict[str, Any]:
        """Verify that the target namespace exists (no auto-create)."""
        self._check_enabled()
        steps: List[Dict[str, Any]] = []

        get_res = await run_host_cli(["kubectl", "get", "ns", namespace], cwd=None)
        steps.append({"step": "get_namespace", "namespace": namespace, **get_res})

        exit_code = int(get_res.get("exit_code", 1))
        if exit_code == 0:
            # Namespace exists and is readable.
            return {"ok": True, "steps": steps}

        stderr = get_res.get("stderr", "") or ""
        # If RBAC forbids reading namespaces, do not fail early here; allow the
        # subsequent apply to surface a more precise error (either missing
        # namespace or missing permissions for the target resources).
        if "Error from server (Forbidden): namespaces" in stderr:
            return {"ok": True, "steps": steps}

        return {"ok": False, "steps": steps}

    async def deploy_app(
        self,
        name: str,
        image_ref: str,
        namespace: str = "AppFactory-apps",
        port: int = 8000,
    ) -> Dict[str, Any]:
        """Deploy an HTTP app as Deployment + Service + Ingress in a prod-capable cluster.

        Returns:
            dict with:
              ok: bool
              steps: list of step dicts
              ingress_host: str (if Ingress created, e.g. "<name>.apps.example.com")
              diagnostics: optional error info when !ok
        """
        self._check_enabled()
        steps: List[Dict[str, Any]] = []

        ns = await self.ensure_namespace_exists(namespace)
        steps.extend(ns.get("steps", []))
        if not ns.get("ok"):
            return {
                "ok": False,
                "diagnostics": {
                    "last_step": "namespace",
                    "stderr_tail": _tail_from_steps(ns.get("steps")),
                },
                "steps": steps,
            }

        manifest = textwrap.dedent(
            f"""
            apiVersion: apps/v1
            kind: Deployment
            metadata:
              name: "{name}"
              namespace: {namespace}
              labels:
                app: "{name}"
            spec:
              replicas: 1
              selector:
                matchLabels:
                  app: "{name}"
              template:
                metadata:
                  labels:
                    app: "{name}"
                spec:
                  imagePullSecrets:
                    - name: "registry-cred"
                  containers:
                    - name: app
                      image: {image_ref}
                      imagePullPolicy: IfNotPresent
                      ports:
                        - containerPort: {port}
            ---
            apiVersion: v1
            kind: Service
            metadata:
              name: "{name}"
              namespace: {namespace}
              labels:
                app: "{name}"
            spec:
              selector:
                app: "{name}"
              ports:
                - port: {port}
                  targetPort: {port}
            """
        )

        # Build Ingress manifest if apps domain is configured
        apps_domain = os.getenv("DEPLOY_AGENT_APPS_DOMAIN", "")
        tls_secret = os.getenv("DEPLOY_AGENT_APPS_TLS_SECRET", "apps-example-wildcard-tls")
        ingress_host = ""
        if apps_domain:
            ingress_host = f"{name}.{apps_domain}"
            # Use cert-manager to auto-issue TLS certs via letsencrypt-prod ClusterIssuer
            ingress_manifest = textwrap.dedent(
                f"""
                ---
                apiVersion: networking.k8s.io/v1
                kind: Ingress
                metadata:
                  name: "{name}"
                  namespace: {namespace}
                  labels:
                    app: "{name}"
                  annotations:
                    traefik.ingress.kubernetes.io/router.entrypoints: websecure
                    traefik.ingress.kubernetes.io/router.tls: "true"
                spec:
                  tls:
                    - hosts:
                        - "{ingress_host}"
                      secretName: "{tls_secret}"
                  rules:
                    - host: "{ingress_host}"
                      http:
                        paths:
                          - path: /
                            pathType: Prefix
                            backend:
                              service:
                                name: "{name}"
                                port:
                                  number: {port}
                """
            )
            manifest += ingress_manifest

        tmp_dir = Path(tempfile.gettempdir())
        tmp_path = tmp_dir / f"AppFactory-prod-deploy-{name}.yaml"
        tmp_path.write_text(manifest, encoding="utf-8")

        apply_cmd = ["kubectl", "apply", "-f", str(tmp_path)]
        apply_res = await run_host_cli(apply_cmd, cwd=None)
        steps.append(
            {
                "step": "kubectl_apply",
                "command": " ".join(apply_cmd),
                "manifest_path": str(tmp_path),
                **apply_res,
            }
        )
        if int(apply_res.get("exit_code", 1)) != 0:
            return {
                "ok": False,
                "diagnostics": {
                    "last_step": "kubectl_apply",
                    "stderr_tail": _tail(apply_res.get("stderr", "")),
                },
                "steps": steps,
            }

        max_wait_seconds = 90
        poll_interval = 3
        elapsed = 0
        last_res: Optional[Dict[str, Any]] = None

        while elapsed < max_wait_seconds:
            get_cmd = [
                "kubectl",
                "get",
                "pods",
                "-n",
                namespace,
                "-l",
                f"app={name}",
            ]
            get_res = await run_host_cli(get_cmd, cwd=None)
            steps.append(
                {
                    "step": "kubectl_get_pods",
                    "command": " ".join(get_cmd),
                    **get_res,
                }
            )
            last_res = get_res

            if int(get_res.get("exit_code", 1)) == 0:
                out = (get_res.get("stdout") or "")
                # Heuristic: look for a Running pod with 1/1 ready.
                if "Running" in out and "1/1" in out:
                    return {
                        "ok": True,
                        "steps": steps,
                        "namespace": namespace,
                        "name": name,
                        "ingress_host": ingress_host,
                    }

            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

        stdout_tail = ""
        stderr_tail = ""
        if isinstance(last_res, Dict):  # type: ignore[arg-type]
            stdout_tail = _tail(last_res.get("stdout", ""))
            stderr_tail = _tail(last_res.get("stderr", ""))
        return {
            "ok": False,
            "diagnostics": {
                "last_step": "kubectl_get_pods",
                "stdout_tail": stdout_tail,
                "stderr_tail": stderr_tail,
            },
            "steps": steps,
        }

    async def teardown_app(
        self,
        name: str,
        namespace: str = "AppFactory-apps",
    ) -> Dict[str, Any]:
        """Delete Deployment, Service, and Ingress for the app in a prod-capable cluster."""
        self._check_enabled()
        steps: List[Dict[str, Any]] = []

        delete_dep_cmd = [
            "kubectl",
            "delete",
            "deployment",
            name,
            "-n",
            namespace,
            "--ignore-not-found=true",
        ]
        delete_dep_res = await run_host_cli(delete_dep_cmd, cwd=None)
        steps.append(
            {
                "step": "kubectl_delete_deployment",
                "command": " ".join(delete_dep_cmd),
                **delete_dep_res,
            }
        )

        delete_svc_cmd = [
            "kubectl",
            "delete",
            "service",
            name,
            "-n",
            namespace,
            "--ignore-not-found=true",
        ]
        delete_svc_res = await run_host_cli(delete_svc_cmd, cwd=None)
        steps.append(
            {
                "step": "kubectl_delete_service",
                "command": " ".join(delete_svc_cmd),
                **delete_svc_res,
            }
        )

        # Also delete Ingress if it exists
        delete_ing_cmd = [
            "kubectl",
            "delete",
            "ingress",
            name,
            "-n",
            namespace,
            "--ignore-not-found=true",
        ]
        delete_ing_res = await run_host_cli(delete_ing_cmd, cwd=None)
        steps.append(
            {
                "step": "kubectl_delete_ingress",
                "command": " ".join(delete_ing_cmd),
                **delete_ing_res,
            }
        )

        ok_dep = int(delete_dep_res.get("exit_code", 1)) == 0
        ok_svc = int(delete_svc_res.get("exit_code", 1)) == 0
        ok_ing = int(delete_ing_res.get("exit_code", 1)) == 0
        if not (ok_dep and ok_svc and ok_ing):
            stderr_tail = _tail(
                delete_svc_res.get("stderr")
                or delete_dep_res.get("stderr")
                or ""
            )
            return {
                "ok": False,
                "steps": steps,
                "diagnostics": {
                    "last_step": "kubectl_delete",
                    "stderr_tail": stderr_tail,
                },
            }

        return {"ok": True, "steps": steps}
