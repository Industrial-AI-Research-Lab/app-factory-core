"""Kubernetes deploy helper for local prototypes.

Internal helper used by local hello-world deploy CLI; not wired into
production flows yet.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
import logging
from pathlib import Path
import tempfile
import textwrap
import asyncio

from sandbox.host_cli import run_host_cli


logger = logging.getLogger(__name__)


def _tail(text: Optional[str], max_len: int = 500) -> str:
    if not text:
        return ""
    if len(text) <= max_len:
        return text
    return text[-max_len:]


def _tail_from_steps(steps: Optional[List[Dict[str, Any]]]) -> str:
    if not steps:
        return ""
    for step in reversed(steps):
        stderr = step.get("stderr") if isinstance(step, dict) else None
        if stderr:
            return _tail(str(stderr))
    return ""


class K8sDeployHelper:
    """Thin wrapper around host kubectl for local K8s deploy/teardown."""

    async def ensure_namespace(self, namespace: str) -> Dict[str, Any]:
        steps: List[Dict[str, Any]] = []

        # kubectl get ns
        get_res = await run_host_cli(["kubectl", "get", "ns", namespace], cwd=None)
        steps.append({"step": "get_namespace", "namespace": namespace, **get_res})
        if int(get_res.get("exit_code", 1)) == 0:
            return {"ok": True, "steps": steps}

        # Try to create namespace if it does not exist
        create_res = await run_host_cli(
            ["kubectl", "create", "namespace", namespace], cwd=None
        )
        steps.append({"step": "create_namespace", "namespace": namespace, **create_res})
        ok = int(create_res.get("exit_code", 1)) == 0
        return {"ok": ok, "steps": steps}

    async def deploy_static_app(
        self,
        name: str,
        image_ref: str,
        namespace: str = "AppFactory-apps",
        port: int = 8000,
    ) -> Dict[str, Any]:
        """Deploy a trivial HTTP app as Deployment + Service.

        Returns dict with:
        - ok: bool
        - steps: list of step dicts (kubectl commands, etc.)
        - diagnostics: optional error info when !ok
        """
        steps: List[Dict[str, Any]] = []

        # Ensure namespace exists
        ns = await self.ensure_namespace(namespace)
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

        # Write Deployment + Service manifest to a temp file
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

        tmp_dir = Path(tempfile.gettempdir())
        tmp_path = tmp_dir / f"AppFactory-deploy-{name}.yaml"
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

        # Wait for pod to become Ready by polling kubectl get pods.
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
                    }

            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

        # Timed out waiting for Ready pod
        stdout_tail = ""
        stderr_tail = ""
        if isinstance(last_res, dict):
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

    async def teardown_static_app(
        self,
        name: str,
        namespace: str = "AppFactory-apps",
    ) -> Dict[str, Any]:
        """Delete Deployment and Service for the static app."""
        steps: List[Dict[str, Any]] = []

        # Delete Deployment first
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

        # Then delete Service
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

        ok_dep = int(delete_dep_res.get("exit_code", 1)) == 0
        ok_svc = int(delete_svc_res.get("exit_code", 1)) == 0
        if not (ok_dep and ok_svc):
            # Prefer service error tail if present, otherwise deployment
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
