"""Deploy helper utilities.

Internal helpers for building and pushing trivial web app images via
Container-Use / ContainerManager, to be used by deploy prototypes and the
deploy agent (GenericAgent).

This module is intentionally small and focused on a minimal "hello world"
static app flow. Higher-level deploy orchestration (creating DeploymentSummary
entries, updating SharedContext, running health checks, etc.) lives elsewhere.
"""
from __future__ import annotations

from typing import Any, Dict, Optional
import logging
from pathlib import Path
import os
import shlex
import tempfile

from sandbox.container_manager import ContainerManager
from sandbox.host_cli import run_host_cli


logger = logging.getLogger(__name__)


def _tail(text: Optional[str], max_len: int = 500) -> str:
    """Return the last max_len characters of text (safe for None)."""
    if not text:
        return ""
    if len(text) <= max_len:
        return text
    return text[-max_len:]


async def maybe_registry_login(
    image_ref: str,
    docker_env: Optional[Dict[str, str]] = None,
) -> Optional[Dict[str, Any]]:
    """Perform optional docker login against the registry in image_ref.

    Uses DEPLOY_AGENT_REGISTRY_USERNAME / DEPLOY_AGENT_REGISTRY_PASSWORD /
    DEPLOY_AGENT_REGISTRY_HOST env vars. Returns a step dict suitable for
    appending to `steps` (or None if no login was attempted).

    When provided, docker_env should contain an isolated DOCKER_CONFIG
    directory that is reused for subsequent docker build/push commands.
    """

    registry_host = image_ref.split("/", 1)[0] if "/" in image_ref else image_ref
    login_username = os.getenv("DEPLOY_AGENT_REGISTRY_USERNAME")
    login_password = os.getenv("DEPLOY_AGENT_REGISTRY_PASSWORD")
    login_host_env = os.getenv("DEPLOY_AGENT_REGISTRY_HOST")

    if not login_username or not login_password:
        return None

    login_host = login_host_env or registry_host
    # Only attempt login when the configured host matches the image ref
    if login_host != registry_host:
        return None

    login_cmd = [
        "docker",
        "login",
        login_host,
        "--username",
        login_username,
        "--password-stdin",
    ]
    login_display = " ".join(shlex.quote(part) for part in login_cmd)
    login_res = await run_host_cli(
        login_cmd,
        cwd=None,
        env=docker_env,
        stdin=login_password,
    )
    return {
        "step": "login",
        "command": login_display,
        **login_res,
    }


class DeployHelper:
    """Helper for simple container-use based deploy flows.

    This helper only knows how to:
    - Write a trivial static app (index.html + optional CSS + Dockerfile)
      into the container-use environment for a project.
    - Build and push a Docker image for that app using docker CLI inside the
      environment (which is expected to talk to DIND via DOCKER_HOST when
      running in the real backend pod).

    It does **not** create Kubernetes resources or touch SharedContext directly;
    higher layers are responsible for that.
    """

    def __init__(self, container_manager: ContainerManager):
        self._cm = container_manager

    async def _write_static_app_files(
        self,
        project_id: str,
        app_dir: str = "deploy_static",
    ) -> Dict[str, Any]:
        """Write a minimal static app and Dockerfile into the container.

        Files are written under `/workdir/{app_dir}` (using paths relative to
        the container-use environment root).

        Returns a dict with at least:
        - ok: bool
        - diagnostics: Optional[dict]
        - details: list of step results
        """
        steps: list[Dict[str, Any]] = []

        # Ensure app directory exists
        mkdir_cmd = f"mkdir -p {app_dir}"
        result = await self._cm.execute_in_container(
            project_id,
            mkdir_cmd,
            cwd="/workdir",
        )
        steps.append({"step": "mkdir", "command": mkdir_cmd, **result})
        if int(result.get("exit_code", 1)) != 0:
            return {
                "ok": False,
                "diagnostics": {
                    "last_step": "dockerfile",
                    "failed_command": mkdir_cmd,
                    "stderr_tail": _tail(result.get("stderr", "")),
                },
                "details": steps,
            }

        index_html = """<!DOCTYPE html>
<html>
  <head>
    <meta charset=\"utf-8\" />
    <title>AppFactory Deploy Prototype</title>
    <link rel=\"stylesheet\" href=\"style.css\" />
  </head>
  <body>
    <main>
      <h1>AppFactory Deploy Prototype</h1>
      <p>If you can see this page, the deploy helper built and ran a trivial app.</p>
    </main>
  </body>
</html>
"""

        style_css = """body { font-family: system-ui, -apple-system, BlinkMacSystemFont, sans-serif; background: #0f172a; color: #e5e7eb; display: flex; align-items: center; justify-content: center; height: 100vh; margin: 0; } main { text-align: center; padding: 2rem 3rem; border-radius: 0.75rem; background: rgba(15,23,42,0.9); box-shadow: 0 20px 40px rgba(15,23,42,0.6); } h1 { font-size: 1.8rem; margin-bottom: 0.75rem; } p { opacity: 0.85; }
"""

        dockerfile = """FROM python:3.11-slim
WORKDIR /app
COPY . .
EXPOSE 8000
CMD ["python", "-m", "http.server", "8000"]
"""

        # Write files
        file_specs = [
            (f"{app_dir}/index.html", index_html, "index_html"),
            (f"{app_dir}/style.css", style_css, "style_css"),
            (f"{app_dir}/Dockerfile", dockerfile, "dockerfile"),
        ]

        for path, content, label in file_specs:
            res = await self._cm.write_file_in_container(project_id, path, content)
            steps.append({"step": label, "path": path, **res})
            status = res.get("status")
            exit_code = res.get("exit_code")
            # ContainerManager.write_file_in_container uses the MCP client, which
            # returns {"status": "success", "path": ...} on success and a
            # shell-style result with exit_code/stdout/stderr when the
            # container is unavailable. Treat explicit non-success status or
            # non-zero exit codes as failure; absence of exit_code with
            # status="success" is considered success.
            if (
                (status is not None and status != "success")
                or (exit_code is not None and int(exit_code) != 0)
            ):
                return {
                    "ok": False,
                    "diagnostics": {
                        "last_step": "dockerfile",
                        "failed_command": f"write_file:{path}",
                        "stderr_tail": _tail(
                            res.get("stderr")
                            or res.get("error")
                            or ""
                        ),
                    },
                    "details": steps,
                }

        return {"ok": True, "diagnostics": None, "details": steps}

    async def _build_and_push_core(
        self,
        project_id: str,
        image_ref: str,
        app_dir: str,
        steps: list[Dict[str, Any]],
        static_fallback: bool = False,
    ) -> Dict[str, Any]:
        used_host_docker = False
        repo_path: Optional[str] = None
        docker_tmp = tempfile.TemporaryDirectory(prefix="AppFactory-deploy-docker-")
        login_env = {"DOCKER_CONFIG": docker_tmp.name}
        host_docker_env: Optional[Dict[str, str]] = None

        try:
            # Optional registry login for prod-capable flows (host docker).
            login_step = await maybe_registry_login(image_ref, docker_env=login_env)
            if login_step is not None:
                host_docker_env = login_env
                steps.append(login_step)
                if int(login_step.get("exit_code", 1)) != 0:
                    return {
                        "ok": False,
                        "image_ref": None,
                        "diagnostics": {
                            "last_step": "login",
                            "failed_command": login_step.get("command"),
                            "stderr_tail": _tail(login_step.get("stderr", "")),
                        },
                        "steps": steps,
                    }

            # 2) docker build (prefer inside container-use env)
            build_cmd = f"cd {app_dir} && docker build -t {image_ref} ."
            build_res = await self._cm.execute_in_container(
                project_id,
                build_cmd,
                cwd="/workdir",
            )
            steps.append({"step": "build", "command": build_cmd, **build_res})
            build_exit = int(build_res.get("exit_code", 1))
            build_stderr = (build_res.get("stderr") or "").lower()

            if build_exit != 0:
                # If docker is missing inside the container-use environment, fall back
                # to host docker for local development. This keeps the primary design
                # (container + DIND) while allowing a working prototype on hosts
                # where docker is only available on the host.
                if build_exit == 127 or "docker: not found" in build_stderr:
                    try:
                        status = await self._cm.get_container_status(project_id)
                        repo_path = status.get("repo_path")
                    except Exception:
                        repo_path = None

                    if not repo_path:
                        return {
                            "ok": False,
                            "image_ref": None,
                            "diagnostics": {
                                "last_step": "build",
                                "failed_command": build_cmd,
                                "stderr_tail": _tail(build_res.get("stderr", "")),
                            },
                            "steps": steps,
                        }

                    used_host_docker = True
                    host_app_dir = Path(repo_path) / app_dir
                    host_app_dir.mkdir(parents=True, exist_ok=True)

                    # For static demo flows, synthesize the same trivial app files
                    # on the host that _write_static_app_files created inside the
                    # container-use environment. Generic artifact-based flows are
                    # expected to have already populated host_app_dir.
                    if static_fallback:
                        index_html = """<!DOCTYPE html>
<html>
  <head>
    <meta charset=\"utf-8\" />
    <title>AppFactory Deploy Prototype</title>
    <link rel=\"stylesheet\" href=\"style.css\" />
  </head>
  <body>
    <main>
      <h1>AppFactory Deploy Prototype</h1>
      <p>If you can see this page, the deploy helper built and ran a trivial app.</p>
    </main>
  </body>
</html>
"""

                        style_css = """body { font-family: system-ui, -apple-system, BlinkMacSystemFont, sans-serif; background: #0f172a; color: #e5e7eb; display: flex; align-items: center; justify-content: center; height: 100vh; margin: 0; } main { text-align: center; padding: 2rem 3rem; border-radius: 0.75rem; background: rgba(15,23,42,0.9); box-shadow: 0 20px 40px rgba(15,23,42,0.6); } h1 { font-size: 1.8rem; margin-bottom: 0.75rem; } p { opacity: 0.85; }
"""

                        dockerfile = """FROM python:3.11-slim
WORKDIR /app
COPY . .
EXPOSE 8000
CMD ["python", "-m", "http.server", "8000"]
"""

                        (host_app_dir / "index.html").write_text(index_html, encoding="utf-8")
                        (host_app_dir / "style.css").write_text(style_css, encoding="utf-8")
                        (host_app_dir / "Dockerfile").write_text(dockerfile, encoding="utf-8")

                    host_build_cmd = f"docker build -t {image_ref} {host_app_dir}"
                    host_build_res = await run_host_cli(
                        [
                            "docker",
                            "build",
                            "-t",
                            image_ref,
                            str(host_app_dir),
                        ],
                        cwd=None,
                        env=host_docker_env,
                    )
                    steps.append({
                        "step": "build_host",
                        "command": host_build_cmd,
                        **host_build_res,
                    })
                    if int(host_build_res.get("exit_code", 1)) != 0:
                        return {
                            "ok": False,
                            "image_ref": None,
                            "diagnostics": {
                                "last_step": "build",
                                "failed_command": host_build_cmd,
                                "stderr_tail": _tail(host_build_res.get("stderr", "")),
                            },
                            "steps": steps,
                        }
                else:
                    return {
                        "ok": False,
                        "image_ref": None,
                        "diagnostics": {
                            "last_step": "build",
                            "failed_command": build_cmd,
                            "stderr_tail": _tail(build_res.get("stderr", "")),
                        },
                        "steps": steps,
                    }

            # 3) docker push
            if used_host_docker:
                # We built the image on the host; push from the host as well.
                push_cmd = f"docker push {image_ref}"
                host_push_res = await run_host_cli(
                    ["docker", "push", image_ref],
                    cwd=None,
                    env=host_docker_env,
                )
                steps.append({"step": "push_host", "command": push_cmd, **host_push_res})
                if int(host_push_res.get("exit_code", 1)) != 0:
                    return {
                        "ok": False,
                        "image_ref": None,
                        "diagnostics": {
                            "last_step": "push",
                            "failed_command": push_cmd,
                            "stderr_tail": _tail(host_push_res.get("stderr", "")),
                        },
                        "steps": steps,
                    }
            else:
                push_cmd = f"docker push {image_ref}"
                push_res = await self._cm.execute_in_container(
                    project_id,
                    push_cmd,
                    cwd="/workdir",
                )
                steps.append({"step": "push", "command": push_cmd, **push_res})
                if int(push_res.get("exit_code", 1)) != 0:
                    return {
                        "ok": False,
                        "image_ref": None,
                        "diagnostics": {
                            "last_step": "push",
                            "failed_command": push_cmd,
                            "stderr_tail": _tail(push_res.get("stderr", "")),
                        },
                        "steps": steps,
                    }

            return {
                "ok": True,
                "image_ref": image_ref,
                "diagnostics": None,
                "steps": steps,
            }
        finally:
            docker_tmp.cleanup()

    async def build_and_push_static_app(
        self,
        project_id: str,
        image_ref: str,
        app_dir: str = "deploy_static",
    ) -> Dict[str, Any]:
        """Build and push a trivial static app image for a project.

        Args:
            project_id: AppFactory project id (used to locate container-use env).
            image_ref: Full image reference to build and push (including
                registry/repository:tag). Caller is responsible for choosing a
                sensible ref according to project/registry policy.
            app_dir: Relative directory under /workdir containing app files.

        Returns a dict suitable for feeding into DeploymentSummary.diagnostics:
        - ok: bool
        - image_ref: string (when ok)
        - diagnostics: Optional[dict] (when !ok)
        - steps: list of step results (mkdir/write/build/push)
        """
        steps: list[Dict[str, Any]] = []

        # 1) Ensure app files exist (inside container-use environment)
        prep = await self._write_static_app_files(project_id, app_dir=app_dir)
        steps.append({"step": "prepare_static_app", **{k: v for k, v in prep.items() if k != "details"}})
        steps.extend(prep.get("details", []))
        if not prep.get("ok"):
            return {
                "ok": False,
                "image_ref": None,
                "diagnostics": prep.get("diagnostics"),
                "steps": steps,
            }

        return await self._build_and_push_core(project_id, image_ref, app_dir, steps, static_fallback=True)

    async def build_and_push_app_dir(
        self,
        project_id: str,
        image_ref: str,
        app_dir: str,
        steps_prefix: Optional[list[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        steps: list[Dict[str, Any]] = list(steps_prefix or [])
        return await self._build_and_push_core(project_id, image_ref, app_dir, steps, static_fallback=False)
