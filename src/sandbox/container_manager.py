# Container Manager: manages container lifecycle per project using Container-Use

from typing import Dict, Any, Optional
from pathlib import Path
import asyncio
import shutil
from .mcp_client_sdk import ContainerUseMCPClient
from .repo_manager import RepoManager
from .host_cli import run_host_cli
from .cu_mcp_inventory import CU_RECOVERY_RECREATE_TIMEOUT_SECONDS
import os
import logging
import platform
import re

# container-use writes every command it runs into a git note verbatim, and
# `cu log` prints those notes back. Any signed URL that rode in on a command
# line is therefore sitting in output we hand to an API response, a Mongo
# snapshot and the UI. A presigned URL is a bearer capability — it needs no
# login and cannot be revoked before it expires — so it is removed here, at the
# one point both publishers share, rather than at each of them.
#
# This does NOT reach the note on the host's disk; it covers what leaves the
# host. It is also the only thing that can help notes already written by runs
# that have already happened.
# Stops at whitespace or a quote so the surrounding command stays readable —
# a redaction that also eats the closing quote and the next operator makes the
# log look corrupted, which is its own kind of unhelpful.
_SIGNED_URL_RE = re.compile(r"""https?://[^\s'"]*?[?&]X-Amz-[A-Za-z-]+=[^\s'"]*""")


def scrub_signed_urls(text: str) -> str:
    """Blank out presigned URLs in text bound for a durable or shared surface."""
    if not text:
        return text
    return _SIGNED_URL_RE.sub("<presigned-url-redacted>", text)

logger = logging.getLogger(__name__)


def _trailing_byte_count(stdout: str) -> Optional[int]:
    """The last integer token of a hydrate command's stdout — the `wc -c` count
    that runs after curl. None when nothing numeric was printed (the sandbox
    swallowed the command); 0 means curl wrote no file, the failure signal."""
    for token in reversed((stdout or "").split()):
        try:
            return int(token)
        except ValueError:
            continue
    return None


class ContainerManager:
    
    def __init__(
        self, 
        cli_path: str = "cu",
        enabled: bool = True,
        repositories_root: Optional[str] = None,
        artifact_store=None,
        archive_store=None,
    ):
        """
        Initialize container manager.

        Args:
            base_image: Docker base image
            cli_path: Path to container-use CLI
            enabled: Whether container execution is enabled
            artifact_store: ArtifactStore instance for DB persistence
            archive_store: ArchiveStore used to hydrate files that were spilled to
                object storage back into the container on recovery. Optional; when
                absent, recovery builds one from the environment per call.
        """
        self.cli_path = cli_path
        self.enabled = enabled
        self.repositories_root = repositories_root or os.getenv("REPOSITORIES_ROOT", "C:/work/repositories")
        self.repo_manager = RepoManager(self.repositories_root)
        self.containers: Dict[str, Dict[str, Any]] = {}
        self.artifact_store = artifact_store
        self.archive_store = archive_store
        # After a failed post-timeout recovery every container call for this
        # project fails fast until message revert, snapshot restore, a successful
        # timeout recovery or an explicit /recover clears it; get_or_create never does.
        self._session_unavailable: Dict[str, str] = {}
        # Survives cleanup_container so /recover can build a replacement from the
        # old environment's branch instead of seeding a fresh AppFactory-{id}.
        self._preserved_workspaces: Dict[str, Dict[str, Optional[str]]] = {}
        
        logger.info(f"ContainerManager initialized (enabled={enabled})")
    
    def mark_session_unavailable(self, project_id: str, *, reason: str) -> None:
        buckets = getattr(self, "_session_unavailable", None)
        if buckets is None:
            buckets = {}
            self._session_unavailable = buckets
        buckets[project_id] = reason or "unavailable"

    def clear_session_unavailable(self, project_id: str) -> None:
        buckets = getattr(self, "_session_unavailable", None)
        if buckets is not None:
            buckets.pop(project_id, None)

    def session_unavailable_reason(self, project_id: str) -> Optional[str]:
        buckets = getattr(self, "_session_unavailable", None) or {}
        return buckets.get(project_id)

    def remember_workspace(
        self,
        project_id: str,
        *,
        repo_path: str,
        environment_id: Optional[str] = None,
    ) -> None:
        if not repo_path:
            return
        buckets = getattr(self, "_preserved_workspaces", None)
        if buckets is None:
            buckets = {}
            self._preserved_workspaces = buckets
        buckets[project_id] = {
            "repo_path": str(repo_path),
            "environment_id": environment_id,
        }

    def preserved_workspace(self, project_id: str) -> Optional[Dict[str, Optional[str]]]:
        buckets = getattr(self, "_preserved_workspaces", None) or {}
        return buckets.get(project_id)

    def clear_preserved_workspace(self, project_id: str) -> None:
        buckets = getattr(self, "_preserved_workspaces", None)
        if buckets is not None:
            buckets.pop(project_id, None)

    def _unavailable_container_result(self, project_id: str, reason: str) -> Dict[str, Any]:
        return {
            "project_id": project_id,
            "client": None,
            "status": "unavailable",
            "error": f"MCP session unavailable after timeout recovery: {reason}",
            "environment_id": None,
            "repo_path": None,
        }

    def _session_unavailable_tool_result(
        self,
        project_id: str,
        container: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        from schemas.infra_error import InfraErrorType

        unavailable = self.session_unavailable_reason(project_id)
        if not unavailable and not (container and container.get("status") == "unavailable"):
            return None
        reason = unavailable or "unavailable"
        return {
            "stdout": "",
            "stderr": f"MCP session unavailable after timeout recovery: {reason}",
            "exit_code": 1,
            "session_state": "unavailable",
            "error_type": InfraErrorType.SESSION_UNAVAILABLE.value,
            "code": InfraErrorType.SESSION_UNAVAILABLE.value,
        }

    async def get_or_create_container(self, project_id: str) -> Dict:
        if not self.enabled:
            logger.warning("Container execution disabled. Using simulation.")
            return {"project_id": project_id, "status": "simulated"}

        unavailable = self.session_unavailable_reason(project_id)
        if unavailable:
            logger.warning(
                "[SESSION_UNAVAILABLE] refuse get_or_create project_id=%s reason=%s",
                project_id,
                unavailable,
            )
            return self._unavailable_container_result(project_id, unavailable)
        
        if project_id in self.containers:
            return self.containers[project_id]

        preserved = self.preserved_workspace(project_id)
        if preserved and preserved.get("repo_path"):
            path = Path(str(preserved["repo_path"]))
            if not path.is_dir():
                logger.warning(
                    "[PRESERVED] path missing project_id=%s repo=%s — abandon and seed",
                    project_id,
                    path,
                )
                self.clear_preserved_workspace(project_id)
            else:
                logger.info(
                    "[PRESERVED] reopen-or-replace instead of seed project_id=%s repo=%s",
                    project_id,
                    path,
                )
                from sandbox.run_command_recovery import reopen_or_replace_on_workspace

                try:
                    container, recovery_mode = await reopen_or_replace_on_workspace(
                        self,
                        project_id,
                        repo_path=str(path),
                        previous_environment_id=preserved.get("environment_id"),
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as restore_err:
                    if isinstance(restore_err, asyncio.TimeoutError):
                        reason = (
                            f"recreate exceeded "
                            f"{CU_RECOVERY_RECREATE_TIMEOUT_SECONDS:g}s"
                        )
                    else:
                        reason = str(restore_err) or type(restore_err).__name__
                    logger.warning(
                        "[PRESERVED] reopen/replace failed project_id=%s reason=%s — "
                        "fail-closed, keep preserved",
                        project_id,
                        reason,
                    )
                    self.mark_session_unavailable(project_id, reason=reason)
                    return self._unavailable_container_result(project_id, reason)
                self.clear_preserved_workspace(project_id)
                logger.info(
                    "[PRESERVED] restored project_id=%s mode=%s env=%s",
                    project_id,
                    recovery_mode,
                    container.get("environment_id"),
                )
                return container
        
        # Create per-project repo first (temporary name based on project)
        temp_repo_name = f"AppFactory-{project_id}"
        repo_path = await asyncio.to_thread(self.repo_manager.ensure_repo, temp_repo_name)
        # Ensure the repo has the project's .container-use config so environment is initialized per config
        try:
            project_root = Path(__file__).resolve().parents[2]
            src_cfg = project_root / ".container-use"
            dst_cfg = Path(repo_path) / ".container-use"
            if src_cfg.exists() and not dst_cfg.exists():
                shutil.copytree(src_cfg, dst_cfg)
                logger.info(f"Copied .container-use config into repo: {dst_cfg}")
        except Exception as copy_err:
            logger.warning(f"Could not copy .container-use config: {copy_err}")
        # Use MCP SDK client bound to this repo as environment_source
        client = ContainerUseMCPClient(project_id, self.cli_path, environment_source=str(repo_path))
        
        try:
            # Preflight: verify container-use can operate in this repo
            pre = await run_host_cli([self.cli_path, "list"], cwd=str(repo_path))
            try:
                logger.info(
                    f"cu list preflight: exit_code={pre.get('exit_code')} stdout_len={len(pre.get('stdout',''))} stderr={pre.get('stderr','')[:200]}"
                )
            except Exception:
                pass
            if int(pre.get("exit_code", 1)) != 0:
                self.containers[project_id] = {
                    "project_id": project_id,
                    "client": None,
                    "status": "simulated",
                    "error": f"cu list failed: {pre.get('stderr','')[:400]}",
                    "environment_id": None,
                    "repo_path": str(repo_path),
                }
                return self.containers[project_id]
            # Connect to container-use MCP server
            await client.connect()
            
            # Create environment (config taken from .container-use/environment.json inside repo)
            await client.create_environment()
            env_id = getattr(client, "environment_id", None)
            # If environment id is available, optionally rename repo to match it (skip on Windows due to file locks)
            if isinstance(env_id, str) and env_id:
                if platform.system() != "Windows":
                    try:
                        new_repo_path = self.repo_manager.rename_repo(Path(repo_path), env_id)
                        repo_path = new_repo_path
                        # Update client environment_source to the renamed path
                        client.environment_source = str(repo_path)
                    except Exception as re:
                        logger.warning(f"Repo rename skipped: {re}")
                else:
                    logger.info("Skipping repo rename on Windows to avoid file lock issues")
            
            self.containers[project_id] = {
                "project_id": project_id,
                "client": client,
                "status": "running",
                "branch": f"AppFactory-{project_id}",
                "environment_id": env_id,
                "repo_path": str(repo_path),
            }
            
            logger.info(f"Container created for project: {project_id}")
            return self.containers[project_id]
            
        except asyncio.CancelledError:
            # wait_for(recreate) cancel: close is time-bounded; re-raise so the
            # TimeoutError path in recovery can mark unavailable (do not return
            # simulated and pretend recreate succeeded).
            try:
                await client.close()
            except Exception as close_err:
                logger.warning(f"Error during client.close(): {close_err}")
            raise
        except Exception as e:
            logger.error(f"Failed to create container: {e}")
            logger.warning("Falling back to simulation mode")

            # Close failed client (best-effort; never propagate)
            try:
                await client.close()
            except Exception as close_err:
                logger.warning(f"Error during client.close(): {close_err}")

            # Fall back to simulation
            self.containers[project_id] = {
                "project_id": project_id,
                "client": None,
                "status": "simulated",
                "error": str(e),
                "environment_id": None,
                "repo_path": None,
            }
            return self.containers[project_id]

    async def reopen_container_workspace(
        self,
        project_id: str,
        *,
        repo_path: str,
        environment_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Reconnect MCP to the same CU environment_id on an existing workspace."""
        if not self.enabled:
            return {"project_id": project_id, "status": "simulated", "client": None}

        path = Path(repo_path)
        if not path.is_dir():
            raise RuntimeError(f"preserved workspace missing: {repo_path}")

        if project_id in self.containers:
            existing = self.containers[project_id]
            if existing.get("client") and str(existing.get("repo_path")) == str(path):
                return existing

        client = ContainerUseMCPClient(
            project_id,
            self.cli_path,
            environment_source=str(path),
            environment_id=environment_id,
        )
        try:
            await client.connect()
            opened = await client.open_environment()
            env_id = opened or environment_id or getattr(client, "environment_id", None)
            if not isinstance(env_id, str) or not env_id:
                raise RuntimeError("environment_open did not yield environment_id")
            client.environment_id = env_id
            client.environment_source = str(path)
            self.containers[project_id] = {
                "project_id": project_id,
                "client": client,
                "status": "running",
                "branch": f"AppFactory-{project_id}",
                "environment_id": env_id,
                "repo_path": str(path),
            }
            logger.info(
                "Container reopened for project=%s env=%s repo=%s",
                project_id,
                env_id,
                path,
            )
            return self.containers[project_id]
        except asyncio.CancelledError:
            try:
                await client.close()
            except Exception as close_err:
                logger.warning(f"Error during client.close(): {close_err}")
            raise
        except Exception:
            try:
                await client.close()
            except Exception as close_err:
                logger.warning(f"Error during client.close(): {close_err}")
            raise

    async def resolve_environment_git_ref(
        self,
        repo_path: str,
        environment_id: str,
    ) -> str:
        """Return a ref to the previous environment tip in the workspace repo.

        Raises if none resolves. Why not workspace HEAD: see
        ``replace_environment_on_workspace``.
        """
        if not environment_id:
            raise RuntimeError("previous environment_id required for from_git_ref")

        fetch = await run_host_cli(
            ["git", "fetch", "container-use", environment_id],
            cwd=str(repo_path),
            timeout=60,
        )
        fetch_ok = int(fetch.get("exit_code", 1)) == 0
        if not fetch_ok:
            logger.warning(
                "[REPLACE_ENV] git fetch container-use %s failed exit=%s stderr=%s",
                environment_id,
                fetch.get("exit_code"),
                (fetch.get("stderr") or "")[:200],
            )

        candidates = [
            f"container-use/{environment_id}",
            f"refs/remotes/container-use/{environment_id}",
            environment_id,
        ]
        if fetch_ok:
            candidates.append("FETCH_HEAD")

        for ref in candidates:
            check = await run_host_cli(
                ["git", "rev-parse", "--verify", ref],
                cwd=str(repo_path),
                timeout=30,
            )
            if int(check.get("exit_code", 1)) == 0 and (check.get("stdout") or "").strip():
                logger.info(
                    "[REPLACE_ENV] resolved from_git_ref=%s for previous env=%s repo=%s",
                    ref,
                    environment_id,
                    repo_path,
                )
                return ref
        raise RuntimeError(
            f"cannot resolve previous environment tip {environment_id!r} in {repo_path}"
        )

    async def replace_environment_on_workspace(
        self,
        project_id: str,
        *,
        repo_path: str,
        previous_environment_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a new CU environment on an existing workspace folder.

        Fallback in reopen_or_replace_on_workspace when the old environment
        cannot be reopened. Keep the git workspace; do not call ensure_repo /
        seed AppFactory-{id}.

        New env is created from ``previous_environment_id`` tip via CU
        ``from_git_ref``. Creating from workspace HEAD would drop files written
        earlier in the execution phase that only live on the old env branch.
        Packages, /tmp, and uncommitted workdir files are not in that tip.
        """
        if not self.enabled:
            return {"project_id": project_id, "status": "simulated", "client": None}

        path = Path(repo_path)
        if not path.is_dir():
            raise RuntimeError(f"preserved workspace missing: {repo_path}")
        if not previous_environment_id:
            raise RuntimeError(
                "replace requires previous_environment_id so from_git_ref "
                "keeps in-phase files"
            )

        git_ref = await self.resolve_environment_git_ref(
            str(path), previous_environment_id
        )

        client = ContainerUseMCPClient(
            project_id,
            self.cli_path,
            environment_source=str(path),
            environment_id=None,
        )
        try:
            await client.connect()
            await client.create_environment(from_git_ref=git_ref)
            env_id = getattr(client, "environment_id", None)
            if not isinstance(env_id, str) or not env_id:
                raise RuntimeError("environment_create did not yield environment_id")
            client.environment_source = str(path)
            self.containers[project_id] = {
                "project_id": project_id,
                "client": client,
                "status": "running",
                "branch": f"AppFactory-{project_id}",
                "environment_id": env_id,
                "repo_path": str(path),
            }
            logger.info(
                "Container env replaced for project=%s env=%s "
                "previous=%s from_git_ref=%s repo=%s",
                project_id,
                env_id,
                previous_environment_id,
                git_ref,
                path,
            )
            return self.containers[project_id]
        except asyncio.CancelledError:
            try:
                await client.close()
            except Exception as close_err:
                logger.warning(f"Error during client.close(): {close_err}")
            raise
        except Exception:
            try:
                await client.close()
            except Exception as close_err:
                logger.warning(f"Error during client.close(): {close_err}")
            raise

    async def execute_in_container(
        self,
        project_id: str,
        command: str,
        cwd: str = "/workdir",
        *,
        timeout_seconds: Optional[float] = None,
    ) -> Dict[str, Any]:
        blocked = self._session_unavailable_tool_result(project_id)
        if blocked:
            return blocked

        container = await self.get_or_create_container(project_id)
        # get_or_create sets the lockout itself when replacing the environment on
        # a preserved workspace fails.
        blocked = self._session_unavailable_tool_result(project_id, container)
        if blocked:
            return blocked

        client = container.get("client")
        
        if not client:
            return {
                "stdout": "",
                "stderr": "Container not available",
                "exit_code": 1,
                "error_type": "unavailable",
            }
        
        # Redacted even at debug level: raising the log level to chase a sandbox
        # problem should not be the thing that copies a signed URL into the logs.
        logger.debug(f"Executing in {project_id}: {scrub_signed_urls(command)}")
        result = await client.execute_command(
            command, cwd=cwd, timeout_seconds=timeout_seconds
        )
        
        logger.debug(f"Exit code: {result.get('exit_code', -1)}")
        return result
    
    async def write_file_in_container(
        self,
        project_id: str,
        path: str,
        content: str,
        sync_to_db: bool = True,
    ) -> Dict[str, Any]:
        blocked = self._session_unavailable_tool_result(project_id)
        if blocked:
            return blocked
        container = await self.get_or_create_container(project_id)
        blocked = self._session_unavailable_tool_result(project_id, container)
        if blocked:
            return blocked
        client = container.get("client")
        
        if not client:
            return {
                "stdout": "",
                "stderr": "Container not available",
                "exit_code": 1,
                "error_type": "unavailable",
            }
        
        logger.info(f"[ARTIFACT] Writing file in {project_id}: {path}")
        result = await client.write_file(path, content)
        
        # Sync to DB for persistence (DB-first principle)
        # Note: write_file returns {"status": "success"} not exit_code
        write_success = result.get("status") == "success" or result.get("exit_code", 1) == 0
        logger.info(f"[ARTIFACT] Write result: {result}, success={write_success}, has_store={self.artifact_store is not None}, sync_to_db={sync_to_db}")
        if sync_to_db and self.artifact_store and write_success:
            try:
                # save_file can refuse a write (e.g. an oversized file whose spill
                # failed) by return value rather than by raising, so a non-success
                # status is the same container/DB divergence the except guards.
                sync = await self.artifact_store.save_file(project_id, path, content)
                if sync.get("status") in {"created", "updated"}:
                    logger.debug(f"Artifact synced to DB: {project_id}/{path}")
                else:
                    logger.warning(
                        "[ARTIFACT_SYNC_FAIL] save_file did not persist %s/%s (%s) — container has the file but the DB does not; revert/restore will diverge from the container state",
                        project_id, path, sync.get("reason") or sync.get("status"),
                    )
            except Exception:
                logger.exception(
                    "[ARTIFACT_SYNC_FAIL] save_file failed for %s/%s — container has the file but DB sync did not complete; revert/restore will diverge from the container state",
                    project_id, path,
                )

        return result

    async def read_file_from_container(
        self,
        project_id: str,
        path: str,
        storage=None
    ) -> str:
        from .command_lifecycle import SessionUnavailableError

        blocked = self._session_unavailable_tool_result(project_id)
        if blocked:
            raise SessionUnavailableError(
                self.session_unavailable_reason(project_id) or "unavailable"
            )
        container = await self.get_or_create_container(project_id)
        blocked = self._session_unavailable_tool_result(project_id, container)
        if blocked:
            raise SessionUnavailableError(
                self.session_unavailable_reason(project_id) or "unavailable"
            )
        client = container.get("client")
        
        if not client:
            # Same as list/write/bash: no live sandbox is infra, not a DB hit.
            raise RuntimeError("Container not available for read_file")
        
        logger.debug(f"Reading file from {project_id}: {path}")
        try:
            content = await client.read_file(path)
            if content is None:
                content = ""

            # If container returns empty, try context artifacts fallback
            if content == "" and storage:
                fallback = await self._read_from_context_artifacts(project_id, path, storage)
                if fallback:
                    logger.info(f"[RECOVERY] Read {path} from context (container returned empty)")
                    return fallback

            return content
        except TimeoutError:
            # Timed-out call must not masquerade as artifact content for the gate.
            raise
        except ConnectionError:
            raise
        except Exception:
            # Session refuse / Not connected / other IO: same as timeout — do not
            # clear infra pending with a DB artifact success.
            raise
    
    async def _read_from_context_artifacts(self, project_id: str, path: str, storage) -> Optional[str]:
        """Read file from projects.context.artifacts (single source of truth)."""
        norm_path = str(path).strip().lstrip("./").rstrip("/")
        try:
            project = await storage.load_project(project_id)
            if project:
                artifacts = project.get("context", {}).get("artifacts", [])
                for art in artifacts:
                    art_path = str(art.get("path", "")).strip().lstrip("./").rstrip("/")
                    if art_path == norm_path and art.get("content"):
                        return art["content"]
        except Exception as e:
            logger.debug(f"[RECOVERY] context artifacts lookup failed: {e}")
        return None
    
    async def _get_all_context_artifacts(self, project_id: str, storage) -> list:
        """Get all artifacts from projects.context.artifacts."""
        try:
            project = await storage.load_project(project_id)
            if project:
                return project.get("context", {}).get("artifacts", [])
        except Exception as e:
            logger.debug(f"[RECOVERY] failed to get context artifacts: {e}")
        return []
    
    async def hydrate_spilled_file(
        self,
        project_id: str,
        path: str,
        archive_ref_id: str,
        storage,
        *,
        budget_seconds: int = 3600,
    ) -> bool:
        """Download a spilled file artifact from object storage straight into the
        container at ``path`` via curl inside the sandbox. The bytes never enter
        this process or the LLM context, so restoring a many-hundred-MB file costs
        no backend memory.

        A file whose content was spilled to S3 comes back from the artifact store
        with content=None; without this it would silently vanish on recovery.
        Returns True only on a byte-count-verified download.
        """
        from storage.archive_store import ArchiveStore

        archive_store = self.archive_store or ArchiveStore.from_env(storage)
        if not archive_store.is_configured():
            logger.warning("[RECOVERY] cannot hydrate %s: object storage not configured", path)
            return False
        record = await archive_store.get_ref(archive_ref_id)
        object_key = (record or {}).get("object_key")
        if not object_key:
            logger.warning("[RECOVERY] cannot hydrate %s: ref %s has no stored object", path, archive_ref_id)
            return False
        # Existence before minting: presigning a vanished object hands curl a 404.
        try:
            head = await archive_store.head_blob(object_key)
        except Exception as exc:
            logger.error("[RECOVERY] head failed hydrating %s (ref=%s): %s", path, archive_ref_id, exc)
            return False
        if head is None:
            logger.warning("[RECOVERY] object gone for %s (ref=%s) — skipping", path, archive_ref_id)
            return False
        # TTL outlives the whole retry window, not just one attempt — a signature
        # dying mid-transfer would fail exactly the slow, retried downloads this
        # budget exists to allow (same shape as archive_fetch).
        ttl = int(budget_seconds) * 2 + 300
        try:
            url = await archive_store.presign_get(object_key, ttl)
        except Exception as exc:
            logger.error("[RECOVERY] presign failed hydrating %s: %s", path, exc)
            return False

        import shlex
        import posixpath

        q = shlex.quote
        parent = posixpath.dirname(path)
        mkdir = f"mkdir -p {q(parent)} && " if parent else ""
        # Both curl limits are required: --max-time bounds one attempt, --retry
        # restarts that clock, and --retry-max-time bounds the retry window. The
        # byte count runs even when curl fails (`;` not `&&`) — a missing file
        # printing 0 IS the failure signal.
        command = (
            f"{mkdir}rm -f {q(path)} && "
            f"curl -fsSL --retry 3 --retry-max-time {int(budget_seconds)} "
            f"--max-time {int(budget_seconds)} -o {q(path)} {q(url)}; "
            f"wc -c < {q(path)} 2>/dev/null || echo 0"
        )
        exec_res = await self.execute_in_container(project_id, command)
        downloaded = _trailing_byte_count(str((exec_res or {}).get("stdout") or ""))
        if not downloaded:
            stderr = str((exec_res or {}).get("stderr") or "").replace(url, "<presigned-url>")[:500]
            logger.error("[RECOVERY] hydrate wrote no bytes for %s (ref=%s): %s", path, archive_ref_id, stderr)
            return False
        # Compare against the object's OWN length; the stored size_bytes is an
        # advisory index and can disagree with the bytes in the bucket.
        expected = head.get("ContentLength") if isinstance(head, dict) else None
        if not isinstance(expected, int) or isinstance(expected, bool) or expected <= 0:
            expected = record.get("size_bytes")
        if isinstance(expected, int) and expected > 0 and downloaded != expected:
            logger.error("[RECOVERY] hydrate short read for %s: got %d expected %d", path, downloaded, expected)
            return False
        logger.info("[RECOVERY] hydrated %s (%d B) from ref %s", path, downloaded, archive_ref_id)
        return True

    async def restore_container_from_context(self, project_id: str, storage) -> int:
        """
        Restore container by creating new one and writing all files from context.
        Returns number of files restored.
        """
        # Get artifacts from the single source of truth
        artifacts = await self._get_all_context_artifacts(project_id, storage)
        if not artifacts:
            logger.info(f"[RECOVERY] No artifacts to restore for {project_id}")
            return 0

        logger.info(f"[RECOVERY] Restoring {len(artifacts)} files to container for {project_id}")

        restored = 0
        # This legacy path restores only context.artifacts (inline {path, content}).
        # Spilled file_artifacts live in the ArtifactStore and are hydrated on the
        # recover_container path — they never reach this source, none to hydrate here.
        for art in artifacts:
            path = art.get("path")
            if not path:
                continue
            content = art.get("content")
            if content is not None:
                try:
                    await self.write_file_in_container(project_id, path, content, sync_to_db=False)
                    restored += 1
                except Exception as e:
                    logger.warning(f"[RECOVERY] Failed to restore {path}: {e}")

        logger.info(f"[RECOVERY] Restored {restored}/{len(artifacts)} files for {project_id}")
        return restored
    
    async def list_files_in_container(
        self,
        project_id: str,
        path: str = ".",
        storage=None
    ) -> list:
        from .command_lifecycle import SessionUnavailableError

        blocked = self._session_unavailable_tool_result(project_id)
        if blocked:
            raise SessionUnavailableError(
                self.session_unavailable_reason(project_id) or "unavailable"
            )
        container = await self.get_or_create_container(project_id)
        blocked = self._session_unavailable_tool_result(project_id, container)
        if blocked:
            raise SessionUnavailableError(
                self.session_unavailable_reason(project_id) or "unavailable"
            )
        client = container.get("client")
        
        if not client:
            # Same as write/bash: no live sandbox is infra, not a DB listing success.
            raise RuntimeError("Container not available for list_files")
        
        files = await client.list_files(path)
        return files

    async def delete_file_in_container(
        self,
        project_id: str,
        path: str,
        sync_to_db: bool = True,
    ) -> Dict[str, Any]:
        blocked = self._session_unavailable_tool_result(project_id)
        if blocked:
            return blocked
        container = await self.get_or_create_container(project_id)
        blocked = self._session_unavailable_tool_result(project_id, container)
        if blocked:
            return blocked
        client = container.get("client")
        if not client:
            return {
                "stdout": "",
                "stderr": "Container not available",
                "exit_code": 1,
                "error_type": "unavailable",
            }
        
        result = await client.delete_file(path)
        
        # Sync deletion to DB
        delete_success = result.get("status") == "success" or result.get("exit_code", 1) == 0
        if sync_to_db and self.artifact_store and delete_success:
            try:
                await self.artifact_store.delete_file(project_id, path)
                logger.debug(f"Artifact deleted from DB: {project_id}/{path}")
            except Exception as e:
                logger.warning(f"Failed to delete artifact from DB: {e}")
        
        return result

    async def find_replace_in_file(
        self,
        project_id: str,
        path: str,
        search_text: str,
        replace_text: str,
        which_match: Optional[str] = None,
        sync_to_db: bool = True,
    ) -> Dict[str, Any]:
        blocked = self._session_unavailable_tool_result(project_id)
        if blocked:
            return blocked
        container = await self.get_or_create_container(project_id)
        blocked = self._session_unavailable_tool_result(project_id, container)
        if blocked:
            return blocked
        client = container.get("client")
        if not client:
            return {
                "stdout": "",
                "stderr": "Container not available",
                "exit_code": 1,
                "error_type": "unavailable",
            }
        
        result = await client.find_replace_file(path, search_text, replace_text, which_match)
        
        # Sync updated file to DB after successful replace
        replace_success = result.get("status") == "success" or result.get("exit_code", 1) == 0
        if sync_to_db and self.artifact_store and replace_success:
            try:
                # Read the updated content and sync to DB
                updated_content = await client.read_file(path)
                if updated_content:
                    sync = await self.artifact_store.save_file(project_id, path, updated_content)
                    if sync.get("status") in {"created", "updated"}:
                        logger.debug(f"Updated artifact synced to DB: {project_id}/{path}")
                    else:
                        logger.warning(
                            "[ARTIFACT_SYNC_FAIL] save_file did not persist %s/%s after find-replace (%s) — container has the file but the DB does not; revert/restore will diverge from the container state",
                            project_id, path, sync.get("reason") or sync.get("status"),
                        )
            except Exception:
                logger.exception(
                    "[ARTIFACT_SYNC_FAIL] save_file failed for %s/%s after find-replace — container has the file but DB sync did not complete; revert/restore will diverge from the container state",
                    project_id, path,
                )
        
        return result
    
    async def get_container_status(self, project_id: str) -> Dict[str, Any]:
        info = self.containers.get(project_id) or {}
        # Presence in ``containers`` alone is not enough: a simulated stub
        # (client=None) must not skip hydrate/recovery and then report file
        # writes as success without a live MCP session.
        is_active = bool(info.get("client")) and info.get("status") not in (
            "simulated",
            "unavailable",
        )
        branch_name = f"AppFactory-{project_id}"
        env_id = info.get("environment_id") if info else None
        repo_path = info.get("repo_path") if info else None

        if not repo_path:
            preserved = self.preserved_workspace(project_id)
            if preserved:
                repo_path = preserved.get("repo_path")
                env_id = env_id or preserved.get("environment_id")
        
        # If repo_path not available from active container, construct it
        # This allows reading artifacts even after container cleanup
        if not repo_path and self.repositories_root:
            constructed_path = Path(self.repositories_root) / f"AppFactory-{project_id}"
            if constructed_path.exists():
                repo_path = str(constructed_path)
        
        return {
            "project_id": project_id,
            "active": is_active,
            "branch": branch_name,
            "enabled": self.enabled,
            "environment_id": env_id,
            "repo_path": repo_path,
        }

    async def checkout_environment(self, project_id: str) -> Dict[str, Any]:
        # Run `container-use checkout <env-id>` from repositories_root
        info = self.containers.get(project_id, {})
        env_id = info.get("environment_id")
        repo_path = info.get("repo_path")
        if not env_id or not repo_path:
            return {"stdout": "", "stderr": "Missing environment or repo_path", "exit_code": 1}
        logger.info(f"Running checkout for env {env_id} in root {self.repositories_root}")
        return await run_host_cli([self.cli_path, "checkout", env_id], cwd=self.repositories_root)

    async def apply_environment(self, project_id: str) -> Dict[str, Any]:
        # Run `container-use apply <env-id>` in the repo folder on the host
        info = self.containers.get(project_id, {})
        env_id = info.get("environment_id")
        repo_path = info.get("repo_path")
        if not env_id or not repo_path:
            return {"stdout": "", "stderr": "Missing environment or repo_path", "exit_code": 1}
        return await run_host_cli([self.cli_path, "apply", env_id], cwd=repo_path)

    async def get_logs(self, project_id: str = None, environment_id: str = None) -> Dict[str, Any]:
        """
        Get container logs using `cu log <env-id>`.
        
        Args:
            project_id: Project identifier (optional if environment_id provided)
            environment_id: Environment ID (optional if project_id provided)
            
        Returns:
            Dict with stdout (logs), stderr, and exit_code
        """
        logger.info(f"🔧 get_logs called with project_id={project_id}, environment_id={environment_id}")
        
        # Try to get env_id and repo_path from project_id first
        if project_id:
            info = self.containers.get(project_id, {})
            env_id = info.get("environment_id")
            repo_path = info.get("repo_path")
            logger.info(f"📦 From containers dict: env_id={env_id}, repo_path={repo_path}")
        else:
            env_id = None
            repo_path = None
            logger.info("⚠️  No project_id provided")
        
        # Fall back to provided environment_id
        if not env_id and environment_id:
            env_id = environment_id
            # Try to find repo path by environment_id
            from pathlib import Path
            repo_path = str(Path(self.repositories_root) / env_id) if self.repositories_root else None
            logger.info(f"✅ Using provided environment_id: {env_id}, computed repo_path: {repo_path}")
        
        if not env_id:
            logger.error("❌ No environment ID available!")
            return {"stdout": "", "stderr": "No environment ID available", "exit_code": 1}
        
        # Run from repo directory if available, otherwise from repositories_root
        cwd = repo_path if repo_path else self.repositories_root
        
        logger.info(f"🚀 Running: {self.cli_path} log {env_id} from cwd={cwd}")
        result = await run_host_cli([self.cli_path, "log", env_id], cwd=cwd)
        logger.info(f"📊 Command result: exit_code={result.get('exit_code')}, stdout_len={len(result.get('stdout', ''))}, stderr={result.get('stderr', '')[:100]}")

        # Every caller of this publishes what it returns — the logs route sends
        # it to a browser AND snapshots it to Mongo, and the run-end hook
        # snapshots it with no one asking. Redact before any of them see it.
        for stream in ("stdout", "stderr"):
            if isinstance(result.get(stream), str):
                result[stream] = scrub_signed_urls(result[stream])

        return result
    
    async def cleanup_container(self, project_id: str, keep_for_review: bool = True):
        if project_id in self.containers:
            container = self.containers.pop(project_id)
            client = container.get("client")
            
            if client:
                await client.close()
            
            if keep_for_review:
                logger.info(
                    f"Container connection closed. "
                    f"Review: git checkout AppFactory-{project_id}"
                )
            else:
                logger.info(f"Container {project_id} cleaned up")
        else:
            logger.debug(f"No active container for {project_id}")
    
    async def close_all(self, keep_for_review: bool = True):
        logger.info(f"Closing {len(self.containers)} containers")
        
        for project_id in list(self.containers.keys()):
            await self.cleanup_container(project_id, keep_for_review)

    async def reset_pre_execution(self, project_id: str) -> Dict[str, Any]:
        """Reset the container workspace to a clean state before execution.

        Uses git to reset the working tree and remove untracked files so that
        subsequent executions start from a consistent baseline.
        """
        try:
            result = await self.execute_in_container(
                project_id,
                "git reset --hard && git clean -fdx",
                cwd="/workdir",
            )
            try:
                logger.info(
                    "reset_pre_execution project_id=%s exit_code=%s",
                    project_id,
                    result.get("exit_code"),
                )
            except Exception:
                pass
            return result
        except Exception as e:
            logger.warning("reset_pre_execution failed for %s: %s", project_id, e)
            return {"stdout": "", "stderr": str(e), "exit_code": 1}

    # Revert helpers
    async def reset_environment(self, project_id: str) -> None:
        """Tear down the current container-use environment for project (best-effort)."""
        try:
            await self.cleanup_container(project_id, keep_for_review=True)
        except Exception:
            pass

    async def recreate_environment_from_repo(self, project_id: str) -> Dict[str, Any]:
        """Recreate environment by (re)connecting container-use to the project's repo."""
        # get_or_create_container will create fresh entry if none exists
        return await self.get_or_create_container(project_id)
    
