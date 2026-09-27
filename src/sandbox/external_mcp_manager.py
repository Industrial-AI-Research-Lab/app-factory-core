"""External MCP container orchestration and lifecycle manager."""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx

from sandbox.host_cli import run_host_cli
from tools.mcp_zip_build_policy import is_zip_smoke_ephemeral_server_id
from tools.external_mcp import ExternalMCPClient, ExternalMCPConfig

logger = logging.getLogger(__name__)


@dataclass
class ExternalMCPServerRuntime:
    """Runtime state for one external MCP server instance."""

    project_id: str
    server_id: str
    tenant_id: str
    image: str
    container_name: str
    host_port: int
    container_port: int
    endpoint: str
    mode: str
    client: ExternalMCPClient
    last_used_ts: float
    # None: use manager default; -1.0: never evict from idle sweeper; >=0: seconds
    idle_timeout_seconds: Optional[float] = None
    runtime_scope: str = "project"
    on_project_complete: str = "remove"  # remove | stop_only
    # True: omit docker --rm so the container can be stopped and later docker start
    durable: bool = False
    # One-shot: tools from ensure_server handshake; discover_tools consumes then clears.
    handshake_tools: Optional[List[Any]] = None


class ExternalMCPServerManager:
    """Manage external MCP server containers with idle-timeout teardown."""

    def __init__(
        self,
        *,
        docker_binary: str = "docker",
        mcp_path: str = "/mcp",
        idle_timeout_seconds: int = 600,
        sweep_interval_seconds: int = 30,
        host_port_start: int = 39000,
        storage: Optional[Any] = None,
    ):
        self.docker_binary = docker_binary
        self.mcp_path = mcp_path
        self.idle_timeout_seconds = idle_timeout_seconds
        self.sweep_interval_seconds = sweep_interval_seconds
        self.host_port_start = host_port_start
        self.storage = storage
        self._servers: Dict[str, ExternalMCPServerRuntime] = {}
        self._key_locks_meta = asyncio.Lock()
        self._key_locks: Dict[str, asyncio.Lock] = {}
        self._sweeper_task: Optional[asyncio.Task] = None
        # Claimed host ports until docker binds / cleanup — avoids TOCTOU with concurrent ensure.
        self._port_alloc_lock = asyncio.Lock()
        self._reserved_ports: set[int] = set()
        self._port_by_container: Dict[str, int] = {}

    async def _server_key_lock(self, key: str) -> asyncio.Lock:
        """Serialize ensure/stop for one (project, tenant, server) without blocking other keys."""
        async with self._key_locks_meta:
            lk = self._key_locks.get(key)
            if lk is None:
                lk = asyncio.Lock()
                self._key_locks[key] = lk
            return lk

    @staticmethod
    def _docker_run_recoverable_stderr(stderr: str) -> bool:
        s = (stderr or "").lower()
        return any(
            needle in s
            for needle in (
                "port is already allocated",
                "address already in use",
                "bind for 0.0.0.0",
                "name is already in use",
                "is already in use by container",
            )
        )

    @staticmethod
    def _skip_http_warmup() -> bool:
        """Skip polling in pytest (fake docker run, no real HTTP listener)."""
        return os.environ.get("AppFactory_MCP_SKIP_HTTP_WARMUP", "").strip().lower() in (
            "1",
            "true",
            "yes",
        )

    async def _wait_for_http_endpoint_ready(
        self,
        endpoint: str,
        *,
        timeout_seconds: float,
    ) -> None:
        """Poll until the container serves HTTP (e.g. Context7 GET /ping)."""
        if self._skip_http_warmup():
            return
        ep = (endpoint or "").rstrip("/")
        origin = ep.rsplit("/mcp", 1)[0] if ep.endswith("/mcp") else ep
        urls = [f"{origin}/ping", ep]
        timeout_s = max(0.1, float(timeout_seconds))
        deadline = time.time() + timeout_s
        last_err = ""
        http_timeout = min(4.0, max(0.25, timeout_s))
        async with httpx.AsyncClient(timeout=httpx.Timeout(http_timeout)) as http:
            while time.time() < deadline:
                for url in urls:
                    try:
                        resp = await http.get(url)
                        if resp.status_code < 500:
                            logger.info(
                                "[EXTERNAL_MCP] [WARMUP] ready url=%s status=%s",
                                url,
                                resp.status_code,
                            )
                            return
                    except Exception as exc:
                        last_err = str(exc)[:240]
                remaining = deadline - time.time()
                if remaining <= 0:
                    break
                await asyncio.sleep(min(2.0, remaining))
        raise RuntimeError(
            f"MCP HTTP endpoint not ready within {int(timeout_seconds)}s ({last_err})"
        )

    async def ensure_server(
        self,
        *,
        project_id: str,
        server_id: str,
        tenant_id: str,
        image: str,
        container_port: int = 8080,
        endpoint_path: Optional[str] = None,
        startup_timeout_seconds: int = 30,
        mode: str = "http",
        docker_env_vars: Optional[Dict[str, str]] = None,
        docker_cmd_args: Optional[list[str]] = None,
        idle_timeout_seconds: Optional[float] = None,
        runtime_scope: str = "project",
        on_project_complete: str = "remove",
    ) -> ExternalMCPClient:
        """Ensure external MCP container exists and return connected client."""
        key = self._key(project_id, tenant_id, server_id)
        idle_f: Optional[float] = None
        if idle_timeout_seconds is not None:
            try:
                idle_f = float(idle_timeout_seconds)
            except (TypeError, ValueError):
                idle_f = None
        opc = (on_project_complete or "remove").strip().lower()
        on_pc = "stop_only" if opc in ("stop_only", "stop", "soft") else "remove"
        durable = (idle_f is not None and idle_f < 0) or (on_pc == "stop_only")

        logger.info(
            "[EXTERNAL_MCP] [ENSURE] key=%s image=%s scope=%s durable=%s idle_s=%s on_project_complete=%s",
            key,
            image,
            runtime_scope,
            durable,
            idle_f if idle_f is not None else idle_timeout_seconds,
            on_pc,
        )
        _lk = await self._server_key_lock(key)
        async with _lk:
            existing = self._servers.get(key)
            if existing:
                existing.last_used_ts = time.time()
                logger.info(
                    "[EXTERNAL_MCP] [ENSURE] key=%s — reuse container=%s endpoint=%s",
                    key,
                    existing.container_name,
                    existing.endpoint,
                )
                return existing.client

            container_name = self._container_name(project_id, tenant_id, server_id)
            client: Optional[ExternalMCPClient] = None
            try:
                host_port = await self._resolve_host_port(
                    tenant_id=tenant_id,
                    server_id=server_id,
                    container_name=container_name,
                    project_id=project_id,
                )
                if durable:
                    # docker start reuses the container's existing -p publish. A concurrent
                    # discovery health probe may briefly hold that host port after
                    # background_cleanup — retry start; never rm an existing durable.
                    start_attempts = 4
                    start_res: Dict[str, Any] = {"exit_code": 1, "stderr": ""}
                    for start_attempt in range(1, start_attempts + 1):
                        start_res = await run_host_cli(
                            [self.docker_binary, "start", container_name], timeout=30
                        )
                        if int(start_res.get("exit_code", 1)) == 0:
                            mapped = await self._get_host_port_for_container(
                                container_name, container_port
                            )
                            if isinstance(mapped, int) and mapped > 0:
                                host_port = mapped
                                self._remember_container_port(container_name, host_port)
                            endpoint = f"http://127.0.0.1:{host_port}{endpoint_path or self.mcp_path}"
                            client = ExternalMCPClient(
                                ExternalMCPConfig(
                                    server_id=server_id,
                                    endpoint=endpoint,
                                    tenant_id=tenant_id,
                                    mode=mode,
                                    timeout_seconds=float(startup_timeout_seconds),
                                )
                            )
                            try:
                                tools = await client.discover_tools()
                                runtime = ExternalMCPServerRuntime(
                                    project_id=project_id,
                                    server_id=server_id,
                                    tenant_id=tenant_id,
                                    image=image,
                                    container_name=container_name,
                                    host_port=host_port,
                                    container_port=container_port,
                                    endpoint=endpoint,
                                    mode=mode,
                                    client=client,
                                    last_used_ts=time.time(),
                                    idle_timeout_seconds=idle_timeout_seconds,
                                    runtime_scope=runtime_scope,
                                    on_project_complete=on_pc,
                                    durable=durable,
                                    handshake_tools=tools if isinstance(tools, list) else [],
                                )
                                self._servers[key] = runtime
                                await self._save_server_port(
                                    tenant_id=tenant_id,
                                    server_id=server_id,
                                    port=host_port,
                                    project_id=project_id,
                                )
                                logger.info(
                                    "[EXTERNAL_MCP] project=%s tenant=%s server=%s container=%s"
                                    " mode=%s endpoint=%s resumed (docker start)",
                                    project_id,
                                    tenant_id,
                                    server_id,
                                    container_name,
                                    mode,
                                    endpoint,
                                )
                                return client
                            except Exception:
                                await self._cleanup_untracked_runtime(container_name, client)
                                raise
                        exists = await self._docker_container_exists(container_name)
                        stderr = (start_res.get("stderr") or "").strip()
                        if not exists:
                            logger.info(
                                "[EXTERNAL_MCP] [ENSURE] key=%s container=%s — docker start "
                                "exit=%s stderr=%s, container missing; running new container",
                                key,
                                container_name,
                                int(start_res.get("exit_code", 1)),
                                stderr[:500],
                            )
                            break
                        if start_attempt >= start_attempts:
                            logger.error(
                                "[EXTERNAL_MCP] [ENSURE] key=%s container=%s — docker start "
                                "failed after %s attempts (exit=%s stderr=%s); refusing "
                                "recreate of durable container",
                                key,
                                container_name,
                                start_attempts,
                                int(start_res.get("exit_code", 1)),
                                stderr[:500],
                            )
                            raise RuntimeError(
                                f"Durable external MCP container {container_name} failed to "
                                "start; refusing recreate to avoid data loss"
                            )
                        logger.warning(
                            "[EXTERNAL_MCP] [ENSURE] key=%s container=%s — docker start "
                            "attempt=%s/%s exit=%s stderr=%s; retry (port may be held by "
                            "health discovery)",
                            key,
                            container_name,
                            start_attempt,
                            start_attempts,
                            int(start_res.get("exit_code", 1)),
                            stderr[:300],
                        )
                        await asyncio.sleep(0.5 * start_attempt)

                max_start_attempts = 5
                last_stderr = ""
                for attempt in range(1, max_start_attempts + 1):
                    rm_res = await run_host_cli(
                        [self.docker_binary, "rm", "-f", container_name],
                        timeout=20,
                    )
                    if attempt > 1:
                        logger.warning(
                            "[EXTERNAL_MCP] [ENSURE] key=%s container=%s retry=%s/%s host_port=%s rm_exit=%s",
                            key,
                            container_name,
                            attempt,
                            max_start_attempts,
                            host_port,
                            int(rm_res.get("exit_code", 1)),
                        )
                    else:
                        logger.debug(
                            "[EXTERNAL_MCP] [ENSURE] key=%s preflight docker rm -f %s exit=%s",
                            key,
                            container_name,
                            int(rm_res.get("exit_code", 1)),
                        )
                    endpoint = f"http://127.0.0.1:{host_port}{endpoint_path or self.mcp_path}"
                    run_cmd = [
                        self.docker_binary,
                        "run",
                        "-d",
                        "--name",
                        container_name,
                        "--label",
                        "AppFactory.external_mcp=true",
                        "--label",
                        f"project_id={project_id}",
                        "--label",
                        f"tenant_id={tenant_id}",
                        "--label",
                        f"server_id={server_id}",
                        "-p",
                        f"{host_port}:{container_port}",
                    ]
                    if not durable:
                        run_cmd.append("--rm")
                    for ek, ev in (docker_env_vars or {}).items():
                        run_cmd += ["-e", f"{ek}={ev}"]
                    run_cmd.append(image)
                    if docker_cmd_args:
                        run_cmd.extend([str(v) for v in docker_cmd_args])

                    logger.info(
                        "[EXTERNAL_MCP] [ENSURE] key=%s container=%s host_port=%s durable=%s docker_run attempt=%s/%s",
                        key,
                        container_name,
                        host_port,
                        durable,
                        attempt,
                        max_start_attempts,
                    )
                    # Task.cancel does not kill run_in_executor(subprocess). shield alone
                    # still raises CancelledError immediately — await the Task so finally/rm
                    # runs only after docker run has finished (container exists).
                    run_task = asyncio.create_task(
                        run_host_cli(run_cmd, timeout=startup_timeout_seconds)
                    )
                    try:
                        run_res = await asyncio.shield(run_task)
                    except asyncio.CancelledError:
                        try:
                            await run_task
                        except Exception:
                            pass
                        raise
                    exit_code = int(run_res.get("exit_code", 1))
                    if exit_code == 0:
                        break
                    last_stderr = (run_res.get("stderr") or "").strip()
                    recoverable = self._docker_run_recoverable_stderr(last_stderr)
                    if recoverable and attempt < max_start_attempts:
                        logger.warning(
                            "[EXTERNAL_MCP] [ENSURE] key=%s recoverable docker_run failure attempt=%s stderr=%s",
                            key,
                            attempt,
                            last_stderr[:800],
                        )
                        host_port = await self._claim_free_port(
                            host_port + 1, container_name=container_name
                        )
                        continue
                    raise RuntimeError(
                        f"Failed to start external MCP container '{container_name}': {last_stderr}"
                    )

                run_mode = (mode or "http").strip().lower()
                if run_mode in ("http", "streamable-http"):
                    warmup_s = min(90.0, float(startup_timeout_seconds))
                    try:
                        await self._wait_for_http_endpoint_ready(
                            endpoint, timeout_seconds=warmup_s
                        )
                    except Exception as warmup_exc:
                        logger.warning(
                            "[EXTERNAL_MCP] [WARMUP] endpoint=%s mode=%s not ready before handshake: %s",
                            endpoint,
                            run_mode,
                            warmup_exc,
                        )

                client = ExternalMCPClient(
                    ExternalMCPConfig(
                        server_id=server_id,
                        endpoint=endpoint,
                        tenant_id=tenant_id,
                        mode=mode,
                        timeout_seconds=float(startup_timeout_seconds),
                    )
                )
                try:
                    tools = await client.discover_tools()

                    runtime = ExternalMCPServerRuntime(
                        project_id=project_id,
                        server_id=server_id,
                        tenant_id=tenant_id,
                        image=image,
                        container_name=container_name,
                        host_port=host_port,
                        container_port=container_port,
                        endpoint=endpoint,
                        mode=mode,
                        client=client,
                        last_used_ts=time.time(),
                        idle_timeout_seconds=idle_timeout_seconds,
                        runtime_scope=runtime_scope,
                        on_project_complete=on_pc,
                        durable=durable,
                        handshake_tools=tools if isinstance(tools, list) else [],
                    )
                    self._servers[key] = runtime
                    await self._save_server_port(
                        tenant_id=tenant_id,
                        server_id=server_id,
                        port=host_port,
                        project_id=project_id,
                    )
                    logger.info(
                        "[EXTERNAL_MCP] project=%s tenant=%s server=%s container=%s mode=%s endpoint=%s started",
                        project_id,
                        tenant_id,
                        server_id,
                        container_name,
                        mode,
                        endpoint,
                    )
                    return client
                except Exception:
                    await self._cleanup_untracked_runtime(container_name, client)
                    raise
            finally:
                # CancelledError is BaseException: except Exception misses it.
                # Ephemeral: best-effort rm (may run after shielded docker run completes).
                # Durable stop_only: never rm on cancel — container must survive for docker start.
                if key not in self._servers:
                    if durable:
                        if client is not None:
                            try:
                                await client.disconnect()
                            except Exception:
                                pass
                        self._release_container_port(container_name)
                    else:
                        await self._cleanup_untracked_runtime(container_name, client)

    async def discover_tools(self, *, project_id: str, tenant_id: str, server_id: str):
        """Proxy discovery call and refresh idle timestamp.

        After ensure_server handshake, returns cached tools once (no second list_tools).
        """
        k = self._key(project_id, tenant_id, server_id)
        runtime = self._servers.get(k)
        if not runtime:
            logger.warning(
                "[EXTERNAL_MCP] [DISCOVER] key=%s — not running in manager cache", k
            )
            raise RuntimeError("External MCP server is not running")
        logger.debug(
            "[EXTERNAL_MCP] [DISCOVER] key=%s container=%s", k, runtime.container_name
        )
        runtime.last_used_ts = time.time()
        if runtime.handshake_tools is not None:
            tools = runtime.handshake_tools
            runtime.handshake_tools = None
            return tools
        return await runtime.client.discover_tools()

    async def call_tool(
        self,
        *,
        project_id: str,
        tenant_id: str,
        server_id: str,
        tool_name: str,
        arguments: Optional[dict] = None,
    ):
        """Proxy tool call and refresh idle timestamp."""
        k = self._key(project_id, tenant_id, server_id)
        runtime = self._servers.get(k)
        if not runtime:
            logger.warning(
                "[EXTERNAL_MCP] [CALL_TOOL] key=%s tool=%s — not running in manager cache",
                k,
                tool_name,
            )
            raise RuntimeError("External MCP server is not running")
        logger.debug(
            "[EXTERNAL_MCP] [CALL_TOOL] key=%s tool=%s container=%s",
            k,
            tool_name,
            runtime.container_name,
        )
        runtime.last_used_ts = time.time()
        return await runtime.client.call_tool(tool_name, arguments or {})

    async def stop_all_for_tenant_mcp_server(
        self, *, tenant_id: str, mcp_server_id: str
    ) -> None:
        """Remove every cached runtime for this (tenant, server) — e.g. config deleted from DB."""
        keys: List[str] = [
            k
            for k, r in list(self._servers.items())
            if r.tenant_id == tenant_id and r.server_id == mcp_server_id
        ]
        logger.info(
            "[EXTERNAL_MCP] [STOP_ALL_TENANT] tenant_id=%s mcp_server_id=%s matching_keys=%d — %s",
            tenant_id,
            mcp_server_id,
            len(keys),
            keys,
        )
        for k in keys:
            p, t, s = k.split(":", 2)
            await self.stop_server(
                project_id=p, tenant_id=t, server_id=s, stop_reason="config"
            )
        logger.info(
            "[EXTERNAL_MCP] [STOP_ALL_TENANT] done tenant_id=%s mcp_server_id=%s",
            tenant_id,
            mcp_server_id,
        )

    async def disconnect_cached_client(
        self,
        *,
        project_id: str,
        tenant_id: str,
        server_id: str,
    ) -> None:
        """Close cached MCP client on the caller's task; leave container for stop_server.

        Health background_cleanup schedules stop_server first (peek/name known), then
        calls this from the discovery task (anyio aclose must stay on that task).
        """
        key = self._key(project_id, tenant_id, server_id)
        runtime = self._servers.get(key)
        if not runtime or runtime.client is None:
            return
        try:
            await runtime.client.disconnect()
        except Exception as e:
            logger.warning(
                "[EXTERNAL_MCP] [DISCONNECT_CACHED] key=%s tenant=%s server=%s — %s",
                key,
                tenant_id,
                server_id,
                e,
            )

    async def stop_server(
        self,
        *,
        project_id: str,
        tenant_id: str,
        server_id: str,
        stop_reason: str = "project_finalize",
        disconnect_client: bool = True,
    ) -> None:
        """Stop one external MCP server and optionally disconnect its client.

        Docker stop/rm runs before client.disconnect so a hung aclose on another task
        (health same-task teardown holding ``_connect_lock``) cannot block killing the
        container. Pass ``disconnect_client=False`` when the caller already owns
        same-task aclose (health ``background_cleanup``).

        stop_reason:
        - config: tool config removed — always remove the container if it still exists
        - lifecycle: API process shutdown — preserve durable containers (stop only)
        - sweeper: idle eviction — stop only (no rm for durable; ephemeral --rm vanishes on stop)
        - project_finalize: project-scoped runtimes only (tenant shared keys are not finalized per project).
          idle_timeout < 0: stop only, never rm. Else if durable: rm when on_project_complete=remove.
        """
        key = self._key(project_id, tenant_id, server_id)
        _lk = await self._server_key_lock(key)
        async with _lk:
            runtime = self._servers.pop(key, None)
        if not runtime:
            container_name = self._container_name(project_id, tenant_id, server_id)
            # Cancel mid-ensure_server (health wall-clock / HTTP abort) can leave a docker
            # container without a cache entry — Task.cancel does not kill run_in_executor.
            if stop_reason == "config":
                logger.warning(
                    "[EXTERNAL_MCP] [STOP] key=%s reason=%s container=%s — no cached runtime "
                    "(best-effort docker rm)",
                    key,
                    stop_reason,
                    container_name,
                )
                await self._cleanup_untracked_runtime(container_name, None)
            else:
                logger.warning(
                    "[EXTERNAL_MCP] [STOP] key=%s reason=%s container=%s — no cached runtime (skip)",
                    key,
                    stop_reason,
                    container_name,
                )
            return

        remove_after = False
        if stop_reason == "config":
            remove_after = True
        elif stop_reason == "lifecycle":
            remove_after = not runtime.durable
        elif stop_reason == "sweeper":
            remove_after = False
        elif stop_reason == "project_finalize":
            # idle_timeout < 0: long-lived / non-evict; on project end only stop, never rm
            # (user can `docker start` or next ensure_server resumes).
            eff_idle: Optional[float] = None
            if runtime.idle_timeout_seconds is not None:
                try:
                    eff_idle = float(runtime.idle_timeout_seconds)
                except (TypeError, ValueError):
                    eff_idle = None
            if eff_idle is not None and eff_idle < 0:
                remove_after = False
            elif runtime.durable:
                remove_after = runtime.on_project_complete == "remove"
        else:
            if runtime.durable:
                remove_after = True

        # Stop-only durable keeps its publish port reserved so health discovery cannot
        # steal it and break the next `docker start`.
        if remove_after:
            self._release_container_port(runtime.container_name)
        else:
            try:
                self._remember_container_port(runtime.container_name, int(runtime.host_port))
            except (TypeError, ValueError):
                pass

        logger.info(
            "[EXTERNAL_MCP] [STOP] key=%s reason=%s container=%s durable=%s on_project_complete=%s "
            "remove_after=%s disconnect_client=%s",
            key,
            stop_reason,
            runtime.container_name,
            runtime.durable,
            runtime.on_project_complete,
            remove_after,
            disconnect_client,
        )

        # Kill container before client.disconnect — hung aclose must not block docker.
        stop_cmd = [self.docker_binary, "stop", runtime.container_name]
        stop_res = await run_host_cli(stop_cmd, timeout=20)
        if int(stop_res.get("exit_code", 1)) != 0:
            logger.warning(
                "[EXTERNAL_MCP] [STOP] key=%s container=%s docker_stop exit=%s stderr=%s",
                key,
                runtime.container_name,
                int(stop_res.get("exit_code", 1)),
                (stop_res.get("stderr") or "").strip()[:800],
            )
        else:
            logger.info(
                "[EXTERNAL_MCP] [STOP] key=%s container=%s — docker stop ok",
                key,
                runtime.container_name,
            )

        if remove_after:
            rm_res = await run_host_cli(
                [self.docker_binary, "rm", "-f", runtime.container_name], timeout=20
            )
            ex = int(rm_res.get("exit_code", 1))
            if ex != 0 and "No such container" not in (rm_res.get("stderr") or ""):
                logger.warning(
                    "[EXTERNAL_MCP] [RM] key=%s container=%s exit=%s stderr=%s",
                    key,
                    runtime.container_name,
                    ex,
                    (rm_res.get("stderr") or "").strip()[:800],
                )
            else:
                logger.info(
                    "[EXTERNAL_MCP] [RM] key=%s container=%s — removed (exit=%s)",
                    key,
                    runtime.container_name,
                    ex,
                )
        else:
            logger.info(
                "[EXTERNAL_MCP] [RM] key=%s container=%s — skipped (stop only, durable or sweeper policy)",
                key,
                runtime.container_name,
            )

        if disconnect_client:
            try:
                await runtime.client.disconnect()
            except Exception as e:
                logger.warning(
                    "[EXTERNAL_MCP] [STOP] key=%s tenant=%s server=%s — disconnect failed: %s",
                    key,
                    tenant_id,
                    server_id,
                    e,
                )

        # Discovery never owns shared (tenant, server) ports — clearing would wipe a
        # concurrent project runtime mapping (worse with background_cleanup).
        # Zip-smoke may leave legacy rows; clear only that ephemeral server_id.
        if is_zip_smoke_ephemeral_server_id(server_id):
            await self._clear_server_port(tenant_id=tenant_id, server_id=server_id)

    async def start_lifecycle(self) -> None:
        """Start background idle sweeper."""
        if self._sweeper_task and not self._sweeper_task.done():
            return
        self._sweeper_task = asyncio.create_task(self._idle_sweeper())
        logger.info(
            "[EXTERNAL_MCP] lifecycle started idle_timeout=%ss sweep_interval=%ss",
            self.idle_timeout_seconds,
            self.sweep_interval_seconds,
        )

    async def stop_lifecycle(self) -> None:
        """Stop background sweeper and teardown all servers."""
        if self._sweeper_task:
            self._sweeper_task.cancel()
            try:
                await self._sweeper_task
            except asyncio.CancelledError:
                pass
            self._sweeper_task = None

        keys = list(self._servers.keys())
        logger.info(
            "[EXTERNAL_MCP] [LIFECYCLE] stop_sweeper — draining servers count=%d keys=%s",
            len(keys),
            keys,
        )
        for key in keys:
            project_id, tenant_id, server_id = key.split(":", 2)
            await self.stop_server(
                project_id=project_id,
                tenant_id=tenant_id,
                server_id=server_id,
                stop_reason="lifecycle",
            )
        logger.info("[EXTERNAL_MCP] [LIFECYCLE] stopped — sweeper and all in-memory runtimes cleared")

    async def _cleanup_untracked_runtime(
        self,
        container_name: str,
        client: Optional[ExternalMCPClient],
    ) -> None:
        """Best-effort cleanup when runtime failed before entering self._servers."""
        if client is not None:
            try:
                await client.disconnect()
            except Exception:
                pass
        try:
            await run_host_cli(
                [self.docker_binary, "rm", "-f", container_name],
                timeout=20,
            )
        except Exception:
            pass
        self._release_container_port(container_name)

    async def _idle_sweeper(self) -> None:
        try:
            while True:
                await asyncio.sleep(self.sweep_interval_seconds)
                now = time.time()
                stale = []
                for _, runtime in list(self._servers.items()):
                    eff = runtime.idle_timeout_seconds
                    if eff is None:
                        eff = float(self.idle_timeout_seconds)
                    if eff < 0:
                        continue
                    if now - runtime.last_used_ts >= eff:
                        stale.append(
                            (runtime.project_id, runtime.tenant_id, runtime.server_id)
                        )
                for project_id, tenant_id, server_id in stale:
                    st_key = self._key(project_id, tenant_id, server_id)
                    rt: Optional[ExternalMCPServerRuntime] = None
                    rt = self._servers.get(st_key)
                    age_s: Optional[float] = None
                    if rt is not None:
                        age_s = now - rt.last_used_ts
                    eff_log: Any = self.idle_timeout_seconds
                    if rt and rt.idle_timeout_seconds is not None:
                        try:
                            eff_log = float(rt.idle_timeout_seconds)
                        except (TypeError, ValueError):
                            eff_log = rt.idle_timeout_seconds
                    logger.info(
                        "[EXTERNAL_MCP] [IDLE] key=%s project=%s tenant=%s server=%s "
                        "idle_s=%s age_s=%s — sweeper stop",
                        st_key,
                        project_id,
                        tenant_id,
                        server_id,
                        eff_log,
                        round(age_s, 1) if age_s is not None else None,
                    )
                    await self.stop_server(
                        project_id=project_id,
                        tenant_id=tenant_id,
                        server_id=server_id,
                        stop_reason="sweeper",
                    )
        except asyncio.CancelledError:
            return

    async def _get_host_port_for_container(
        self, container_name: str, container_port: int
    ) -> Optional[int]:
        res = await run_host_cli(
            [self.docker_binary, "port", container_name], timeout=8
        )
        if int(res.get("exit_code", 1)) != 0:
            return None
        out = res.get("stdout") or ""
        needle = f"{container_port}/tcp"
        for line in out.splitlines():
            if needle not in line:
                continue
            for token in line.split():
                if "0.0.0.0:" in token or "127.0.0.1:" in token:
                    try:
                        return int(token.rsplit(":", 1)[-1].strip())
                    except (TypeError, ValueError):
                        continue
        return None

    async def _docker_container_exists(self, container_name: str) -> bool:
        """True if docker still has this container (running or stopped)."""
        res = await run_host_cli(
            [self.docker_binary, "inspect", "--format", "{{.Id}}", container_name],
            timeout=15,
        )
        return int(res.get("exit_code", 1)) == 0 and bool((res.get("stdout") or "").strip())

    def _pick_free_port(self, start: int) -> int:
        for port in range(int(start), int(start) + 2000):
            if port in self._reserved_ports:
                continue
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                if sock.connect_ex(("127.0.0.1", port)) != 0:
                    return port
        raise RuntimeError("No free port available for external MCP container")

    async def _claim_free_port(
        self, start: int, *, container_name: Optional[str] = None
    ) -> int:
        """Reserve a free port; if container_name given, bind mapping under the same lock."""
        async with self._port_alloc_lock:
            port = self._pick_free_port(start)
            self._reserved_ports.add(port)
            if container_name:
                old = self._port_by_container.get(container_name)
                if old is not None and old != port:
                    self._reserved_ports.discard(old)
                self._port_by_container[container_name] = port
            return port

    def _release_port(self, port: Optional[int]) -> None:
        if port is None:
            return
        try:
            self._reserved_ports.discard(int(port))
        except (TypeError, ValueError):
            pass

    def _remember_container_port(self, container_name: str, port: int) -> None:
        port_i = int(port)
        old = self._port_by_container.get(container_name)
        if old is not None and old != port_i:
            self._release_port(old)
        self._port_by_container[container_name] = port_i
        self._reserved_ports.add(port_i)

    def _release_container_port(self, container_name: str) -> None:
        port = self._port_by_container.pop(container_name, None)
        self._release_port(port)

    def _container_name(self, project_id: str, tenant_id: str, server_id: str) -> str:
        def sanitize(s: str) -> str:
            out = "".join(ch if (ch.isalnum() or ch in "-_.") else "-" for ch in s.lower())
            return out.strip("-_.") or "x"

        return f"AppFactory-mcp-{sanitize(project_id)}-{sanitize(tenant_id)}-{sanitize(server_id)}"

    def _key(self, project_id: str, tenant_id: str, server_id: str) -> str:
        return f"{project_id}:{tenant_id}:{server_id}"

    async def _resolve_host_port(
        self,
        *,
        tenant_id: str,
        server_id: str,
        container_name: str,
        project_id: str = "",
    ) -> int:
        # Discovery/zip-smoke must not claim the shared persisted port of a real runtime.
        if (project_id or "").startswith("discovery-") or is_zip_smoke_ephemeral_server_id(
            server_id
        ):
            return await self._claim_free_port(
                self.host_port_start, container_name=container_name
            )
        if self.storage and hasattr(self.storage, "get_external_mcp_server_port"):
            try:
                stored_port = await self.storage.get_external_mcp_server_port(tenant_id, server_id)
                if isinstance(stored_port, int) and stored_port > 0:
                    async with self._port_alloc_lock:
                        if (
                            stored_port not in self._reserved_ports
                            and self._is_port_free(stored_port)
                        ):
                            self._reserved_ports.add(stored_port)
                            old = self._port_by_container.get(container_name)
                            if old is not None and old != stored_port:
                                self._reserved_ports.discard(old)
                            self._port_by_container[container_name] = stored_port
                            return stored_port
            except Exception as e:
                logger.warning("[EXTERNAL_MCP] failed to read stored port tenant=%s server=%s: %s", tenant_id, server_id, e)
        return await self._claim_free_port(
            self.host_port_start, container_name=container_name
        )

    async def _save_server_port(
        self,
        *,
        tenant_id: str,
        server_id: str,
        port: int,
        project_id: str = "",
    ) -> None:
        if (project_id or "").startswith("discovery-") or is_zip_smoke_ephemeral_server_id(
            server_id
        ):
            return
        if self.storage and hasattr(self.storage, "save_external_mcp_server_port"):
            try:
                await self.storage.save_external_mcp_server_port(tenant_id, server_id, int(port))
            except Exception as e:
                logger.warning("[EXTERNAL_MCP] failed to save port tenant=%s server=%s: %s", tenant_id, server_id, e)

    async def _clear_server_port(self, *, tenant_id: str, server_id: str) -> None:
        if self.storage and hasattr(self.storage, "delete_external_mcp_server_port"):
            try:
                await self.storage.delete_external_mcp_server_port(tenant_id, server_id)
            except Exception as e:
                logger.warning(
                    "[EXTERNAL_MCP] failed to clear port tenant=%s server=%s: %s",
                    tenant_id,
                    server_id,
                    e,
                )

    def _is_port_free(self, port: int) -> bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return sock.connect_ex(("127.0.0.1", port)) != 0
