"""
MCP Client for Container-Use using Official SDK

Uses the official MCP Python SDK to connect to container-use stdio server.
Based on: https://github.com/modelcontextprotocol/python-sdk
"""
import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple
from contextlib import AsyncExitStack

from mcp import ClientSession, StdioServerParameters, types
from pathlib import Path
from mcp.client.stdio import stdio_client
import re
import os
import platform
import tempfile
import base64
import uuid

from sandbox.command_lifecycle import (
    CommandTransportTimeout,
    RUNNING,
    log_lifecycle,
)

logger = logging.getLogger(__name__)


class SandboxListingError(RuntimeError):
    """The sandbox refused a listing outright — the path is a file, or absent.

    Distinct from "the directory is empty", which is a plain empty list. Callers
    that cannot tell the two apart report "nothing found" for a path they never
    managed to open.
    """


class SandboxReadError(RuntimeError):
    """The sandbox refused a read (missing path, is a directory, session refuse).

    Must not become success-with-empty-content: that looks like a real empty file
    to the agent and to the terminal gate.
    """


def _annotate_sandbox_error(detail: str) -> str:
    """Add the way out to a sandbox error that has one.

    The sandbox records each command and its output together as one argument to
    git, so the pair is capped at the kernel's 128 KiB per-argument limit. Past
    that the command really ran, but its output is thrown away and only this
    message comes back — so an agent that is told nothing but "it failed" will
    re-run the identical command and lose the output again.
    """
    if "argument list too long" in detail.lower():
        return (
            f"{detail}\n\n"
            "The command ran, but produced more than 128 KiB and the sandbox "
            "discarded its output. Re-run it writing the output to a file "
            "(e.g. `mycommand > /tmp/out.txt 2>&1`), then inspect that file "
            "with the read or grep tool instead of printing it."
        )
    return detail


_EXIT_MARKER_PREFIX = "__cu_exit_"


def _wrap_command_for_exit_status(command: str) -> Tuple[str, str]:
    """Ask the shell to report how the command ended, and return how to spot it.

    The sandbox hands back output only: it reads the exit status, files it in
    its own log, then drops it. Without this the caller is left inferring
    success from the wording of the output, which reads a command that merely
    prints "not found" as a failure.

    A trap rather than a trailing `; echo $?`, because an explicit `exit 3` ends
    the shell before anything queued behind it runs — and that is exactly the
    case worth catching. The token is per-call so that output containing the
    marker text cannot pass itself off as the real status.
    """
    marker = f"{_EXIT_MARKER_PREFIX}{uuid.uuid4().hex[:12]}="
    wrapped = f"trap 'printf \"\\n{marker}%s\\n\" \"$?\"' EXIT; {command}"
    return wrapped, marker


def _split_exit_status(text: str, marker: str) -> Tuple[str, Optional[int]]:
    """Lift the reported status out of the output and remove the line holding it.

    Reads the LAST marker: the trap fires after the command, so output that
    printed the marker itself can never get in front of the real one. A
    non-numeric value means the sandbox quoted our own trap back at us inside an
    error message rather than the shell running it, so there is no status here.

    Returning None means the shell never reported — `exec` and a command that
    installs its own EXIT trap both replace ours — and the caller should fall
    back to whatever it did before.
    """
    if not isinstance(text, str) or marker not in text:
        return text, None
    # Last marker that actually carries a number: `set -x` echoes the trap's own
    # printf line, marker and all, so the final occurrence can be the unexpanded
    # template rather than the status.
    hits = list(re.finditer(re.escape(marker) + r"(-?\d+)", text))
    if not hits:
        return text, None
    match = hits[-1]
    head, tail = text[: match.start()], text[match.end():]
    # Take back exactly the two newlines the trap wrapped its own line in — no
    # more, or output that deliberately ended in a blank line comes back altered.
    if head.endswith("\n"):
        head = head[:-1]
    if tail.startswith("\n"):
        tail = tail[1:]
    return head + tail, int(match.group(1))


class ContainerUseMCPClient:
    """
    MCP client for container-use using official SDK.
    
    Connects to 'container-use stdio' and provides methods to:
    - Create environments
    - Write/read files
    - Execute commands
    """
    
    def __init__(self, project_id: str, cli_path: str = "cu", environment_source: Optional[str] = None, environment_id: Optional[str] = None):
        """
        Initialize MCP client.
        
        Args:
            project_id: Unique project identifier
            cli_path: Path to container-use CLI (default: "cu")
        """
        self.project_id = project_id
        self.cli_path = cli_path
        self.session: Optional[ClientSession] = None
        self.exit_stack: Optional[AsyncExitStack] = None
        self.environment_id: Optional[str] = environment_id
        # Set environment_source to the provided repo path if given; otherwise infer from current project root
        if environment_source:
            self.environment_source = environment_source
        else:
            # This module lives at: <repo>/src/sandbox/mcp_client_sdk.py → repo root is parents[2]
            try:
                self.environment_source: Optional[str] = str(Path(__file__).resolve().parents[2])
            except Exception:
                # Fallback: current working directory
                self.environment_source = str(Path.cwd())
        
        # Server parameters for stdio connection
        env_map = dict(os.environ)
        if platform.system() != "Windows" and not env_map.get("DOCKER_HOST"):
            env_map["DOCKER_HOST"] = "tcp://localhost:2375"
        env_map.setdefault("NO_COLOR", "1")
        env_map.setdefault("PAGER", "")
        env_map.setdefault("TERM", "dumb")
        # Capture server stderr to a file for post-mortem if it exits early
        safe_pid = (self.project_id or "proj").replace("/", "_")
        self._stderr_path = os.path.join(tempfile.gettempdir(), f"cu-stdio-{safe_pid}.err")
        # Use platform-specific launch for stdio server
        if platform.system() == "Windows":
            # Directly spawn the CLI: more reliable on Windows than going through cmd.exe
            self.server_params = StdioServerParameters(
                command=cli_path,
                args=[
                    "stdio",
                ],
                env=env_map,
                cwd=self.environment_source,
            )
        else:
            from pathlib import Path as _Path
            self.server_params = StdioServerParameters(
                command="/bin/sh",
                args=[
                    "-lc",
                    f"exec {cli_path} stdio --single-tenant 2> {_Path(self._stderr_path)}",
                ],
                env=env_map,
                cwd=self.environment_source,
            )
        try:
            logger.info(
                "MCPClient init: project=%s cli_path=%s env_source=%s cwd=%s DOCKER_HOST=%s stderr_path=%s",
                self.project_id,
                self.cli_path,
                self.environment_source,
                getattr(self, "environment_source", None),
                env_map.get("DOCKER_HOST"),
                self._stderr_path,
            )
        except Exception:
            pass
    
    async def connect(self):
        """
        Connect to container-use MCP server and initialize session.
        """
        try:
            # Create exit stack for managing async context
            self.exit_stack = AsyncExitStack()
            
            # Connect to stdio server
            try:
                logger.info(
                    "Starting container-use stdio: cmd=%s args=%s DOCKER_HOST=%s",
                    self.server_params.command,
                    " ".join(self.server_params.args or []),
                    (self.server_params.env or {}).get("DOCKER_HOST"),
                )
            except Exception:
                pass
            read, write = await self.exit_stack.enter_async_context(
                stdio_client(self.server_params)
            )
            
            # Create client session
            self.session = await self.exit_stack.enter_async_context(
                ClientSession(read, write)
            )
            
            # Initialize the connection
            await self.session.initialize()
            
            logger.info(f"Connected to container-use MCP server for project {self.project_id}")
            
            # List available tools
            tools = await self.session.list_tools()
            try:
                names = [getattr(t, "name", "unknown") for t in tools.tools]
                logger.info("Tools available (%d): %s", len(names), ", ".join(names))
            except Exception:
                logger.info("Tools available: %s", str(getattr(tools, "tools", [])))
            
        except Exception as e:
            logger.error(f"Failed to connect to container-use: {e}")
            # Swallow cleanup errors so we don't bubble anyio cancel-scope issues
            try:
                if self.exit_stack:
                    import asyncio as _asyncio
                    await _asyncio.wait_for(self.exit_stack.aclose(), timeout=1.0)
            except BaseException as close_err:  # include CancelledError
                logger.warning(f"Ignoring MCP cleanup error: {close_err}")
            # Attempt to read stdio stderr for diagnostics
            try:
                if getattr(self, "_stderr_path", None):
                    from pathlib import Path as _Path
                    errp = _Path(self._stderr_path)
                    if errp.exists():
                        try:
                            err_txt = errp.read_text(encoding="utf-8", errors="replace")
                            logger.error("container-use stdio stderr (%s): %s", str(errp), err_txt[:1000])
                        except Exception as _re:
                            logger.warning(f"Could not read stdio stderr file {errp}: {_re}")
            except Exception:
                pass
            # Try a diagnostic `cu list` in the repo to surface a concrete error
            try:
                from .host_cli import run_host_cli
                diag = await run_host_cli([self.cli_path, "list"], cwd=self.environment_source)
                logger.error(
                    "cu list diag: exit_code=%s stdout_len=%s stderr_head=%s",
                    diag.get("exit_code"),
                    len(diag.get("stdout", "")),
                    (diag.get("stderr", "")[:400] if isinstance(diag.get("stderr", ""), str) else str(diag.get("stderr")))
                )
            except Exception as _diage:
                logger.warning(f"Failed to run cu list diag: {_diage}")
            raise
    
    async def create_environment(
        self,
        *,
        from_git_ref: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a Container-Use environment on ``environment_source``.

        ``from_git_ref``: ref in the source repo to start from; Container-Use
        uses HEAD when omitted.
        """
        if not self.session:
            raise RuntimeError("Not connected. Call connect() first.")
        
        try:
            logger.info(
                "Calling environment_create for project %s (source=%s from_git_ref=%s)",
                self.project_id,
                self.environment_source,
                from_git_ref or "HEAD",
            )
            # Find environment_create tool
            tools = await self.session.list_tools()
            create_tool = None
            
            for tool in tools.tools:
                if "environment_create" in tool.name:
                    create_tool = tool.name
                    break
            
            if not create_tool:
                raise RuntimeError("environment_create tool not found")

            arguments: Dict[str, Any] = {
                "environment_source": self.environment_source,
                "title": f"AppFactory {self.project_id}",
                "explanation": "Minimal PoC environment creation",
            }
            if from_git_ref:
                arguments["from_git_ref"] = from_git_ref
            
            result = await self.session.call_tool(
                create_tool,
                arguments=arguments,
            )
            try:
                logger.info("environment_create returned type=%s", type(result).__name__)
            except Exception:
                pass
            
            # Store environment ID from result if available
            # Try structuredContent first; fallback to parsing text content as JSON
            env_id: Optional[str] = None
            try:
                if getattr(result, "structuredContent", None):
                    structured = result.structuredContent
                    if isinstance(structured, dict):
                        if structured.get("environment_id"):
                            env_id = structured.get("environment_id")
                        elif structured.get("id"):
                            env_id = structured.get("id")
                        elif structured.get("environment") and isinstance(structured.get("environment"), dict):
                            env_id = structured["environment"].get("id") or structured["environment"].get("environment_id")
                    elif isinstance(structured, list) and structured:
                        first = structured[0]
                        if isinstance(first, dict):
                            env_id = first.get("environment_id") or first.get("id") or (
                                first.get("environment", {}).get("id") if isinstance(first.get("environment"), dict) else None
                            )
                if not env_id and getattr(result, "content", None):
                    if len(result.content) > 0:
                        # Scan all text blocks for an env id (JSON blob or inline commands)
                        for block in result.content:
                            if isinstance(block, types.TextContent):
                                candidate = self._extract_env_id_from_text(block.text)
                                if candidate:
                                    env_id = candidate
                                    break
            except Exception:
                pass

            if env_id:
                self.environment_id = env_id
                logger.info(f"Environment created (id={env_id}) for project {self.project_id}")
            else:
                logger.info(f"Environment created for project {self.project_id} (no environment_id parsed)")
                # Try opening the environment to retrieve an ID
                try:
                    await self.open_environment()
                except Exception as e:
                    logger.warning(
                        "open_environment fallback failed for project %s after environment_create returned no parseable id: %s: %s",
                        self.project_id, type(e).__name__, e,
                        exc_info=True,
                    )
            
            # Return raw result details for debugging
            raw_structured = None
            raw_content_texts: List[str] = []
            try:
                raw_structured = getattr(result, "structuredContent", None)
                if getattr(result, "content", None):
                    for blk in result.content:
                        if isinstance(blk, types.TextContent):
                            raw_content_texts.append(blk.text)
                        else:
                            raw_content_texts.append(str(blk))
            except Exception:
                pass

            return {
                "status": "success",
                "environment_id": self.environment_id,
                "raw_structured": raw_structured,
                "raw_content": raw_content_texts,
            }
            
        except Exception as e:
            logger.error(f"Failed to create environment: {e}")
            raise
    
    async def write_file(self, path: str, content: str) -> Dict[str, Any]:
        """
        Write file to container environment.
        
        Args:
            path: File path in container
            content: File content
            
        Returns:
            Result from file_write tool
        """
        if not self.session:
            raise RuntimeError("Not connected. Call connect() first.")
        
        try:
            await self.ensure_environment()
            tools = await self.session.list_tools()
            write_tool = None
            
            for tool in tools.tools:
                if "file_write" in tool.name:
                    write_tool = tool.name
                    break
            
            if not write_tool:
                raise RuntimeError("file_write tool not found")
            
            args = {
                "target_file": path,
                "contents": content,
            }
            # Both env_source and env_id are required by schema; include both when id available
            if isinstance(self.environment_id, str) and self.environment_id:
                args["environment_id"] = self.environment_id
            args["environment_source"] = self.environment_source

            result = await self.session.call_tool(write_tool, arguments=args)

            if getattr(result, "isError", False):
                # Same contract as execute_command: isError means the sandbox
                # refused the write — not a successful empty write.
                detail = " ".join(
                    str(getattr(blk, "text", "")).strip()
                    for blk in (getattr(result, "content", None) or [])
                ).strip() or "sandbox rejected the write"
                return {
                    "stdout": "",
                    "stderr": detail,
                    "exit_code": 1,
                    "error_type": "unavailable",
                }

            logger.debug(f"Wrote file: {path}")
            return {"status": "success", "path": path}
            
        except Exception as e:
            logger.error(f"Failed to write file {path}: {e}")
            raise
    
    async def read_file(self, path: str) -> str:
        """
        Read file from container environment.
        
        Args:
            path: File path in container
            
        Returns:
            File content
        """
        if not self.session:
            raise RuntimeError("Not connected. Call connect() first.")
        
        try:
            await self.ensure_environment()
            tools = await self.session.list_tools()
            read_tool = None
            
            for tool in tools.tools:
                if "file_read" in tool.name:
                    read_tool = tool.name
                    break
            
            if not read_tool:
                raise RuntimeError("file_read tool not found")
            
            args = {
                "target_file": path,
                "should_read_entire_file": True,
            }
            if isinstance(self.environment_id, str) and self.environment_id:
                args["environment_id"] = self.environment_id
            args["environment_source"] = self.environment_source

            async def _read_once(read_args: Dict[str, Any]) -> str:
                res = await self.session.call_tool(read_tool, arguments=read_args)
                if getattr(res, "isError", False):
                    detail = " ".join(
                        str(getattr(b, "text", "")).strip()
                        for b in (getattr(res, "content", None) or [])
                    ).strip()
                    raise SandboxReadError(detail or f"could not read {path}")
                out = ""
                try:
                    if getattr(res, "content", None):
                        for blk in res.content:
                            # Text block
                            if isinstance(blk, types.TextContent):
                                out += blk.text or ""
                                continue
                            # Generic blob-like block: any object with a 'data' attribute
                            try:
                                data = getattr(blk, "data", None)
                            except Exception:
                                data = None
                            if data is not None:
                                decoded = ""
                                if isinstance(data, (bytes, bytearray)):
                                    try:
                                        decoded = data.decode("utf-8", errors="replace")
                                    except Exception:
                                        decoded = data.decode("latin-1", errors="replace")
                                elif isinstance(data, str):
                                    try:
                                        decoded_bytes = base64.b64decode(data, validate=False)
                                        decoded = decoded_bytes.decode("utf-8", errors="replace")
                                    except Exception:
                                        decoded = data
                                out += decoded
                                continue
                            # Fallback stringify for unknown block types
                            try:
                                out += str(blk)
                            except Exception:
                                pass
                    if not out and getattr(res, "structuredContent", None):
                        sc = res.structuredContent
                        if isinstance(sc, dict):
                            for k in ("contents", "content", "text", "data"):
                                v = sc.get(k)
                                if isinstance(v, str):
                                    out = v
                                    break
                                if isinstance(v, (bytes, bytearray)):
                                    try:
                                        out = v.decode("utf-8", errors="replace")
                                    except Exception:
                                        out = v.decode("latin-1", errors="replace")
                                    break
                                if isinstance(v, dict):
                                    bv = v.get("data") or v.get("value")
                                    if isinstance(bv, str):
                                        try:
                                            out = base64.b64decode(bv, validate=False).decode("utf-8", errors="replace")
                                        except Exception:
                                            out = bv
                                    break
                except Exception:
                    try:
                        return str(getattr(res, "content", "")) or ""
                    except Exception:
                        return ""
                return out

            # Perform first attempt with provided path
            result_text = await _read_once(args)
            variant_used = "original"
            # Path fallbacks if empty: try './path' and '/workdir/path'
            if not result_text and isinstance(path, str):
                try:
                    # './path'
                    alt_args = dict(args)
                    alt_args["target_file"] = f"./{path}" if not str(path).startswith("./") else path
                    got = await _read_once(alt_args)
                    if got:
                        result_text = got
                        variant_used = "dot"
                except Exception:
                    pass
            if not result_text and isinstance(path, str):
                try:
                    # '/workdir/path'
                    alt_args2 = dict(args)
                    # Ensure no leading slash duplication
                    stripped = path[1:] if path.startswith("/") else path
                    alt_args2["target_file"] = f"/workdir/{stripped}"
                    got2 = await _read_once(alt_args2)
                    if got2:
                        result_text = got2
                        variant_used = "workdir"
                except Exception:
                    pass

            try:
                logger.info("file_read: path=%s variant=%s len=%s", path, variant_used, len(result_text or ""))
            except Exception:
                pass

            return result_text
            
        except Exception as e:
            logger.error(f"Failed to read file {path}: {e}")
            raise
    
    async def delete_file(self, path: str) -> Dict[str, Any]:
        """
        Delete a file in the container environment.
        """
        if not self.session:
            raise RuntimeError("Not connected. Call connect() first.")
        try:
            await self.ensure_environment()
            tools = await self.session.list_tools()
            delete_tool = None
            for tool in tools.tools:
                if "file_delete" in getattr(tool, "name", ""):
                    delete_tool = tool.name
                    break
            if not delete_tool:
                raise RuntimeError("file_delete tool not found")
            args = {
                "target_file": path,
            }
            if isinstance(self.environment_id, str) and self.environment_id:
                args["environment_id"] = self.environment_id
            else:
                args["environment_source"] = self.environment_source
            mcp_result = await self.session.call_tool(delete_tool, arguments=args)
            if getattr(mcp_result, "isError", False):
                detail = " ".join(
                    str(getattr(blk, "text", "")).strip()
                    for blk in (getattr(mcp_result, "content", None) or [])
                ).strip() or "sandbox rejected the delete"
                return {
                    "stdout": "",
                    "stderr": detail,
                    "exit_code": 1,
                    "error_type": "unavailable",
                }
            logger.debug(f"Deleted file: {path}")
            return {"status": "success", "path": path}
        except Exception as e:
            logger.error(f"Failed to delete file {path}: {e}")
            raise

    async def find_replace_file(self, path: str, search_text: str, replace_text: str, which_match: Optional[str] = None) -> Dict[str, Any]:
        """
        Find and replace text in a file in the container environment.
        """
        if not self.session:
            raise RuntimeError("Not connected. Call connect() first.")
        try:
            await self.ensure_environment()
            tools = await self.session.list_tools()
            edit_tool = None
            for tool in tools.tools:
                if "file_edit" in getattr(tool, "name", ""):
                    edit_tool = tool.name
                    break
            if not edit_tool:
                raise RuntimeError("file_edit tool not found")
            args: Dict[str, Any] = {
                "target_file": path,
                "search_text": search_text,
                "replace_text": replace_text,
            }
            if which_match:
                args["which_match"] = which_match
            if isinstance(self.environment_id, str) and self.environment_id:
                args["environment_id"] = self.environment_id
            else:
                args["environment_source"] = self.environment_source
            mcp_result = await self.session.call_tool(edit_tool, arguments=args)
            if getattr(mcp_result, "isError", False):
                detail = " ".join(
                    str(getattr(blk, "text", "")).strip()
                    for blk in (getattr(mcp_result, "content", None) or [])
                ).strip() or "sandbox rejected the find/replace"
                return {
                    "stdout": "",
                    "stderr": detail,
                    "exit_code": 1,
                    "error_type": "unavailable",
                }
            logger.debug(f"Edited file (find/replace): {path}")
            return {"status": "success", "path": path}
        except Exception as e:
            logger.error(f"Failed to edit file {path}: {e}")
            raise
    
    async def execute_command(
        self,
        command: str,
        cwd: str = ".",
        *,
        timeout_seconds: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Execute command in container environment.
        
        Args:
            command: Shell command to execute
            cwd: Working directory (unused by CU MCP; kept for call-site compat)
            timeout_seconds: Optional client wait for the full MCP round-trip
                (ensure environment + list_tools + call_tool). When set,
                expiry raises CommandTransportTimeout — never mapped to
                exit_code: 1.
            
        Returns:
            Command result with stdout, stderr, exit_code
        """
        del cwd
        if not self.session:
            raise RuntimeError("Not connected. Call connect() first.")

        run_tool_name = "environment_run_cmd"
        
        try:
            async def _mcp_round_trip():
                nonlocal run_tool_name
                await self.ensure_environment()
                tools = await self.session.list_tools()
                run_tool = None

                for tool in tools.tools:
                    if "run_cmd" in tool.name:
                        run_tool = tool.name
                        break

                if not run_tool:
                    raise RuntimeError("run_cmd tool not found")
                run_tool_name = run_tool

                wrapped_command, exit_marker = _wrap_command_for_exit_status(command)
                args = {
                    "command": wrapped_command,
                    "environment_source": self.environment_source,
                }
                if isinstance(self.environment_id, str) and self.environment_id:
                    args["environment_id"] = self.environment_id

                log_lifecycle(
                    "start",
                    project_id=self.project_id,
                    environment_id=self.environment_id,
                    tool=run_tool,
                    command_state=RUNNING,
                    timeout_seconds=timeout_seconds,
                )
                result = await self.session.call_tool(run_tool, arguments=args)
                return result, exit_marker

            try:
                if timeout_seconds is not None and timeout_seconds > 0:
                    result, exit_marker = await asyncio.wait_for(
                        _mcp_round_trip(), timeout=float(timeout_seconds)
                    )
                else:
                    result, exit_marker = await _mcp_round_trip()
            except asyncio.TimeoutError as exc:
                try:
                    budget = (
                        float(timeout_seconds) if timeout_seconds is not None else 0.0
                    )
                except (TypeError, ValueError):
                    budget = 0.0
                if budget < 0:
                    budget = 0.0
                log_lifecycle(
                    "timeout",
                    project_id=self.project_id,
                    environment_id=self.environment_id,
                    tool=run_tool_name,
                    timeout_seconds=budget,
                )
                raise CommandTransportTimeout(
                    tool_name=run_tool_name,
                    timeout_seconds=budget,
                    project_id=self.project_id,
                    environment_id=self.environment_id,
                ) from exc

            if getattr(result, "isError", False):
                # Sandbox refused the call; the command's real output is gone.
                # Tag as typed infra for the terminal gate — do NOT set
                # outcome_unknown (that hard-aborts the agent loop before retry).
                detail = " ".join(
                    str(getattr(blk, "text", "")).strip()
                    for blk in (getattr(result, "content", None) or [])
                ).strip() or "sandbox rejected the command"
                return {
                    "stdout": "",
                    "stderr": _annotate_sandbox_error(detail),
                    "exit_code": 1,
                    "error_type": "unavailable",
                }

            # Prefer structured result fields when MCP tool provides them
            stdout_text = ""
            stderr_text = ""
            exit_code: Optional[int] = None
            try:
                structured = getattr(result, "structuredContent", None)
                if isinstance(structured, dict):
                    if isinstance(structured.get("stdout"), str):
                        stdout_text = structured.get("stdout", "")
                    if isinstance(structured.get("stderr"), str):
                        stderr_text = structured.get("stderr", "")
                    if isinstance(structured.get("exit_code"), int):
                        exit_code = structured.get("exit_code")
                    elif isinstance(structured.get("status"), int):
                        exit_code = structured.get("status")
                    elif isinstance(structured.get("code"), int):
                        exit_code = structured.get("code")
            except Exception:
                pass

            # Fallback to textual content if structured fields are missing
            try:
                if not stdout_text and result.content and len(result.content) > 0:
                    block = result.content[0]
                    if isinstance(block, types.TextContent):
                        stdout_text = block.text
                    else:
                        stdout_text = str(result.content)
            except Exception:
                if not stdout_text:
                    stdout_text = str(getattr(result, "content", ""))

            stdout_text, reported_exit = _split_exit_status(stdout_text, exit_marker)
            if reported_exit is not None:
                exit_code = reported_exit

            # Heuristic: treat obvious error messages as failures (avoid generic 'error' substring)
            # Only when the shell did not report — reading intent from the wording
            # calls a command that prints "not found" a failure and drops its output.
            if exit_code is None:
                lowered = stdout_text.lower()
                if (
                    "required argument\n" in lowered
                    or 'required argument "environment_source"' in lowered
                    or 'required argument "environment_id"' in lowered
                    or "environment \"" in lowered and "not found" in lowered
                    or 'argument "environment_id" is not a string' in lowered
                ):
                    return {"stdout": "", "stderr": stdout_text, "exit_code": 1}
                # Command-not-found patterns from sh/bash
                not_found_patterns = [
                    "command not found",
                    "not found",
                    "python: not found",
                    "python3: not found",
                    "pip: not found",
                    "pip3: not found",
                    "pytest: not found",
                    "no such file or directory",
                ]
                if any(pat in lowered for pat in not_found_patterns):
                    return {"stdout": "", "stderr": stdout_text, "exit_code": 127}

            # If tool encoded stderr into stdout (common "stderr: ..."), treat as failure.
            # This prevents false-positive successes when command output contains traceback.
            if not stderr_text and isinstance(stdout_text, str):
                # lstrip first: the sandbox only omits the leading newline before
                # "stderr:" when stdout was empty, and the exit-status line means
                # it never is, so an anchored match would stop finding stderr.
                unpadded = stdout_text.lstrip()
                lowered_stdout = unpadded.lower()
                if lowered_stdout.startswith("stderr:"):
                    stderr_text = unpadded[len("stderr:"):].lstrip()
                    stdout_text = ""
                elif exit_code is None and "traceback (most recent call last)" in lowered_stdout:
                    stderr_text = stdout_text
                    stdout_text = ""

            # Derive exit code from stderr/traceback if MCP did not provide one.
            if exit_code is None:
                exit_code = 0
                if isinstance(stderr_text, str) and stderr_text.strip():
                    exit_code = 1
                elif isinstance(stdout_text, str) and "traceback (most recent call last)" in stdout_text.lower():
                    exit_code = 1

            return {
                "stdout": stdout_text,
                "stderr": stderr_text,
                "exit_code": int(exit_code),
            }

        except CommandTransportTimeout:
            raise
        except TimeoutError:
            raise
        except ConnectionError:
            raise
        except RuntimeError:
            # Missing tool / not connected — never domain exit 1 (gate needs the raise).
            raise
        except Exception as e:
            logger.error(f"Failed to execute command: {e}")
            return {
                "stdout": "",
                "stderr": str(e),
                "exit_code": 1,
            }
    
    async def list_files(self, path: str = ".") -> List[str]:
        """
        List files in the container workspace using container-use stdio MCP.
        Uses environment_file_list if available; falls back to file_list.
        """
        if not self.session:
            raise RuntimeError("Not connected. Call connect() first.")

        try:
            await self.ensure_environment()
            tools = await self.session.list_tools()
            list_tool = None
            for tool in tools.tools:
                name = getattr(tool, "name", "")
                if "environment_file_list" in name:
                    list_tool = name
                    break
            if not list_tool:
                for tool in tools.tools:
                    name = getattr(tool, "name", "")
                    if "file_list" in name:
                        list_tool = name
                        break
            if not list_tool:
                raise RuntimeError("environment_file_list/file_list tool not found")

            args: Dict[str, Any] = {"path": path}
            # Prefer to provide both identifiers when available to satisfy stricter schemas
            if isinstance(self.environment_id, str) and self.environment_id:
                args["environment_id"] = self.environment_id
            args["environment_source"] = self.environment_source
            # Some implementations accept an explanation for auditability
            args["explanation"] = "List workspace directory contents"

            result = await self.session.call_tool(list_tool, arguments=args)

            if getattr(result, "isError", False):
                # The sandbox says exactly what went wrong here ("path X is a
                # file, not a directory"). The parser below drops that sentence
                # so it is never mistaken for a filename, which leaves an empty
                # list and loses the reason with it.
                detail = " ".join(
                    str(getattr(blk, "text", "")).strip()
                    for blk in (getattr(result, "content", None) or [])
                ).strip()
                raise SandboxListingError(detail or f"could not list {path}")

            # Parse results
            files: List[str] = []

            def _append_structured_entry(entry: Any) -> None:
                if not isinstance(entry, dict):
                    return
                p = entry.get("path") or entry.get("name")
                if not isinstance(p, str):
                    return
                # Dict entries carry directory-ness in a type field, not the
                # trailing slash the search walker keys on — re-mark it here,
                # or every subdirectory reads back as a plain file and the
                # walk silently stops at the top level.
                kind = entry.get("type")
                is_dir = (
                    kind.lower() in ("directory", "dir")
                    if isinstance(kind, str)
                    else bool(entry.get("isDirectory") or entry.get("is_dir"))
                )
                if is_dir and not p.endswith("/"):
                    p += "/"
                files.append(p)

            try:
                structured = getattr(result, "structuredContent", None)
                if structured:
                    # Common shapes: list of entries or dict with entries
                    if isinstance(structured, list):
                        for entry in structured:
                            _append_structured_entry(entry)
                    elif isinstance(structured, dict):
                        entries = structured.get("entries") or structured.get("files") or []
                        if isinstance(entries, list):
                            for entry in entries:
                                _append_structured_entry(entry)
                # Fallback: parse text content lines
                if not files and getattr(result, "content", None):
                    for blk in result.content:
                        try:
                            from mcp import types as _types
                            if isinstance(blk, _types.TextContent):
                                for line in (blk.text or "").splitlines():
                                    line = line.strip()
                                    if not line or line in (".", ".."):
                                        continue
                                    if len(line) > 512:
                                        continue
                                    lower_line = line.lower()
                                    # Skip obvious error messages so they are never treated as filenames
                                    if (
                                        "failed to list directory" in lower_line
                                        or "failed to stat file" in lower_line
                                        or "no such file or directory" in lower_line
                                        or "file name too long" in lower_line
                                        or "is a file, not a directory" in lower_line
                                        or "unable to get environment" in lower_line
                                        or ("environment \"" in lower_line and "not found" in lower_line)
                                    ):
                                        continue
                                    if line.startswith("-") or line.startswith("*"):
                                        line = line.lstrip("-* ")
                                    if line:
                                        files.append(line)
                        except Exception:
                            pass
            except Exception as pe:
                logger.warning(f"Failed to parse list_files result: {pe}")

            # De-duplicate and normalize
            norm: List[str] = []
            seen = set()
            for p in files:
                try:
                    # The sandbox marks a directory with a trailing slash, and the
                    # search walker relies on that mark to know what to recurse
                    # into. str(Path()) drops it, so re-attach it after normalizing
                    # — otherwise every subdirectory reads back as a plain file and
                    # the walk never descends.
                    is_dir = isinstance(p, str) and p.rstrip().endswith("/")
                    pp = str(Path(p)).replace("\\", "/")
                    if is_dir and not pp.endswith("/"):
                        pp += "/"
                    if pp not in seen:
                        seen.add(pp)
                        norm.append(pp)
                except Exception:
                    continue

            return norm
        except SandboxListingError:
            raise
        except TimeoutError:
            raise
        except Exception as e:
            logger.error(f"Failed to list files: {e}")
            # Empty list means "directory is empty", not "listing failed" — raise so
            # the executor can tag infra and existence checks refuse create/edit.
            raise

    async def export_artifacts(self, dest_path: str) -> Dict[str, Any]:
        """
        Minimal PoC: stubbed export that reports success.
        The CLI already writes artifacts from shared_context to disk.
        """
        logger.info(f"Stub export_artifacts to {dest_path} (PoC) ")
        return {"stdout": "", "stderr": "", "exit_code": 0}

    async def list_available_tools(self) -> List[Dict[str, Any]]:
        """Return available tool names and basic schemas for debugging."""
        if not self.session:
            raise RuntimeError("Not connected. Call connect() first.")
        tools = await self.session.list_tools()
        out: List[Dict[str, Any]] = []
        for t in tools.tools:
            schema = getattr(t, "inputSchema", None)
            schema_str = None
            try:
                if schema is not None and not isinstance(schema, dict):
                    schema_str = str(schema)
            except Exception:
                schema_str = None
            out.append({
                "name": getattr(t, "name", "unknown"),
                "schema": schema if isinstance(schema, dict) else None,
                "schema_str": schema_str,
            })
        return out

    async def open_environment(self) -> Optional[str]:
        if not self.session:
            raise RuntimeError("Not connected. Call connect() first.")
        tools = await self.session.list_tools()
        open_tool = None
        for tool in tools.tools:
            if "environment_open" in tool.name:
                open_tool = tool.name
                break
        if not open_tool:
            raise RuntimeError("environment_open tool not found")
        if not isinstance(self.environment_id, str) or not self.environment_id:
            raise RuntimeError(
                "environment_open requires environment_id (schema required)"
            )
        result = await self.session.call_tool(
            open_tool,
            arguments={
                "environment_source": self.environment_source,
                "environment_id": self.environment_id,
            },
        )
        env_id: Optional[str] = None
        try:
            if getattr(result, "structuredContent", None):
                structured = result.structuredContent
                if isinstance(structured, dict):
                    env_id = structured.get("environment_id") or structured.get("id")
                elif isinstance(structured, list) and structured:
                    first = structured[0]
                    if isinstance(first, dict):
                        env_id = first.get("environment_id") or first.get("id")
            if not env_id and getattr(result, "content", None):
                for block in result.content:
                    if isinstance(block, types.TextContent):
                        candidate = self._extract_env_id_from_text(block.text)
                        if candidate:
                            env_id = candidate
                            break
        except Exception:
            pass
        if env_id:
            self.environment_id = env_id
            logger.info(f"Environment opened (id={env_id}) for project {self.project_id}")
        return self.environment_id

    def _extract_env_id_from_text(self, text: str) -> Optional[str]:
        """Extract environment id from mixed text. Tries JSON key, then CLI hints."""
        # 1) JSON key: "id":"<env_id>"
        m = re.search(r'"id"\s*:\s*"([^"]+)"', text)
        if m:
            return m.group(1)
        # 2) CLI command hints: container-use (checkout|log|diff) <env_id>
        m2 = re.search(r'container-use\s+(?:checkout|log|diff)\s+([\w\-]+)', text)
        if m2:
            return m2.group(1)
        return None

    async def ensure_environment(self):
        """Ensure we have an environment_id by opening environment if needed."""
        if not self.environment_id:
            await self.open_environment()
    
    async def close(self):
        """Close MCP connection.

        Detach ``exit_stack`` first, then wait briefly for ``aclose``. Use
        ``asyncio.wait`` (not ``wait_for``): after a cancelled in-flight
        ``call_tool``, anyio cancel-scope can make ``aclose`` ignore cancel, and
        ``wait_for`` then hangs until the orphan MCP round-trip finishes.
        """
        stack = self.exit_stack
        self.exit_stack = None
        self.session = None
        if stack is None:
            return
        task = asyncio.create_task(
            stack.aclose(), name=f"cu-aclose-{self.project_id}"
        )
        try:
            done, _pending = await asyncio.wait({task}, timeout=2.0)
            if not done:
                logger.warning(
                    "MCP connection aclose abandoned after 2s for project %s",
                    self.project_id,
                )
            elif not task.cancelled():
                exc = task.exception()
                if exc is not None:
                    logger.warning(
                        "Error closing MCP exit stack for project %s: %s",
                        self.project_id,
                        exc,
                    )
        except Exception as e:
            logger.warning(
                f"Error closing MCP exit stack for project {self.project_id}: {e}"
            )
        logger.info(f"Closed MCP connection for project {self.project_id}")
