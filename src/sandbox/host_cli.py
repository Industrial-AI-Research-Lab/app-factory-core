"""
Host CLI utilities for running container-use commands on the host filesystem.
"""
from __future__ import annotations

import asyncio
from typing import Dict, Any, Mapping, Optional


async def run_host_cli(
    cmd: list[str],
    cwd: Optional[str] = None,
    timeout: int = 600,
    env: Optional[Mapping[str, str]] = None,
    stdin: Optional[str | bytes] = None,
) -> Dict[str, Any]:
    """Run a host CLI command asynchronously and return stdout/stderr/exit_code.
    
    Args:
        cmd: Command and arguments to run
        cwd: Working directory
        timeout: Timeout in seconds (default 600s / 10 minutes for docker builds)
        env: Optional environment overrides for the subprocess
        stdin: Optional text/bytes passed to subprocess stdin
    """
    import logging
    import subprocess
    logger = logging.getLogger(__name__)
    
    logger.info(f"🔧 run_host_cli: Running {' '.join(cmd)} in {cwd} (timeout={timeout}s)")
    
    try:
        import os
        process_env = os.environ.copy()
        process_env.update({
            "NO_COLOR": "1",
            "PAGER": "",
            "TERM": "dumb",
        })
        if env:
            process_env.update(dict(env))

        input_bytes = stdin.encode("utf-8") if isinstance(stdin, str) else stdin
        
        loop = asyncio.get_event_loop()
        
        def _run_sync():
            result = subprocess.run(
                cmd,
                cwd=str(cwd) if cwd else None,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=process_env,
                input=input_bytes,
                timeout=timeout,
            )
            return result
        
        result = await loop.run_in_executor(None, _run_sync)
        
        stdout_str = result.stdout.decode("utf-8", errors="replace")
        stderr_str = result.stderr.decode("utf-8", errors="replace")
        
        logger.info(f"📊 run_host_cli: exit_code={result.returncode}, stdout_len={len(stdout_str)}, stderr_len={len(stderr_str)}")
        
        return {
            "stdout": stdout_str,
            "stderr": stderr_str,
            "exit_code": result.returncode,
        }
    except FileNotFoundError:
        logger.error(f"❌ Command not found: {cmd[0]}")
        return {"stdout": "", "stderr": f"Command not found: {cmd[0]}", "exit_code": 127}
    except subprocess.TimeoutExpired:
        logger.error(f"⏱️  Command timed out after {timeout}s")
        return {"stdout": "", "stderr": f"Command timed out after {timeout} seconds", "exit_code": 124}
    except Exception as e:
        logger.error(f"❌ Exception running command: {e}", exc_info=True)
        return {"stdout": "", "stderr": str(e), "exit_code": 1}
