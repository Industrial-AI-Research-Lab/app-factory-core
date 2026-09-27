"""
Container-Use CLI Wrapper

Simpler approach: Call `cu` CLI directly instead of MCP stdio.
This avoids implementing the full MCP protocol.
"""

import asyncio
import json
from typing import Dict, Any, List
from pathlib import Path


class ContainerUseCLI:
    """
    Wrapper for container-use CLI commands.
    
    Uses subprocess calls instead of MCP protocol.
    """
    
    def __init__(self, project_id: str, cli_path: str = "cu"):
        self.project_id = project_id
        self.cli_path = cli_path
        self.environment_id = None
    
    async def create_environment(self, image: str = "python:3.11-slim") -> Dict[str, Any]:
        """Create container environment."""
        # Use project ID as environment name
        cmd = [
            self.cli_path,
            "create",
            "--name", self.project_id,
            "--image", image
        ]
        
        result = await self._run_command(cmd)
        if result["exit_code"] == 0:
            self.environment_id = self.project_id
        
        return result
    
    async def write_file(self, path: str, content: str) -> Dict[str, Any]:
        """Write file to container."""
        if not self.environment_id:
            raise RuntimeError("Environment not created")
        
        # Create temp file with content
        temp_file = Path(f".temp_{self.project_id}_{Path(path).name}")
        temp_file.write_text(content, encoding="utf-8")
        
        try:
            # Copy file into container
            cmd = [
                self.cli_path,
                "cp",
                str(temp_file),
                f"{self.environment_id}:{path}"
            ]
            
            result = await self._run_command(cmd)
            return result
        finally:
            # Cleanup temp file
            if temp_file.exists():
                temp_file.unlink()
    
    async def execute_command(self, command: str) -> Dict[str, Any]:
        """Execute command in container."""
        if not self.environment_id:
            raise RuntimeError("Environment not created")
        
        cmd = [
            self.cli_path,
            "exec",
            self.environment_id,
            "--",
            "sh", "-c", command
        ]
        
        return await self._run_command(cmd)
    
    async def read_file(self, path: str) -> str:
        """Read file from container."""
        if not self.environment_id:
            raise RuntimeError("Environment not created")
        
        cmd = [
            self.cli_path,
            "exec",
            self.environment_id,
            "--",
            "cat", path
        ]
        
        result = await self._run_command(cmd)
        return result.get("stdout", "")
    
    async def list_files(self, path: str = ".") -> List[str]:
        """List files in container directory."""
        if not self.environment_id:
            raise RuntimeError("Environment not created")
        
        cmd = [
            self.cli_path,
            "exec",
            self.environment_id,
            "--",
            "find", path, "-type", "f"
        ]
        
        result = await self._run_command(cmd)
        if result["exit_code"] == 0:
            files = result["stdout"].strip().split("\n")
            return [f for f in files if f]
        return []
    
    async def export_artifacts(self, dest_path: str) -> Dict[str, Any]:
        """Export all files from container to local directory."""
        if not self.environment_id:
            raise RuntimeError("Environment not created")
        
        # Create destination
        Path(dest_path).mkdir(parents=True, exist_ok=True)
        
        # Copy files from container
        cmd = [
            self.cli_path,
            "cp",
            f"{self.environment_id}:/workspace/.",
            dest_path
        ]
        
        return await self._run_command(cmd)
    
    async def close(self):
        """Keep container for review (don't delete)."""
        # Container-use automatically manages cleanup
        # We keep it so user can review with: git checkout AppFactory-{project_id}
        pass
    
    async def _run_command(self, cmd: List[str]) -> Dict[str, Any]:
        """
        Run shell command and return result.
        
        Returns:
            {"stdout": str, "stderr": str, "exit_code": int}
        """
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await process.communicate()
            
            return {
                "stdout": stdout.decode("utf-8", errors="replace"),
                "stderr": stderr.decode("utf-8", errors="replace"),
                "exit_code": process.returncode
            }
        except FileNotFoundError:
            return {
                "stdout": "",
                "stderr": f"Command not found: {cmd[0]}",
                "exit_code": 127
            }
        except Exception as e:
            return {
                "stdout": "",
                "stderr": str(e),
                "exit_code": 1
            }
