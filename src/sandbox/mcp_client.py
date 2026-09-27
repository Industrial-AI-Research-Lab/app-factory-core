"""
MCP Client for Container-Use

Communicates with container-use MCP server via JSON-RPC 2.0 over stdio.
"""

import asyncio
import json
import uuid
from typing import Dict, Any, Optional, List
import logging

logger = logging.getLogger(__name__)


class MCPClient:
    """
    Client for communicating with container-use MCP server.
    
    Spawns 'cu stdio' subprocess and communicates via JSON-RPC 2.0.
    """
    
    def __init__(self, project_id: str, base_image: str = "python:3.11-slim"):
        """
        Initialize MCP client.
        
        Args:
            project_id: Unique project identifier
            base_image: Docker base image to use
        """
        self.project_id = project_id
        self.base_image = base_image
        self.process = None
        self.reader_task = None
        self.request_id = 0
        self.pending_requests = {}  # request_id -> Future
        self.available_tools = {}  # tool_name -> tool_info
        
    async def start_server(self, cli_path: str = "cu"):
        """
        Start container-use MCP server subprocess and initialize.
        
        Args:
            cli_path: Path to container-use CLI (cu or container-use)
        """
        try:
            # Spawn container-use stdio process
            self.process = await asyncio.create_subprocess_exec(
                cli_path,
                "stdio",
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            
            # Start reading responses
            self.reader_task = asyncio.create_task(self._read_responses())
            
            logger.info(f"Started container-use MCP server for project {self.project_id}")
            
            # Perform MCP initialization handshake
            await self._initialize_protocol()
            
            # List available tools
            await self._list_tools()
            
            # Create environment for this project
            await self._create_environment()
            
        except Exception as e:
            logger.error(f"Failed to start container-use: {e}")
            raise RuntimeError(f"Could not start container-use MCP server: {e}")
    
    async def _initialize_protocol(self):
        """Perform MCP initialization handshake."""
        try:
            # Send initialize request
            result = await self._send_request("initialize", {
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "roots": {"listChanged": True}
                },
                "clientInfo": {
                    "name": "AppFactory",
                    "version": "0.1.0"
                }
            })
            
            logger.info(f"MCP initialized: {result.get('serverInfo', {}).get('name', 'unknown')}")
            
            # Send initialized notification
            await self._send_notification("notifications/initialized", {})
            
        except Exception as e:
            logger.error(f"Failed to initialize MCP: {e}")
            raise
    
    async def _list_tools(self):
        """List available tools from MCP server."""
        try:
            result = await self._send_request("tools/list", {})
            tools = result.get("tools", [])
            
            self.available_tools = {tool["name"]: tool for tool in tools}
            logger.info(f"Discovered {len(tools)} MCP tools")
            
            for tool in tools[:5]:  # Log first 5 tools
                logger.debug(f"  - {tool.get('name')}")
            
        except Exception as e:
            logger.warning(f"Failed to list tools: {e}")
            self.available_tools = {}
    
    async def _create_environment(self):
        """Create container environment for this project."""
        try:
            # Find the environment_create tool
            create_tool = None
            for name in self.available_tools:
                if "environment_create" in name:
                    create_tool = name
                    break
            
            if not create_tool:
                raise RuntimeError("environment_create tool not found")
            
            await self._send_request("tools/call", {
                "name": create_tool,
                "arguments": {
                    "image": self.base_image
                }
            })
            logger.info(f"Created environment for project {self.project_id}")
        except Exception as e:
            logger.warning(f"Failed to create environment: {e}")
    
    async def _read_responses(self):
        """Background task to read JSON-RPC responses from stdout."""
        if not self.process or not self.process.stdout:
            return
        
        try:
            while True:
                line = await self.process.stdout.readline()
                if not line:
                    break
                
                try:
                    response = json.loads(line.decode())
                    request_id = response.get("id")
                    
                    if request_id in self.pending_requests:
                        future = self.pending_requests.pop(request_id)
                        
                        if "error" in response:
                            future.set_exception(
                                Exception(response["error"].get("message", "Unknown error"))
                            )
                        else:
                            future.set_result(response.get("result"))
                            
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON response: {line}")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error reading MCP responses: {e}")
    
    async def _send_request(self, method: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Send JSON-RPC request and wait for response.
        
        Args:
            method: RPC method name
            params: Method parameters
            
        Returns:
            Response result
        """
        if not self.process or not self.process.stdin:
            raise RuntimeError("MCP server not started")
        
        self.request_id += 1
        request = {
            "jsonrpc": "2.0",
            "id": self.request_id,
            "method": method,
            "params": params
        }
        
        # Create future for response
        future = asyncio.Future()
        self.pending_requests[self.request_id] = future
        
        # Send request
        request_json = json.dumps(request) + "\n"
        self.process.stdin.write(request_json.encode())
        await self.process.stdin.drain()
        
        logger.debug(f"Sent MCP request: {method}")
        
        # Wait for response with timeout
        try:
            result = await asyncio.wait_for(future, timeout=30.0)
            return result
        except asyncio.TimeoutError:
            self.pending_requests.pop(self.request_id, None)
            # TimeoutError (not RuntimeError) so mcp_executor tags error_type=timeout
            # and the terminal gate treats this as infra, not domain-fail.
            raise TimeoutError(f"MCP request timeout: {method}")
    
    async def _send_notification(self, method: str, params: Dict[str, Any]):
        """
        Send JSON-RPC notification (no response expected).
        
        Args:
            method: RPC method name
            params: Method parameters
        """
        if not self.process or not self.process.stdin:
            raise RuntimeError("MCP server not started")
        
        notification = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params
        }
        
        # Send notification
        notification_json = json.dumps(notification) + "\n"
        self.process.stdin.write(notification_json.encode())
        await self.process.stdin.drain()
        
        logger.debug(f"Sent MCP notification: {method}")
    
    async def execute_command(
        self, 
        command: str, 
        cwd: str = "/workspace"
    ) -> Dict[str, Any]:
        """
        Execute shell command in container.
        
        Args:
            command: Shell command to execute
            cwd: Working directory
            
        Returns:
            {"stdout": str, "stderr": str, "exit_code": int}
        """
        run_tool = self._find_tool("run_cmd")
        if not run_tool:
            raise RuntimeError("run_cmd tool not available")
        
        result = await self._send_request("tools/call", {
            "name": run_tool,
            "arguments": {
                "command": command
            }
        })
        
        return result
    
    async def write_file(self, path: str, content: str) -> Dict[str, Any]:
        """
        Create or update file in container.
        
        Args:
            path: File path (relative to /workspace)
            content: File content
            
        Returns:
            {"status": "success", "path": str}
        """
        # Find the write tool
        write_tool = self._find_tool("file_write")
        if not write_tool:
            raise RuntimeError("file_write tool not available")
        
        result = await self._send_request("tools/call", {
            "name": write_tool,
            "arguments": {
                "path": path,
                "content": content
            }
        })
        
        return result
    
    def _find_tool(self, keyword: str) -> str:
        """Find tool name containing keyword."""
        for name in self.available_tools:
            if keyword in name.lower():
                return name
        return None
    
    async def read_file(self, path: str) -> str:
        """
        Read file from container.
        
        Args:
            path: File path (relative to /workspace)
            
        Returns:
            File content as string
        """
        read_tool = self._find_tool("file_read")
        if not read_tool:
            raise RuntimeError("file_read tool not available")
        
        result = await self._send_request("tools/call", {
            "name": read_tool,
            "arguments": {
                "path": path
            }
        })
        
        return result.get("content", "")
    
    async def list_files(self, path: str = "/workspace") -> List[str]:
        """
        List files in container directory.
        
        Args:
            path: Directory path
            
        Returns:
            List of file paths
        """
        list_tool = self._find_tool("file_list")
        if not list_tool:
            raise RuntimeError("file_list tool not available")
        
        result = await self._send_request("tools/call", {
            "name": list_tool,
            "arguments": {
                "path": path
            }
        })
        
        return result.get("files", [])
    
    async def close(self):
        """Shutdown MCP server connection."""
        if self.reader_task:
            self.reader_task.cancel()
            try:
                await self.reader_task
            except asyncio.CancelledError:
                pass
        
        if self.process:
            self.process.terminate()
            try:
                await asyncio.wait_for(self.process.wait(), timeout=5)
            except asyncio.TimeoutError:
                self.process.kill()
                await self.process.wait()
        
        logger.info(f"Closed MCP connection for project {self.project_id}")
