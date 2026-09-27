"""
CLI for testing AppFactory without UI

Quick way to test the orchestration system.
"""

import asyncio
import sys
from dotenv import load_dotenv

from storage.mongo_backend import MongoStorageBackend
from llm.client import LLMClient
from tools.tool_registry import ToolRegistry
from tools.mcp_executor import MCPToolExecutor
from events.emitter import EventEmitter
from orchestration.orchestrator import Orchestrator
from config.agent_loader import load_agents_from_db
from sandbox.container_manager import ContainerManager
from config.seed import seed_run_configurations
import os


async def main(user_prompt: str):
    """
    Run a project through the orchestration system.
    
    Args:
        user_prompt: User's project description
    """
    print("🚀 Initializing AppFactory...")
    
    # Load environment
    load_dotenv()
    
    # Initialize components (MongoDB only)
    mongodb_uri = os.getenv("MONGODB_URI") or "mongodb://localhost:27017"
    storage = MongoStorageBackend(
        connection_string=mongodb_uri,
        database=os.getenv("MONGODB_DATABASE", "AppFactory"),
        enable_transactions=os.getenv("MONGODB_ENABLE_TRANSACTIONS", "true").lower() == "true"
    )
    await storage.initialize()
    await seed_run_configurations(storage)

    llm_client = LLMClient(model_config_storage=storage)
    
    tool_registry = ToolRegistry()
    await tool_registry.load_from_db(storage)
    
    event_emitter = EventEmitter(storage_backend=storage)
    
    # Register event callbacks for console output
    def print_event(event):
        print(f"📢 [{event['type']}] {event['data']}")
    
    def print_container_event(event):
        data = event.get("data", {})
        env_id = data.get("environment_id")
        print(f"🐳 [container_created] environment_id={env_id}")
        if env_id:
            print(f"   Tip: container-use log {env_id}")
    
    def print_tool_event(event):
        data = event.get("data", {})
        tool_id = data.get("tool_id")
        result = data.get("result", {}) or {}
        status = result.get("status")
        exit_code = result.get("exit_code")
        print(f"🛠️ [tool_executed] tool_id={tool_id} status={status if status is not None else exit_code}")
        # Truncate noisy output
        stdout = result.get("stdout")
        stderr = result.get("stderr")
        if stdout:
            print("   stdout:", (stdout[:300] + ("..." if len(stdout) > 300 else "")))
        if stderr:
            print("   stderr:", (stderr[:300] + ("..." if len(stderr) > 300 else "")))
    
    event_emitter.register_callback("project_started", print_event)
    event_emitter.register_callback("auction.auction_completed", print_event)
    event_emitter.register_callback("phase.requirements.started", print_event)
    event_emitter.register_callback("phase.planning.started", print_event)
    event_emitter.register_callback("phase.execution.started", print_event)
    event_emitter.register_callback("approval_requested", print_event)
    event_emitter.register_callback("container_created", print_container_event)
    event_emitter.register_callback("tool_executed", print_tool_event)
    
    # Initialize container manager
    container_enabled = os.getenv("CONTAINER_USE_ENABLED", "true").lower() == "true"
    container_cli_path = os.getenv("CONTAINER_USE_CLI_PATH", "cu")
    repositories_root = os.getenv("REPOSITORIES_ROOT", r"C:\\work\\repositories")
    container_manager = ContainerManager(
        cli_path=container_cli_path,
        enabled=container_enabled,
        repositories_root=repositories_root,
    )
    
    # Initialize MCP executor
    mcp_executor = MCPToolExecutor(container_manager, storage=storage)
    
    print(f"🐳 Container execution: {'ENABLED' if container_enabled else 'DISABLED (simulated)'}")
    
    # Create orchestrator
    orchestrator = Orchestrator(
        storage_backend=storage,
        llm_client=llm_client,
        tool_registry=tool_registry,
        event_emitter=event_emitter,
        container_manager=container_manager,
        mcp_executor=mcp_executor,
    )
    
    # Register agents with MCP executor injection
    print("🤖 Registering agents...")
    agents = await load_agents_from_db(storage)
    
    for agent in agents:
        orchestrator.register_agent(agent)
        # Inject MCP executor
        agent.mcp_executor = mcp_executor
    
    print(f"✅ Registered {len(orchestrator.agent_pool)} agents")
    print(f"✅ Loaded {tool_registry.get_stats()['total_tools']} tools")
    
    # Show agents and their available tools
    print("\n🤖 Agent Overview:")
    for agent in agents:
        agent_type = agent.agent_type.value if hasattr(agent.agent_type, 'value') else str(agent.agent_type)
        print(f"  • {agent.agent_id} ({agent_type})")
        
        allowed = getattr(agent, "_effective_allowed_tools", None) or getattr(agent, "allowed_tools", None) or []
        if allowed:
            allow = set(allowed)
            available_tools = [t for t in tool_registry.tools if t["tool_id"] in allow]
        else:
            available_tools = []
        if available_tools:
            tool_names = [t["name"] for t in available_tools[:5]]  # Show first 5
            more = f" +{len(available_tools) - 5} more" if len(available_tools) > 5 else ""
            print(f"    Tools: {', '.join(tool_names)}{more}")
    print()
    
    # Start project
    print(f"📝 User prompt: {user_prompt}")
    print()
    
    project_id = await orchestrator.start_project(user_prompt)
    print(f"🆔 Project ID: {project_id}")
    print()
    # Echo container environment ID for convenience
    try:
        status = await container_manager.get_container_status(project_id)
        env_id = status.get("environment_id") if isinstance(status, dict) else None
        repo_path = status.get("repo_path") if isinstance(status, dict) else None
        if env_id:
            print(f"🐳 Environment ID: {env_id}")
            print(f"   View logs: container-use log {env_id}")
        if repo_path:
            print(f"📁 Repo Path: {repo_path}")
    except Exception:
        pass
    
    # Auto-approve all gates for CLI testing
    async def auto_approve():
        """Automatically approve all gates after short delay"""
        await asyncio.sleep(5)  # Wait for first gate
        
        for approval_type in ["requirements", "plan", "output"]:
            approval_id = f"{project_id}_{approval_type}"
            
            # Wait for approval to be pending
            while approval_id not in orchestrator.pending_approvals:
                await asyncio.sleep(0.5)
            
            print(f"\n✅ Auto-approving {approval_type} gate...")
            await orchestrator.approve(approval_id, "Auto-approved for CLI test")
            await asyncio.sleep(2)
    
    # Run workflow and auto-approver in parallel
    try:
        await asyncio.gather(
            orchestrator.run_workflow(project_id),
            auto_approve()
        )
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Show results
    print("\n" + "="*60)
    print("📊 PROJECT COMPLETE")
    print("="*60)
    
    project = orchestrator.active_projects.get(project_id)
    if project:
        shared_context = project["shared_context"]
        
        print("\n📋 Requirements:")
        requirements = shared_context.get("requirements", {})
        print(f"  Goal: {requirements.get('goal', 'N/A')}")
        print(f"  Project Type: {requirements.get('project_type', 'N/A')}")
        
        print("\n📝 Plan:")
        plan = shared_context.get("plan", {})
        tasks = plan.get("tasks", [])
        print(f"  Total Tasks: {len(tasks)}")
        for task in tasks:
            print(f"    - {task.get('description', 'No description')}")
        
        print("\n📦 Artifacts:")
        artifacts = shared_context.get("artifacts", [])
        print(f"  Total Files: {len(artifacts)}")
        for artifact in artifacts:
            print(f"    - {artifact.get('path', 'unknown')}")
        
        # Auto-export artifacts to disk
        if artifacts:
            from pathlib import Path
            output_dir = Path("./artifacts") / project_id
            output_dir.mkdir(parents=True, exist_ok=True)
            
            print(f"\n📁 Exporting artifacts to: {output_dir.absolute()}")
            for artifact in artifacts:
                path = artifact.get('path', 'unknown.txt')
                content = artifact.get('content', '')
                
                file_path = output_dir / path
                file_path.parent.mkdir(parents=True, exist_ok=True)
                
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                
                print(f"  ✅ {path}")
            
            print(f"\n💾 Files saved to: {output_dir.absolute()}")
    
    # Cleanup
    if container_manager:
        await container_manager.close_all(keep_for_review=True)
    await storage.close()
    print("\n✅ Done!")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m cli \"Your project description\"")
        print()
        print("Example:")
        print('  python -m cli "Create a simple REST API with FastAPI"')
        sys.exit(1)
    
    user_prompt = " ".join(sys.argv[1:])
    asyncio.run(main(user_prompt))
