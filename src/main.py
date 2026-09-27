"""
Main entry point for AppFactory

Runs the FastAPI server.
"""

import uvicorn
import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv(dotenv_path=Path(__file__).resolve().with_name(".env"))


def main():
    """Start the AppFactory API server"""
    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "8000"))
    debug = os.getenv("DEBUG", "true").lower() == "true"
    # Minimal logging config so module logs show in console
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    for name in (
        "src.orchestration.orchestrator",
        "src.orchestration.auction",
        "src.agents.base",
        "src.tools.mcp_executor",
    ):
        logging.getLogger(name).setLevel(logging.INFO)
    
    print("""
╔═══════════════════════════════════════════════════════════╗
║                                                           ║
║   ███████╗██╗   ██╗███╗   ██╗ █████╗ ██████╗ ███████╗   ║
║   ██╔════╝╚██╗ ██╔╝████╗  ██║██╔══██╗██╔══██╗██╔════╝   ║
║   ███████╗ ╚████╔╝ ██╔██╗ ██║███████║██████╔╝███████╗   ║
║   ╚════██║  ╚██╔╝  ██║╚██╗██║██╔══██║██╔═══╝ ╚════██║   ║
║   ███████║   ██║   ██║ ╚████║██║  ██║██║     ███████║   ║
║   ╚══════╝   ╚═╝   ╚═╝  ╚═══╝╚═╝  ╚═╝╚═╝     ╚══════╝   ║
║                                                           ║
║         Multi-Agent Orchestration System v0.1.0          ║
║                                                           ║
╚═══════════════════════════════════════════════════════════╝
    """)
    
    print(f"🌐 Starting server on http://{host}:{port}")
    print(f"🔍 Debug mode: {debug}")
    print(f"📚 API docs: http://{host}:{port}/docs")
    print(f"🔄 Alternative docs: http://{host}:{port}/redoc")
    print()
    
    uvicorn.run(
        "api.main:app",
        host=host,
        port=port,
        reload=debug,
        log_level="info" if debug else "warning"
    )


if __name__ == "__main__":
    main()
