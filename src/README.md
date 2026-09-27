# AppFactory — Backend (`src/`)

> **Full project documentation:** see [**README.md**](../README.md) in the repo root.

## Quick Start

```powershell
# Backend
python -m venv venv && .\venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy ..\deploy\AppFactory\.env.example .env   # set OPENAI_API_KEY + MONGODB_URI
python main.py           # → http://localhost:8000/docs

# Frontend
cd ui && npm install && npm run dev   # → http://localhost:5173

# CLI (no UI)
python -m cli "Create a simple hello world script"
```

## Key Directories

| Directory | Purpose |
|-----------|---------|
| `agents/` | GenericAgent + BaseAgent (all agents are GenericAgent instances) |
| `orchestration/` | Workflow engine, auction, phase runners, intent classifier |
| `context/` | SharedContext (per-project state) |
| `api/` | FastAPI app, routes, auth, intent router |
| `config/` | Seed files: `agents.yaml`, `workflows.json`, `tools.yaml` |
| `storage/` | MongoDB CRUD (`mongo_backend.py`) |
| `llm/` | LLM client (Bifrost / direct OpenAI) |
| `sandbox/` | Container-Use manager |
| `deploy/` | Deploy to K8s |
| `ui/` | React frontend (Vite + TailwindCSS) |
| `tests/` | pytest tests |

## Documentation

All detailed docs in `skills/` (relative to repo root):

| Doc | Covers |
|-----|--------|
| [system-deep-dive.md](../skills/system-deep-dive.md) | Architecture, boot sequence, GenericAgent, API layer |
| [system-deep-dive-part2.md](../skills/system-deep-dive-part2.md) | Orchestration, SharedContext, MongoDB schema |
| [getting-started.md](../skills/getting-started.md) | Full setup guide, env vars |
| [bifrost.md](../skills/bifrost.md) | LLM gateway |
| [mongodb.md](../skills/mongodb.md) | Collections, indexes |
| [container-use.md](../skills/container-use.md) | Sandbox |
| [infrastructure.md](../skills/infrastructure.md) | K8s, observability (Prom/Jaeger/Grafana on logging host), CI/CD |
| [fastapi-auth-multitenancy.md](../skills/fastapi-auth-multitenancy.md) | Auth, RBAC, tenants |
| [agent-delegation.md](../docs/agent-delegation.md) | Делегация через `delegate_to_agent`: runtime flow, policy и events |
| [agent-context-contracts.md](../docs/agent-context-contracts.md) | Контракты `reads`/`writes` на workflow phase nodes |
| [agent-context-contracts-review.md](../docs/agent-context-contracts-review.md) | Reviewer checklist для context contracts |

## License

MIT
