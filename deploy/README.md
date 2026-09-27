# Container deployment

`deploy/` contains the versioned container deployment assets needed to run one
AppFactory instance. Each directory that contains a Compose file has its own
README with exact validation, startup, status, log, and shutdown commands plus
the minimal and complete environment-variable sets.

## Core application

- [`AppFactory/`](AppFactory/) runs the application stack: gateway, frontend,
  backend, MongoDB, MinIO, Loki, Promtail, and Grafana.
- [`backend/`](backend/) defines the backend runtime environment contract and
  the scripts that render and validate a secret env file.

## MCP services

- [`oil-mcp/`](oil-mcp/) runs Docling, Domain Research, Reporting, FalkorDB,
  and their supporting proxy and object-store integration.
- [`blocksnet/`](blocksnet/) runs the BlocksNet MCP service.
- [`coscientist/`](coscientist/) extends the upstream CoScientist Compose
  project with AppFactory-specific networking and configuration.
- [`mcp-npx-runner/`](mcp-npx-runner/) builds the reusable Node.js image used
  to launch stdio MCP packages through `npx`.

## Observability

- [`observability/`](observability/) runs an independent Loki, Promtail, and
  Grafana stack when observability is deployed separately from the core stack.
- [`jaeger/`](jaeger/) runs an OTLP-compatible Jaeger collector and UI.

## Choosing components

Start with [`AppFactory/`](AppFactory/). Add only the MCP services needed by the
instance. The core stack already includes Loki, Promtail, and Grafana; use the
independent [`observability/`](observability/) Compose project only when those
services must be managed separately. Jaeger is optional.

GitHub Actions workflows remain under `.github/workflows/`; they consume the
assets documented here.

Compose files and Dockerfiles under `docs/` are runnable examples and MCP
package templates, not services of the AppFactory instance. Experimental stacks
remain with their experiments for the same reason.

## Verification

Deployment contract tests live in [`tests/deploy/`](../tests/deploy/). Before
deploying, run the relevant test script and the `docker compose ... config`
command shown in that component's README.
