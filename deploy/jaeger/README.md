# Inner-cluster Jaeger (node20 lab stand)

A Jaeger **all-in-one** container plus a narrow authenticated OTLP gateway for the local stand.
The backends export over the private compose network; adapters outside that network use the
host-facing gateway. Run **Actions → Deploy Jaeger (inner cluster) → Run workflow**.

This is **not** the external cluster's Jaeger. That one is `.github/workflows/deploy-jaeger.yml`
(kubectl into the K3s cluster, Authelia-protected UI on a public FQDN). This one is docker-compose
on node20, alongside the stand's existing grafana/loki/promtail.

## How it wires up

The container attaches to the lab-local `AppFactory-lab_internal` network (the same one the
`AppFactory-lab-backend-*` containers sit on), so a backend reaches it by service name:

- **OTLP HTTP (in-network):** `http://jaeger:4318/v1/traces` — what the AppFactory backends export
  to. Jaeger's receiver is not published directly on the host.
- **OTLP HTTP (outside compose):** `http://<JAEGER_OTLP_BIND>:<JAEGER_OTLP_PORT>/v1/traces` — a
  Caddy reverse proxy which accepts only authenticated `POST /v1/traces` and forwards it to
  `jaeger:4318`. On this stand the intended URL is
  `http://10.0.15.21:4318/v1/traces`.
- **UI:** published on the host at `<JAEGER_UI_BIND>:16686`. The address comes from the repo
  Variable `JAEGER_UI_BIND` (or the `ui_bind` workflow input); on the stand it is the node LAN IP
  `10.0.15.21`, the same as `GRAFANA_BIND`/`LOKI_BIND`, so from the internal network the UI is
  <http://10.0.15.21:16686>. Without the Variable it falls back to loopback `127.0.0.1`
  (reach it with `ssh -L 16686:127.0.0.1:16686 node20`). The UI has no authentication: keep the
  bind on the internal LAN or loopback, never a public interface.

The raw Jaeger OTLP ports (4317/4318) are deliberately **not** published to the host. Port 4318
belongs to the Caddy gateway, not to Jaeger, so unauthenticated requests cannot bypass the proxy.
The gateway does not expose the UI, query API, metrics, logs, or OTLP gRPC. Storage is
**in-memory** (bounded by `MEMORY_MAX_TRACES`, default 50 000): fine for a lab where you look at
recent traces, but **traces are lost on restart**.

### OTLP gateway credentials and network boundary

Configure these GitHub repository settings before deployment:

| Kind | Name | Value on this stand |
| --- | --- | --- |
| Variable | `JAEGER_OTLP_BIND` | `10.0.15.21` |
| Variable | `JAEGER_OTLP_USERNAME` | `AppFactory-scientist` (or another non-secret username) |
| Secret | `JAEGER_OTLP_PASSWORD` | 24–64 random printable ASCII characters without spaces |

The `otlp_port` workflow input defaults to `4318`; change it explicitly at dispatch time only if
that host port is unavailable.

The workflow hashes the password with Caddy's bcrypt implementation. Only the hash enters the
gateway container; the plaintext remains a GitHub Secret and is used during deployment solely to
run the authenticated acceptance smoke. Give the username to the adapter owner in the integration
instructions and transfer the password through the approved password manager or another secure
secret channel. Never paste the password into Tracker, a PR, logs, or chat.

This stand exposes HTTP on a private cluster/VPN address. HTTP Basic credentials are not safe on
the public Internet because they are only base64-encoded on the wire. Keep `JAEGER_OTLP_BIND` on
the explicit trusted interface (`0.0.0.0` is rejected by the workflow). If the endpoint is ever
made Internet-routable, terminate HTTPS at a trusted ingress before distributing credentials.

An OTLP/HTTP client sends its normal Protobuf or JSON payload with an Authorization header:

```text
POST http://10.0.15.21:4318/v1/traces
Content-Type: application/x-protobuf
Authorization: Basic <base64(username:password)>
```

The workflow proves the boundary on every deploy: the same JSON OTLP span gets `401` without
credentials, succeeds with Basic auth, and is then queried back from Jaeger by its trace ID.

## What makes traces flow (both settings are GitHub-managed since 2026-09-01)

The backends export nothing unless two per-environment GitHub **Variables** (`lab-dev` /
`lab-staging` / `lab-prod`) are set; **Deploy backend secrets** renders them into
`backend.<env>.env` and recreates the container:

1. **`OTEL_ENABLED=true`.** The stand's `lab-local/docker/compose.yaml` no longer overrides it
   inline (those lines were removed on 2026-09-01), so the env file's value wins.
2. **`OTEL_ENDPOINT=http://jaeger:4318/v1/traces`.** `localhost` is wrong under compose: it would
   be the backend container itself.

`src/telemetry/tracer.py` degrades gracefully: with `OTEL_ENABLED=true` but Jaeger down, the
batch exporter logs and drops spans in the background — application work does not depend on the
collector being reachable. Initialization and span-instrumentation failures also fall back to a
no-op tracer.
Each environment sets its own `OTEL_SERVICE_NAME` (`AppFactory-backend-<env>`), so one Jaeger
separates dev/staging/prod by `service.name` in the UI.

Spans are created only inside orchestration (`telemetry.tracer.start_span` in the orchestrator,
agents, LLM client, MCP executor and auction); HTTP requests are not instrumented. An idle stand
therefore shows no backend service in the UI until a project runs.

## Rollout order

1. Set the gateway repository settings above, then run **Deploy Jaeger (inner cluster)**. Confirm
   both the unauthenticated/authenticated OTLP smoke and the Jaeger query check are green.
2. Check the backend Variables above in all three Environments (set on 2026-09-01). If you change one,
   re-run **Deploy backend secrets** with `recreate` so the backend picks it up.
3. Exercise a project, then look for its `service.name` in the Jaeger UI.

## Inputs

`jaeger_version` (default `1.76.0`), `caddy_version` (default `2.11.3-alpine`), `ui_bind` (default
empty = repo Variable `JAEGER_UI_BIND`, else `127.0.0.1`), `ui_port` (default `16686`),
`otlp_bind` (default empty = `JAEGER_OTLP_BIND`, then `JAEGER_UI_BIND`, then loopback), `otlp_port`
(default `4318`), `memory_max_traces` (default `50000`). Image tags are pinned; bump them
deliberately rather than tracking latest.

## Minimal environment

Set `JAEGER_OTLP_USERNAME` and `JAEGER_OTLP_PASSWORD_HASH`. The hash must be a
Caddy-compatible password hash, not plaintext. The external Docker network
`AppFactory-lab_internal` and external volume `jaeger-caddy-config` must exist.

```bash
docker network inspect AppFactory-lab_internal
docker volume create jaeger-caddy-config
```

Stage `deploy/jaeger/Caddyfile` into the volume as `/etc/caddy/Caddyfile`
before the first start.

## Full environment reference

| Variable | Default | Purpose |
| --- | --- | --- |
| `JAEGER_OTLP_USERNAME` | required | Basic-auth username accepted by the OTLP gateway. |
| `JAEGER_OTLP_PASSWORD_HASH` | required | Caddy password hash; secret. |
| `JAEGER_VERSION` | `1.76.0` | Jaeger all-in-one image tag. |
| `CADDY_VERSION` | `2.11.3-alpine` | OTLP gateway image tag. |
| `JAEGER_MEMORY_MAX_TRACES` | `50000` | In-memory trace cap. |
| `JAEGER_UI_BIND` | `127.0.0.1` | Host address for the UI. |
| `JAEGER_UI_PORT` | `16686` | UI host port. |
| `JAEGER_OTLP_BIND` | `127.0.0.1` | Host address for authenticated OTLP HTTP. |
| `JAEGER_OTLP_PORT` | `4318` | Authenticated OTLP HTTP port. |
| `JAEGER_MEM_LIMIT` | `1g` | Jaeger memory limit. |
| `JAEGER_CPUS` | `1` | Jaeger CPU limit. |
| `JAEGER_OTLP_GATEWAY_MEM_LIMIT` | `128m` | Gateway memory limit. |
| `JAEGER_OTLP_GATEWAY_CPUS` | `0.25` | Gateway CPU limit. |

## Validate the configuration

```bash
JAEGER_OTLP_USERNAME=test \
JAEGER_OTLP_PASSWORD_HASH='$2a$14$test-placeholder' \
docker compose -f deploy/jaeger/docker-compose.yml config --quiet
```

## Start

```bash
docker create --name jaeger-caddy-config-stage -v jaeger-caddy-config:/config alpine:3
docker cp deploy/jaeger/Caddyfile jaeger-caddy-config-stage:/config/Caddyfile
docker rm jaeger-caddy-config-stage
docker compose --project-name jaeger -f deploy/jaeger/docker-compose.yml up -d --wait
```

## Status and logs

```bash
docker compose --project-name jaeger -f deploy/jaeger/docker-compose.yml ps
docker compose --project-name jaeger -f deploy/jaeger/docker-compose.yml logs -f jaeger otlp-gateway
```

## Stop

```bash
docker compose --project-name jaeger -f deploy/jaeger/docker-compose.yml down
```

The external Caddy configuration volume is retained.
