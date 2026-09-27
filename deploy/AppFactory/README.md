# AppFactory container stack

This Compose project runs one AppFactory instance: gateway, frontend, backend,
MongoDB, MinIO, Loki, Promtail, and Grafana. Run every command from the
repository root.

## Minimal environment

Copy the tracked example and choose one LLM connection:

```powershell
Copy-Item deploy/AppFactory/.env.example deploy/AppFactory/.env
```

For direct OpenAI access, set `OPENAI_API_KEY` and set `USE_BIFROST=false`.
For Bifrost, keep `USE_BIFROST=true` and set `BIFROST_URL` and `BIFROST_VK`.
The remaining values have defaults suitable for a single workstation. Replace
`MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `GRAFANA_ADMIN_USER`, and
`GRAFANA_ADMIN_PASSWORD` before exposing their ports outside loopback.

## Full environment reference

Backend runtime settings are documented in
[`../backend/.env.template`](../backend/.env.template). The Compose file also
interpolates these container and host settings:

| Variable | Default | Purpose |
| --- | --- | --- |
| `API_PORT` | `8000` | Backend port published on the host. |
| `FRONTEND_PORT` | `5173` | Gateway port and browser origin. |
| `MONGODB_PORT` | `27017` | MongoDB port published on the host. |
| `MONGODB_DATABASE` | `synaps` | Database created and used by the backend. |
| `CONTAINER_USE_ENABLED` | `true` | Enables container-use in the backend. |
| `MINIO_ROOT_USER` | `minioadmin` | MinIO administrator name; treat as a secret outside an isolated workstation. |
| `MINIO_ROOT_PASSWORD` | `minioadmin` | MinIO administrator password; secret. |
| `MINIO_BIND` | `127.0.0.1` | Host address for MinIO ports. |
| `MINIO_PORT` | `9000` | MinIO API port. |
| `MINIO_CONSOLE_PORT` | `9001` | MinIO console port. |
| `MINIO_VERSION` | pinned release | MinIO image tag. |
| `MINIO_MC_VERSION` | pinned release | MinIO client image tag used to create buckets. |
| `ARCHIVE_S3_REGION` | `us-east-1` | Region supplied to the backend S3 client. |
| `ARCHIVE_S3_BUCKET` | `AppFactory-archives` | Tool-result archive bucket created at startup. |
| `FILE_S3_BUCKET` | `AppFactory-files` | Attachment bucket created at startup. |
| `LOKI_BIND` | `127.0.0.1` | Host address for Loki. |
| `LOKI_PORT` | `3100` | Loki port. |
| `LOKI_VERSION` | `2.9.8` | Loki and Promtail-compatible image line. |
| `PROMTAIL_VERSION` | `2.9.8` | Promtail image tag. |
| `GRAFANA_ADMIN_USER` | `admin` | Grafana administrator name. |
| `GRAFANA_ADMIN_PASSWORD` | `admin` | Grafana administrator password; secret. |
| `GRAFANA_BIND` | `127.0.0.1` | Host address for Grafana. |
| `GRAFANA_PORT` | `3000` | Grafana port and default root URL. |
| `GRAFANA_VERSION` | `11.2.0` | Grafana image tag. |
| `NGINX_VERSION` | `1.27-alpine` | Gateway image tag. |

The backend also receives `API_HOST=0.0.0.0`, the internal MongoDB and MinIO
addresses, the Docker socket path, and durable repository/state paths directly
from Compose because those values describe container wiring.

## Validate the configuration

```powershell
docker compose --env-file deploy/AppFactory/.env -f deploy/AppFactory/docker-compose.yml config --quiet
```

Validation renders paths and variables without starting or pulling containers.

## Start

```powershell
docker compose --env-file deploy/AppFactory/.env -f deploy/AppFactory/docker-compose.yml up -d --build
```

Default endpoints are the application at `http://localhost:5173`, API at
`http://localhost:8000`, MinIO at `http://localhost:9000`, its console at
`http://localhost:9001`, and Grafana at `http://localhost:3000`.

## Status and logs

```powershell
docker compose --env-file deploy/AppFactory/.env -f deploy/AppFactory/docker-compose.yml ps
docker compose --env-file deploy/AppFactory/.env -f deploy/AppFactory/docker-compose.yml logs -f backend
```

The backend image includes the Docker CLI and container-use. The host Docker
socket is mounted so agent code still executes in managed containers rather
than in the backend container itself.

## Stop

```powershell
docker compose --env-file deploy/AppFactory/.env -f deploy/AppFactory/docker-compose.yml down
```

Named volumes are retained. Add `--volumes` only when the MongoDB, MinIO,
Grafana, Loki, repository, and backend state are intentionally disposable.
