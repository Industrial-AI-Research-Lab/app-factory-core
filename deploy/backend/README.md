# Backend runtime environment

`deploy/backend/.env.template` is the authoritative list of backend runtime
variables managed outside Compose. It contains no real values. The rendering
script copies values from like-named process variables into a private env file;
unset values are written as empty strings so application defaults remain
visible and testable.

Container wiring stays outside this schema. `API_HOST`, `API_PORT`,
`CORS_ALLOW_ORIGINS`, `DOCKER_HOST`, build metadata, and mounted paths belong
to the Compose or Kubernetes definition that knows those values.

## Minimal environment

A usable shared backend needs these non-empty secrets:

| Variable | Purpose |
| --- | --- |
| `MONGODB_URI` | MongoDB connection string including required credentials and transport options. |
| `JWT_SECRET_KEY` | Signs access and refresh tokens. |
| `OPENAI_API_KEY` | Direct OpenAI credential or fallback when the selected LLM path needs it. |

When `USE_BIFROST=true`, also provide `BIFROST_URL` and the secret `BIFROST_VK`.
Root-user bootstrap needs `AppFactory_ROOT_EMAIL` and the secret
`AppFactory_ROOT_PASSWORD`. SMTP, deployment-agent registry, and object-storage
values are required only when those features are enabled.

## Full environment reference

The template groups the complete schema by responsibility:

| Group | Variables |
| --- | --- |
| MongoDB and migrations | `MONGODB_URI`, `MONGODB_DATABASE`, `MONGODB_ENABLE_TRANSACTIONS`, `RUN_MIGRATIONS_ON_START`, `MIGRATIONS_LOCK_WAIT_SECONDS` |
| LLM gateway | `USE_BIFROST`, `BIFROST_URL`, `BIFROST_VK`, `BIFROST_FALLBACK_MODELS`, `OPENAI_API_KEY`, `MAX_PRICE_PER_MILLION` |
| Authentication | `JWT_SECRET_KEY`, `AppFactory_ROOT_EMAIL`, `AppFactory_ROOT_PASSWORD`, `DEPLOY_ADMIN_KEY` |
| Email | `AppFactory_SMTP_HOST`, `AppFactory_SMTP_PORT`, `AppFactory_SMTP_TLS`, `AppFactory_SMTP_USER`, `AppFactory_SMTP_PASS` |
| Deployment agent | `DEPLOY_AGENT_LOCAL_ENABLED`, `DEPLOY_AGENT_PROD_ENABLED`, `DEPLOY_AGENT_APPS_DOMAIN`, `DEPLOY_AGENT_REGISTRY_HOST`, `DEPLOY_AGENT_REGISTRY_USERNAME`, `DEPLOY_AGENT_REGISTRY_PASSWORD` |
| Object storage | `ARCHIVE_S3_ENDPOINT`, `ARCHIVE_S3_REGION`, `ARCHIVE_S3_BUCKET`, `ARCHIVE_S3_ACCESS_KEY`, `ARCHIVE_S3_SECRET_KEY`, `ARCHIVE_S3_ADDRESSING_STYLE`, `FILE_S3_BUCKET` |
| Upload and attachment limits | `FILE_UPLOAD_MAX_FILES`, `FILE_UPLOAD_MAX_BYTES`, `FILE_DOWNLOAD_URL_TTL_SECONDS`, `ATTACHMENT_INLINE_TEXT_MAX_BYTES`, `ATTACHMENT_INLINE_TEXT_TOTAL_MAX_BYTES` |
| Archive limits | `ARCHIVE_SPILL_THRESHOLD_BYTES`, `ARCHIVE_QUERY_TIMEOUT_SECONDS`, `ARCHIVE_QUERY_MAX_SCAN_BYTES`, `ARCHIVE_FETCH_TIMEOUT_SECONDS`, `ARCHIVE_DOWNLOAD_URL_TTL_SECONDS` |
| Container-use | `CONTAINER_USE_ENABLED`, `CONTAINER_USE_CLI_PATH`, `REPOSITORIES_ROOT` |
| Telemetry | `OTEL_ENABLED`, `OTEL_ENDPOINT`, `OTEL_SERVICE_NAME`, `OTEL_TOOL_RESULT_PREVIEW_LIMIT`, `JAEGER_UI_URL` |
| General | `DEBUG`, `SEED_ENABLED`, `WEB_HOST` |

`[S]` in the template marks secrets. `[V]` marks non-secret configuration.
Empty archive credentials disable object storage. Empty numeric limits use the
defaults recorded next to each key.

## Render an env file

```bash
export MONGODB_URI='mongodb://user:password@mongo:27017/synaps'
export JWT_SECRET_KEY='replace-with-a-random-secret'
export OPENAI_API_KEY='replace-with-a-provider-key'
bash deploy/backend/render-backend-env.sh \
  deploy/backend/.env.template \
  /tmp/backend.env
```

The result has mode `600`. Remove it after use if it contains secrets.

## Check Compose ownership

Compose `environment:` values override `env_file`, which would create two
owners for one setting. Before applying a rendered file, pipe the resolved
Compose JSON to the guard:

```bash
docker compose -f /path/to/docker-compose.yml \
  config --no-env-resolution --format json |
bash deploy/backend/check-compose-overrides.sh \
  deploy/backend/.env.template backend
```

The command exits non-zero and names any template key also present in the
service's inline environment. Wiring-only keys are allowed because they do not
appear in the template.

## Load values into GitHub configuration

Preview classification without sending values:

```bash
bash deploy/backend/load-secrets-to-github.sh --source /path/to/env-files --dry-run
```

Run the same command without `--dry-run` only from a protected machine with an
authenticated `gh` session. Secret values travel over standard input and are
not printed. The deployment workflow renders the template, checks the critical
keys, runs the Compose ownership guard, installs the file, and optionally
recreates the backend.

## Regression checks

```bash
bash tests/deploy/test_backend_secrets.sh
```
