# Inner-cluster observability (node20 lab stand)

Loki + Promtail + Grafana for the local stand, from committed config. Run **Actions → Deploy
Observability (inner cluster) → Run workflow**.

Replaces the three services that currently exist only in the unversioned lab-local `AppFactory-lab`
compose. Same images (`grafana/loki:2.9.8`, `grafana/promtail:2.9.8`, `grafana/grafana:11.2.0`),
same provisioning, same published ports — the stand behaves identically, but the config now lives
in git and can be rebuilt by re-running the workflow.

## How it works

- **Promtail** discovers every container over the docker socket and tails its json log file from
  `/var/lib/docker/containers`, then pushes to **Loki** at `http://loki:3100/loki/api/v1/push`.
  Backends never talk to Loki directly — they just write to stdout.
- **Grafana** reads Loki as its (default, provisioned) datasource and ships one provisioned
  dashboard, **AppFactory Lab — Docker Logs** (`uid: AppFactory-lab-logs`): a log panel and a
  log-volume-by-service panel, filtered to `compose_project="AppFactory-lab"` with `service` /
  `container` / free-text `search` template variables.

The three keep their compose **service names** (`loki`/`promtail`/`grafana`) so in-stack DNS keeps
resolving, and attach to the shared external `AppFactory-lab_internal` network like the other
`deploy/` components. Promtail relabels by each *scraped* container's own compose project, so
moving these services into their own project does **not** change the dashboard's
`compose_project="AppFactory-lab"` view of backend logs.

## How the config reaches the container

Promtail's `config.yml` and Grafana's `provisioning/` are **not bind-mounted from the checkout**.
The node20 runner is a sibling container that shares no `_work` directory with the host, so the
host Docker daemon can't see files in our checkout: a `./promtail/config.yml` bind mount resolves
to a path the daemon auto-creates as an empty directory, and promtail's file-mount then dies with
`not a directory ... mount a directory onto a file`. (A directory mount like Grafana's provisioning
fails more quietly — it just comes up empty.)

Instead the deploy workflow stages both into external named volumes (`obs-promtail-config`,
`obs-grafana-provisioning`) with `docker cp`, which streams content through the daemon API and so
works across the sibling boundary. Each run wipes and refills them, so editing the committed config
and re-running the workflow fully replaces it.

## GitHub Secret / Variables

Set once on the repo (the workflow reads them):

| Kind | Name | Value on this stand | Notes |
| --- | --- | --- | --- |
| **Secret** | `GRAFANA_ADMIN_PASSWORD` | *(the current admin password)* | required — the deploy fails loudly without it |
| Variable | `GRAFANA_ADMIN_USER` | `admin` | optional, defaults to `admin` |
| Variable | `GRAFANA_ROOT_URL` | `http://10.0.15.21:3000` | must match how Grafana is reached, or share/deeplink URLs come out wrong |

Binds default to the repo Variables `GRAFANA_BIND` / `LOKI_BIND` (both `10.0.15.21`, the node
LAN IP — that is how the stand has always published Grafana/Loki, and what `lab-local/scripts/status.sh`
and the grafana MCP expect), so a plain dispatch reproduces external access. The `grafana_bind` /
`loki_bind` inputs override per run; `127.0.0.1` (loopback, SSH-tunnel only) is used only when
neither an input nor the Variable is set.

## Rollout order — the lab-local cutover is required

Loki/Promtail/Grafana are **still defined in lab-local `compose.yaml`**. If you deploy this and
leave them there, the next `docker compose up` of the `AppFactory-lab` project will recreate
duplicates and collide on the names, ports (3000/3100), and the `loki` network alias. So:

1. Run **Deploy Observability**. It removes the lab-local-owned `AppFactory-lab-{loki,promtail,grafana}-1`
   containers, then brings the stack up in the `observability` project. Confirm the verify step is
   green.
2. **Comment the `loki`, `promtail`, and `grafana` service blocks out of lab-local
   `compose.yaml`** (back the file up first). This is the one manual step on the live stand; it
   stops the `AppFactory-lab` project from resurrecting them.
3. Reach the UI: browser to `http://<grafana_bind>:<grafana_port>`, or
   `ssh -L 3000:127.0.0.1:3000 node20` when bound to loopback.

## Data

The `observability` project uses its own named volumes for `loki-data` and `promtail-positions`;
`grafana-data` reuses the stand's `AppFactory-lab_grafana-data` (see below). The old
`AppFactory-lab_loki-data` volume is **left intact** (orphaned, not deleted — reversible).
Consequences of the first cutover:

- **Loki history restarts** from empty. In a lab where you read recent logs this is fine and
  re-accumulates within minutes; if you need the old history preserved, point the compose at the
  existing volume as `external` instead.
- **Grafana state is reused, not reset:** `grafana-data` is declared `external` with the stand's
  pre-existing volume name (`AppFactory-lab_grafana-data`), so service-account tokens (the grafana
  MCP authenticates with one), users and preferences survive. Dashboards and the datasource are
  provisioned from git regardless. The workflow `docker volume create`s the name first, so a
  brand-new node still bootstraps with an empty DB. (The first cutover used a fresh volume and
  silently invalidated the MCP's service-account token — that is why the volume is external now.)

## Inputs

`grafana_bind` and `loki_bind` (empty by default → repo Variables `GRAFANA_BIND` / `LOKI_BIND`,
set to the node LAN IP `10.0.15.21` so a plain dispatch reproduces the stand; `127.0.0.1` only if
neither is set), `grafana_port` (`3000`), `loki_port` (`3100`). Image tags are pinned in the
compose; bump them there deliberately rather than tracking latest.

## Minimal environment

Set the secret `GRAFANA_ADMIN_PASSWORD`. The external network
`AppFactory-lab_internal` and the external volumes `AppFactory-lab_grafana-data`,
`obs-promtail-config`, and `obs-grafana-provisioning` must exist. Stage
`promtail/config.yml` and the Grafana provisioning tree into the two
configuration volumes before starting.

## Full environment reference

| Variable | Default | Purpose |
| --- | --- | --- |
| `GRAFANA_ADMIN_PASSWORD` | required | Grafana administrator password; secret. |
| `GRAFANA_ADMIN_USER` | `admin` | Grafana administrator name. |
| `GRAFANA_BIND` | `127.0.0.1` | Grafana host bind address. |
| `GRAFANA_PORT` | `3000` | Grafana host port. |
| `GRAFANA_ROOT_URL` | `http://localhost:3000` | Public URL used in links. |
| `GRAFANA_VERSION` | `11.2.0` | Grafana image tag. |
| `GRAFANA_MEM_LIMIT` | `512m` | Grafana memory limit. |
| `GRAFANA_CPUS` | `0.5` | Grafana CPU limit. |
| `LOKI_BIND` | `127.0.0.1` | Loki host bind address. |
| `LOKI_PORT` | `3100` | Loki host port. |
| `LOKI_VERSION` | `2.9.8` | Loki image tag. |
| `LOKI_MEM_LIMIT` | `1g` | Loki memory limit. |
| `LOKI_CPUS` | `1` | Loki CPU limit. |
| `PROMTAIL_VERSION` | `2.9.8` | Promtail image tag. |
| `PROMTAIL_MEM_LIMIT` | `512m` | Promtail memory limit. |
| `PROMTAIL_CPUS` | `0.5` | Promtail CPU limit. |

## Validate the configuration

```bash
GRAFANA_ADMIN_PASSWORD=test-password \
docker compose -f deploy/observability/docker-compose.yml config --quiet
```

## Start

```bash
docker network inspect AppFactory-lab_internal
docker volume create AppFactory-lab_grafana-data
docker volume create obs-promtail-config
docker volume create obs-grafana-provisioning
docker run -d --name obs-config-stage \
  -v obs-promtail-config:/promtail \
  -v obs-grafana-provisioning:/grafana alpine:3 sleep 300
docker cp deploy/observability/promtail/config.yml obs-config-stage:/promtail/config.yml
docker cp deploy/observability/grafana/provisioning/. obs-config-stage:/grafana/
docker rm -f obs-config-stage
docker compose --project-name observability -f deploy/observability/docker-compose.yml up -d --wait
```

## Status and logs

```bash
docker compose --project-name observability -f deploy/observability/docker-compose.yml ps
docker compose --project-name observability -f deploy/observability/docker-compose.yml logs -f loki promtail grafana
```

## Stop

```bash
docker compose --project-name observability -f deploy/observability/docker-compose.yml down
```

External configuration and Grafana state volumes are retained. Loki data and
Promtail positions remain in project-owned named volumes unless `--volumes` is
explicitly added.
