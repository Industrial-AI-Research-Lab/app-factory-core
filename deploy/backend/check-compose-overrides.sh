#!/usr/bin/env bash
# Refuse when the stand's compose.yaml sets, under `environment:`, a key that backend.<env>.env
# (rendered from GitHub) also carries. Compose lets `environment:` win over `env_file`, so such a
# key silently shadows the GitHub value on every recreate and the stand ends up with two sources.
#
# Usage: docker compose ... config --no-env-resolution --format json \
#          | check-compose-overrides.sh <.env.template> <service>
# --no-env-resolution matters: without it compose folds env_file into `environment` and every
# template key would look like an override.
set -euo pipefail
export LC_ALL=C

template="${1:?usage: check-compose-overrides.sh <.env.template> <service> < compose-config.json}"
service="${2:?usage: check-compose-overrides.sh <.env.template> <service> < compose-config.json}"
command -v jq >/dev/null || { echo "jq is required" >&2; exit 2; }

config="$(cat)"
[ -n "$config" ] || { echo "empty compose config on stdin" >&2; exit 2; }

if ! printf '%s' "$config" | jq -e --arg s "$service" '.services | has($s)' >/dev/null; then
  echo "service '$service' not found in compose config" >&2
  exit 2
fi
inline="$(printf '%s' "$config" | jq -r --arg s "$service" '.services[$s].environment // {} | if type == "object" then keys[] else error("environment is not a map; feed docker compose config output") end' | sort -u)"
managed="$(grep -oE '^[A-Za-z_][A-Za-z0-9_]*' "$template" | sort -u)"
overlap="$(comm -12 <(printf '%s\n' "$inline") <(printf '%s\n' "$managed") | sed '/^$/d')"

if [ -n "$overlap" ]; then
  echo "compose.yaml overrides GitHub-managed keys for '$service' (environment: wins over env_file):" >&2
  printf '  %s\n' $overlap >&2
  echo "Delete them from the stand's compose.yaml; the rendered backend.<env>.env is the only source." >&2
  exit 1
fi
echo "OK: '$service' has no environment: override of the $(printf '%s\n' "$managed" | wc -l | tr -d ' ') GitHub-managed keys."
