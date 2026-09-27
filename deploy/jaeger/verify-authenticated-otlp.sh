#!/usr/bin/env bash
# Acceptance smoke for the host-facing OTLP/HTTP boundary deployed by deploy-jaeger-inner.yml.
set -euo pipefail

for command in base64 curl date grep mktemp rm seq sleep tr; do
  command -v "$command" >/dev/null \
    || { echo "Required command unavailable for OTLP smoke: $command"; exit 1; }
done

: "${JAEGER_OTLP_BIND:?set JAEGER_OTLP_BIND}"
: "${JAEGER_OTLP_PORT:?set JAEGER_OTLP_PORT}"
: "${JAEGER_OTLP_USERNAME:?set JAEGER_OTLP_USERNAME}"
: "${JAEGER_OTLP_PASSWORD:?set JAEGER_OTLP_PASSWORD}"
: "${JAEGER_UI_BIND:?set JAEGER_UI_BIND}"
: "${JAEGER_UI_PORT:?set JAEGER_UI_PORT}"

endpoint="http://${JAEGER_OTLP_BIND}:${JAEGER_OTLP_PORT}/v1/traces"
service="AppFactory-external-otlp-smoke"
curl_common=(--connect-timeout 5 --max-time 10 --silent --show-error)
trace_id="$(tr -d '-' < /proc/sys/kernel/random/uuid)"
span_id="${trace_id:0:16}"
start_ns="$(date +%s%N)"
end_ns="$((start_ns + 1000000))"
payload="$(printf '{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"%s"}},{"key":"deployment.environment","value":{"stringValue":"acceptance"}}]},"scopeSpans":[{"scope":{"name":"AppFactory.deploy.otlp-smoke"},"spans":[{"traceId":"%s","spanId":"%s","name":"external-otlp-auth-smoke","kind":1,"startTimeUnixNano":"%s","endTimeUnixNano":"%s"}]}]}]}' "$service" "$trace_id" "$span_id" "$start_ns" "$end_ns")"

# Feed Authorization through mode-600 curl config files, not process arguments visible to other
# workloads on this shared runner.
umask 077
auth_config="$(mktemp)"
wrong_auth_config="$(mktemp)"
response_file="$(mktemp)"
trap 'rm -f "$auth_config" "$wrong_auth_config" "$response_file"' EXIT

write_auth_config() {
  local password="$1" target="$2" token
  token="$(printf '%s:%s' "$JAEGER_OTLP_USERNAME" "$password" | base64 | tr -d '\r\n')"
  printf 'header = "Authorization: Basic %s"\n' "$token" > "$target"
}

write_auth_config "$JAEGER_OTLP_PASSWORD" "$auth_config"
write_auth_config "${JAEGER_OTLP_PASSWORD}x" "$wrong_auth_config"

unauth_status="$(curl "${curl_common[@]}" --output /dev/null --write-out '%{http_code}' \
  --request POST --header 'Content-Type: application/json' --data "$payload" "$endpoint")"
[ "$unauth_status" = 401 ] \
  || { echo "unauthenticated OTLP returned $unauth_status, expected 401"; exit 1; }

wrong_auth_status="$(curl "${curl_common[@]}" --output /dev/null --write-out '%{http_code}' \
  --config "$wrong_auth_config" \
  --request POST --header 'Content-Type: application/json' --data "$payload" "$endpoint")"
[ "$wrong_auth_status" = 401 ] \
  || { echo "wrong OTLP credentials returned $wrong_auth_status, expected 401"; exit 1; }

query_path_status="$(curl "${curl_common[@]}" --output /dev/null --write-out '%{http_code}' \
  --config "$auth_config" \
  "http://${JAEGER_OTLP_BIND}:${JAEGER_OTLP_PORT}/api/services")"
[ "$query_path_status" = 404 ] \
  || { echo "OTLP gateway exposed /api/services with $query_path_status, expected 404"; exit 1; }

auth_status="$(curl "${curl_common[@]}" --output "$response_file" --write-out '%{http_code}' \
  --config "$auth_config" \
  --request POST --header 'Content-Type: application/json' --data "$payload" "$endpoint")"
[ "$auth_status" = 200 ] \
  || { echo "authenticated OTLP returned $auth_status, expected 200"; cat "$response_file"; exit 1; }

query="http://${JAEGER_UI_BIND}:${JAEGER_UI_PORT}/api/traces?service=${service}&limit=20&lookback=1h"
for attempt in $(seq 1 20); do
  if curl "${curl_common[@]}" --fail "$query" | grep -qi "$trace_id"; then
    echo "✅ external authenticated trace visible in Jaeger: trace_id=$trace_id"
    exit 0
  fi
  echo "waiting for trace $trace_id to become queryable ($attempt/20)"
  sleep 1
done

echo "trace $trace_id was accepted but did not become queryable in Jaeger"
exit 1
