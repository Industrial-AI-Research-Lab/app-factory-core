#!/usr/bin/env bash
# deploy-alloy.sh — Idempotent deploy of Grafana Alloy to a per-environment k3s cluster.
#
# Alloy runs as a DaemonSet and does three jobs:
#   1. LOGS    — tails all pod logs, pushes to Loki on the logging host (10.0.0.3).
#   2. METRICS — remote-writes to Prometheus on the same logging host. Production scrapes
#                kubelet / cAdvisor / kube-state-metrics / node-exporter / CoreDNS; every other
#                env ships only node + pod CPU, memory, disk and container-start counters.
#   3. CERT PROBES (production only) — blackbox TLS probes of the public endpoints so we get
#                alerted BEFORE a served cert expires. This is the backstop behind the mongo
#                cert-reloader self-heal (see infra/k8s/mongodb/mongodb.yaml).
#
# Managed by Helm (chart grafana/alloy). Safe to re-run: helm upgrade --install is idempotent
# AND this script now carries the FULL live config. Previously the metrics pipeline and cert
# probes existed only in the live ConfigMap (added out-of-band), so re-running the old script
# would have silently wiped them — this reconciles that drift.

set -euo pipefail

# K3s kubeconfig
export KUBECONFIG="${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"

NAMESPACE="monitoring"
RELEASE_NAME="alloy"
# Loki and Prometheus both live on the shared logging host (10.0.0.3) — same for every env.
LOKI_URL="${LOKI_URL:-http://10.0.0.3:3100/loki/api/v1/push}"
PROM_REMOTE_WRITE_URL="${PROM_REMOTE_WRITE_URL:-http://10.0.0.3:9090/api/v1/write}"
ENVIRONMENT="${ENVIRONMENT:-production}"
CHART_REPO_NAME="grafana"
CHART_REPO_URL="https://grafana.github.io/helm-charts"
CHART_VERSION="${CHART_VERSION:-1.6.1}"   # default = prod's version (Alloy v1.13.2); dev/staging run 1.7.0, pass it there

echo "=== Deploying Grafana Alloy (env=${ENVIRONMENT}) ==="

# 1. Add/update Grafana Helm repo
echo "[1/5] Adding Grafana Helm repo..."
helm repo add "${CHART_REPO_NAME}" "${CHART_REPO_URL}" 2>/dev/null || true
helm repo update

# 2. Create namespace if it doesn't exist
echo "[2/5] Ensuring namespace '${NAMESPACE}' exists..."
kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

# 3. Build the Alloy River config.
# Environment-specific values (LOKI_URL, PROM_REMOTE_WRITE_URL, ENVIRONMENT) are read by Alloy
# at RUNTIME via sys.env(...) from the container env set in extraEnv below — so these heredocs are
# fully literal (quoted delimiters), which also preserves the literal `$1` in the CoreDNS rule.
echo "[3/5] Building Alloy River config..."
cat > /tmp/alloy-config.alloy << 'CONFIG_EOF'
// ==== LOGS PIPELINE ====
discovery.kubernetes "pods" {
  role = "pod"
}
discovery.relabel "pods" {
  targets = discovery.kubernetes.pods.targets
  rule {
    source_labels = ["__meta_kubernetes_namespace"]
    target_label  = "namespace"
  }
  rule {
    source_labels = ["__meta_kubernetes_pod_name"]
    target_label  = "pod"
  }
  rule {
    source_labels = ["__meta_kubernetes_pod_container_name"]
    target_label  = "container"
  }
  rule {
    source_labels = ["__meta_kubernetes_pod_label_app"]
    target_label  = "app"
  }
  rule {
    source_labels = ["__meta_kubernetes_pod_label_app_kubernetes_io_name"]
    target_label  = "app_name"
  }
}
loki.source.kubernetes "pods" {
  targets    = discovery.relabel.pods.output
  forward_to = [loki.process.pipeline.receiver]
}
loki.process "pipeline" {
  stage.json { expressions = { level = "level", msg = "msg", ts = "timestamp" } }
  stage.labels { values = { level = "" } }
  stage.static_labels { values = { environment = sys.env("ENVIRONMENT") } }
  forward_to = [loki.write.loki_remote.receiver]
}
loki.write "loki_remote" {
  endpoint { url = sys.env("LOKI_URL") }
}

// ==== METRICS PIPELINE ====
prometheus.remote_write "default" {
  endpoint { url = sys.env("PROM_REMOTE_WRITE_URL") }
  external_labels = {
    cluster = "AppFactory",
    host    = sys.env("ENVIRONMENT"),
  }
}
discovery.kubernetes "nodes" {
  role = "node"
}
CONFIG_EOF

# Only production runs kube-state-metrics and node-exporter. The kubelet's /metrics also carries
# ~40k k3s API-server series per node, so other envs keep only whole-node and per-pod series
# (~300 instead of ~50k; every env shares the logging host's Prometheus disk).
if [ "${ENVIRONMENT}" = "production" ]; then
  cat >> /tmp/alloy-config.alloy << 'PROD_METRICS_EOF'
prometheus.scrape "kubelet" {
  job_name          = "kubelet"
  targets           = discovery.kubernetes.nodes.targets
  forward_to        = [prometheus.remote_write.default.receiver]
  scheme            = "https"
  bearer_token_file = "/var/run/secrets/kubernetes.io/serviceaccount/token"
  tls_config {
    ca_file              = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
    insecure_skip_verify = true
  }
  honor_labels = true
}
prometheus.scrape "cadvisor" {
  job_name          = "cadvisor"
  targets           = discovery.kubernetes.nodes.targets
  forward_to        = [prometheus.remote_write.default.receiver]
  scheme            = "https"
  metrics_path      = "/metrics/cadvisor"
  bearer_token_file = "/var/run/secrets/kubernetes.io/serviceaccount/token"
  tls_config {
    ca_file              = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
    insecure_skip_verify = true
  }
  honor_labels = true
}
prometheus.scrape "kube_state_metrics" {
  job_name   = "kube-state-metrics"
  targets    = [{ __address__ = "kube-prometheus-kube-state-metrics.monitoring.svc.cluster.local:8080" }]
  forward_to = [prometheus.remote_write.default.receiver]
}
prometheus.scrape "node_exporter" {
  job_name   = "node-exporter"
  targets    = [{ __address__ = "kube-prometheus-prometheus-node-exporter.monitoring.svc.cluster.local:9100" }]
  forward_to = [prometheus.remote_write.default.receiver]
}
discovery.relabel "coredns" {
  targets = discovery.kubernetes.pods.targets
  rule {
    source_labels = ["__meta_kubernetes_namespace", "__meta_kubernetes_pod_label_k8s_app"]
    regex         = "kube-system;kube-dns"
    action        = "keep"
  }
  rule {
    source_labels = ["__meta_kubernetes_pod_ip"]
    target_label  = "__address__"
    replacement   = "$1:9153"
  }
}
prometheus.scrape "coredns" {
  job_name   = "coredns"
  targets    = discovery.relabel.coredns.output
  forward_to = [prometheus.remote_write.default.receiver]
}
PROD_METRICS_EOF
else
  cat >> /tmp/alloy-config.alloy << 'LEAN_METRICS_EOF'
prometheus.scrape "kubelet_resource" {
  job_name          = "kubelet-resource"
  targets           = discovery.kubernetes.nodes.targets
  forward_to        = [prometheus.remote_write.default.receiver]
  scheme            = "https"
  metrics_path      = "/metrics/resource"
  bearer_token_file = "/var/run/secrets/kubernetes.io/serviceaccount/token"
  tls_config {
    ca_file              = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
    insecure_skip_verify = true
  }
}
prometheus.scrape "cadvisor" {
  job_name          = "cadvisor"
  targets           = discovery.kubernetes.nodes.targets
  forward_to        = [prometheus.relabel.cadvisor_node.receiver]
  scheme            = "https"
  metrics_path      = "/metrics/cadvisor"
  bearer_token_file = "/var/run/secrets/kubernetes.io/serviceaccount/token"
  tls_config {
    ca_file              = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
    insecure_skip_verify = true
  }
}
// The root cgroup (id="/") is the whole node: its usage of the real disk and its major page
// faults, which climb into the thousands per second when the node runs out of memory.
prometheus.relabel "cadvisor_node" {
  forward_to = [prometheus.remote_write.default.receiver]
  rule {
    source_labels = ["__name__"]
    regex         = "up|container_fs_usage_bytes|container_fs_limit_bytes|container_memory_failures_total|machine_memory_bytes|machine_cpu_cores"
    action        = "keep"
  }
  rule {
    source_labels = ["id"]
    regex         = "/|"
    action        = "keep"
  }
  rule {
    source_labels = ["device"]
    regex         = "|/dev/(sd|vd|nvme|xvd).*"
    action        = "keep"
  }
  rule {
    source_labels = ["failure_type", "scope"]
    regex         = "pgfault;.*|.*;hierarchy"
    action        = "drop"
  }
}
prometheus.scrape "kubelet" {
  job_name          = "kubelet"
  targets           = discovery.kubernetes.nodes.targets
  forward_to        = [prometheus.relabel.kubelet_counters.receiver]
  scheme            = "https"
  bearer_token_file = "/var/run/secrets/kubernetes.io/serviceaccount/token"
  tls_config {
    ca_file              = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
    insecure_skip_verify = true
  }
}
// Without kube-state-metrics there are no per-pod restart counts; a climbing
// kubelet_started_containers_total is how a crash loop shows up.
prometheus.relabel "kubelet_counters" {
  forward_to = [prometheus.remote_write.default.receiver]
  rule {
    source_labels = ["__name__"]
    regex         = "up|kubelet_started_containers_total|kubelet_running_pods|kubelet_running_containers"
    action        = "keep"
  }
}
LEAN_METRICS_EOF
fi

# 3b. Cert-expiry blackbox probes — PRODUCTION ONLY.
# Targets are production's public endpoints; other envs don't publish all of them (llm/registry
# are prod-only), so the block is scoped here. Feeds the Grafana alert "TLS served-cert expiring
# soon (under 14d)". The module config is one-line flow YAML on purpose: this whole config gets
# indented when embedded into the Helm values below, and a multi-line YAML raw string would have
# its interior indentation shifted. To probe another env, append a block with that env's hostnames.
if [ "${ENVIRONMENT}" = "production" ]; then
  cat >> /tmp/alloy-config.alloy << 'PROBE_EOF'

// ==== CERT EXPIRY PROBES (blackbox) ====
prometheus.exporter.blackbox "cert" {
  config = `{"modules":{"tcp_cert":{"prober":"tcp","timeout":"5s","tcp":{"tls":true,"tls_config":{"insecure_skip_verify":true}}}}}`
  target {
    name    = "mongo"
    address = "mongo.AppFactory.example.com:30017"
    module  = "tcp_cert"
  }
  target {
    name    = "AppFactory"
    address = "AppFactory.example.com:443"
    module  = "tcp_cert"
  }
  target {
    name    = "llm"
    address = "llm.AppFactory.example.com:443"
    module  = "tcp_cert"
  }
  target {
    name    = "registry"
    address = "registry.AppFactory.example.com:443"
    module  = "tcp_cert"
  }
}
prometheus.scrape "cert_probe" {
  job_name        = "blackbox-cert"
  targets         = prometheus.exporter.blackbox.cert.targets
  forward_to      = [prometheus.remote_write.default.receiver]
  scrape_interval = "5m"
}
PROBE_EOF
fi

# 4. Generate the Alloy Helm values, embedding the config under configMap.content.
echo "[4/5] Generating Alloy Helm values..."
cat > /tmp/alloy-values.yaml << 'VALUES_EOF'
alloy:
  configMap:
    content: |
CONFIG_PLACEHOLDER
  extraArgs:
  - --storage.path=/var/lib/alloy/data
  extraEnv:
  - name: LOKI_URL
    value: "LOKI_URL_PLACEHOLDER"
  - name: PROM_REMOTE_WRITE_URL
    value: "PROM_URL_PLACEHOLDER"
  - name: ENVIRONMENT
    value: "ENVIRONMENT_PLACEHOLDER"
  resources:
    requests:
      cpu: 50m
      memory: 64Mi
    limits:
      cpu: 500m
      memory: 1Gi
  livenessProbe:
    httpGet:
      path: /-/healthy
      port: 12345
    initialDelaySeconds: 15
    periodSeconds: 20
    failureThreshold: 3
    timeoutSeconds: 5
  # Persist positions/WAL so a pod restart resumes each log stream instead of re-reading from
  # the start (which floods Loki with duplicates).
  mounts:
    extra:
    - name: positions
      mountPath: /var/lib/alloy/data
controller:
  type: daemonset
  # hostPath per node — DaemonSets can't use volumeClaimTemplates; survives pod restarts.
  volumes:
    extra:
    - name: positions
      hostPath:
        path: /var/lib/alloy/data
        type: DirectoryOrCreate
serviceAccount:
  create: true
VALUES_EOF

# Replace the placeholder line with the config file, indented 6 spaces to sit under 'content: |'.
sed 's/^/      /' /tmp/alloy-config.alloy > /tmp/alloy-config-indented.alloy
sed -i -e '/CONFIG_PLACEHOLDER/{r /tmp/alloy-config-indented.alloy' -e 'd}' /tmp/alloy-values.yaml

# Fill the single-line runtime env values (| delimiter so URLs' slashes don't clash with sed).
# Escape the replacement's sed metachars first (\ & |): an overridden value containing them
# would otherwise break the s|...| expression or expand & to the whole matched placeholder.
sed_escape() { printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/&/\\&/g' -e 's/|/\\|/g'; }
sed -i "s|LOKI_URL_PLACEHOLDER|$(sed_escape "${LOKI_URL}")|g" /tmp/alloy-values.yaml
sed -i "s|PROM_URL_PLACEHOLDER|$(sed_escape "${PROM_REMOTE_WRITE_URL}")|g" /tmp/alloy-values.yaml
sed -i "s|ENVIRONMENT_PLACEHOLDER|$(sed_escape "${ENVIRONMENT}")|g" /tmp/alloy-values.yaml

# 5. Install/upgrade Alloy
echo "[5/5] Installing/upgrading Alloy via Helm..."
helm upgrade --install "${RELEASE_NAME}" "${CHART_REPO_NAME}/alloy" \
  --namespace "${NAMESPACE}" \
  --version "${CHART_VERSION}" \
  --values /tmp/alloy-values.yaml \
  --atomic \
  --wait \
  --timeout 5m

echo ""
echo "=== Alloy deployment complete (env=${ENVIRONMENT}) ==="
echo "Logs    -> ${LOKI_URL}"
echo "Metrics -> ${PROM_REMOTE_WRITE_URL}"
if [ "${ENVIRONMENT}" = "production" ]; then
  echo "Cert probes active: mongo/AppFactory/llm/registry (Grafana alert: served-cert < 14d)"
fi
echo "Check pods: kubectl -n ${NAMESPACE} get pods -l app.kubernetes.io/name=alloy"
