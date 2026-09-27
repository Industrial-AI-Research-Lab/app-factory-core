#!/usr/bin/env bash
# deploy-monitoring.sh — Idempotent script to deploy Prometheus monitoring stack to K3s
# Deploys: Prometheus, node-exporter, kube-state-metrics
# Exposes Prometheus on NodePort 30090 so Grafana on 10.0.0.3 can scrape it
# Safe to run multiple times (helm upgrade --install is idempotent)

set -euo pipefail

# K3s kubeconfig
export KUBECONFIG="${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"

NAMESPACE="monitoring"
RELEASE_NAME="kube-prometheus"
CHART_REPO_NAME="prometheus-community"
CHART_REPO_URL="https://prometheus-community.github.io/helm-charts"
PROMETHEUS_PORT=30090

echo "=== Deploying Prometheus Monitoring Stack ==="

# 1. Add/update Prometheus Helm repo
echo "[1/4] Adding Prometheus Helm repo..."
helm repo add "${CHART_REPO_NAME}" "${CHART_REPO_URL}" 2>/dev/null || true
helm repo update

# 2. Create namespace if it doesn't exist
echo "[2/4] Ensuring namespace '${NAMESPACE}' exists..."
kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

# 3. Generate values
echo "[3/4] Generating Helm values..."
cat > /tmp/prometheus-values.yaml << PROM_EOF
# Disable components we don't need (Grafana is on separate server)
grafana:
  enabled: false

alertmanager:
  enabled: false

# Prometheus server
prometheus:
  prometheusSpec:
    retention: 15d
    storageSpec:
      volumeClaimTemplate:
        spec:
          accessModes: ["ReadWriteOnce"]
          resources:
            requests:
              storage: 10Gi
    resources:
      requests:
        cpu: 200m
        memory: 512Mi
      limits:
        cpu: 1000m
        memory: 2Gi
  service:
    type: NodePort
    nodePort: ${PROMETHEUS_PORT}

# Node exporter — system metrics (CPU, RAM, disk, network)
nodeExporter:
  enabled: true

# Kube-state-metrics — K8s object metrics (pods, deployments, etc.)
kubeStateMetrics:
  enabled: true

# Prometheus Operator
prometheusOperator:
  resources:
    requests:
      cpu: 100m
      memory: 128Mi
    limits:
      cpu: 500m
      memory: 512Mi
PROM_EOF

# 4. Install/upgrade
echo "[4/4] Installing/upgrading Prometheus stack via Helm..."
helm upgrade --install "${RELEASE_NAME}" "${CHART_REPO_NAME}/kube-prometheus-stack" \
  --namespace "${NAMESPACE}" \
  --values /tmp/prometheus-values.yaml \
  --wait \
  --timeout 10m

echo ""
echo "=== Prometheus monitoring stack deployed ==="
echo "Prometheus accessible at: http://10.0.0.2:${PROMETHEUS_PORT}"
echo ""
echo "Check pods:"
kubectl -n "${NAMESPACE}" get pods
echo ""
echo "Next step: Add Prometheus datasource in Grafana pointing to http://10.0.0.2:${PROMETHEUS_PORT}"
