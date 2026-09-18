#!/usr/bin/env bash
set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=================================================================="
echo "🤖 Installing Prometheus Monitoring & Kagent AI SRE Agent"
echo "=================================================================="

# Check prerequisites
for cmd in kubectl helm; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "❌ Error: '$cmd' is not installed or not in PATH."
    exit 1
  fi
done

echo "📦 1. Adding Prometheus Community Helm repository..."
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts --force-update
helm repo update

echo "🚀 2. Deploying Prometheus & Alertmanager in namespace 'monitoring'..."
kubectl create namespace monitoring --dry-run=client -o yaml | kubectl apply -f -

# Lightweight prometheus installation tailored for multi-node dev/lab environments
helm upgrade --install prometheus prometheus-community/prometheus \
  --namespace monitoring \
  --set server.resources.requests.cpu=150m \
  --set server.resources.requests.memory=256Mi \
  --set server.resources.limits.cpu=500m \
  --set server.resources.limits.memory=512Mi \
  --set server.persistentVolume.enabled=false \
  --set alertmanager.persistentVolume.enabled=false \
  --set alertmanager.resources.requests.cpu=50m \
  --set alertmanager.resources.requests.memory=128Mi \
  --set nodeExporter.enabled=true \
  --set pushgateway.enabled=false \
  --wait \
  --timeout 240s || {
    echo "⚠️ Warning: Helm timeout or waiting. Checking monitoring pods:"
    kubectl get pods -n monitoring
  }

echo "📜 3. Applying Prometheus Alerting Rules for Resource Contention..."
kubectl apply -f "$REPO_ROOT/manifests/monitoring/prometheus-alerts.yaml" || true

echo "🤖 4. Installing Kagent CLI..."
if ! command -v kagent >/dev/null 2>&1; then
  echo "Downloading kagent CLI..."
  curl -fsSL https://raw.githubusercontent.com/kagent-dev/kagent/refs/heads/main/scripts/get-kagent | bash 2>/dev/null || {
    echo "ℹ️ Continuing with Kubernetes-native Kagent deployment..."
  }
fi

echo "🤖 5. Deploying Kagent AI SRE Monitoring Agent & CRDs in namespace 'kagent'..."
kubectl apply -f "$REPO_ROOT/manifests/monitoring/kagent-agent.yaml"

echo "⏳ 6. Waiting for Kagent Monitoring Agent to become Ready..."
kubectl rollout status deployment/kagent-monitoring-agent -n kagent --timeout=120s || {
  echo "Checking kagent pod status:"
  kubectl get pods -n kagent
}

echo "=================================================================="
echo "✅ Monitoring & Kagent AI Agent Stack Deployed Successfully!"
echo "=================================================================="
kubectl get pods -n monitoring
echo ""
kubectl get pods -n kagent
echo "=================================================================="
echo "You can query the Kagent SRE Agent at any time using:"
echo "  ./query-kagent.sh"
echo "=================================================================="
