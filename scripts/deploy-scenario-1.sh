#!/usr/bin/env bash
set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=================================================================="
echo "💥 Deploying Scenario 1: Kafka & EDB Postgres on Same Node"
echo "Target Node: minikube-m02"
echo "=================================================================="

# Check if target node exists
if ! kubectl get node minikube-m02 >/dev/null 2>&1; then
  echo "⚠️ Warning: Node 'minikube-m02' not found. Available nodes:"
  kubectl get nodes -o wide
  echo "Make sure minikube was started with: minikube start --nodes 2"
fi

echo "📁 1. Creating Namespace lab-workloads..."
kubectl apply -f "$REPO_ROOT/manifests/common/namespace.yaml"

echo "🐘 2. Applying EDB Postgres cluster manifest (pinned to minikube-m02)..."
kubectl apply -f "$REPO_ROOT/manifests/scenario-1-conflict/edb-postgres.yaml"

echo "📬 3. Applying Kafka cluster manifest (pinned to minikube-m02)..."
kubectl apply -f "$REPO_ROOT/manifests/scenario-1-conflict/kafka-cluster.yaml"

echo "⏳ 4. Waiting for EDB Postgres cluster to become Ready..."
kubectl wait --for=condition=Ready cluster/edb-postgres -n lab-workloads --timeout=300s || {
  echo "Checking Postgres pod status:"
  kubectl get pods -n lab-workloads -l cnpg.io/cluster=edb-postgres -o wide
}

echo "⏳ 5. Waiting for Kafka cluster to become Ready..."
kubectl wait --for=condition=Ready kafka/kafka-cluster -n lab-workloads --timeout=360s || {
  echo "Checking Kafka pod status:"
  kubectl get pods -n lab-workloads -l app.kubernetes.io/name=kafka -o wide
}

echo "📜 6. Creating Kafka Topic: perf-test-topic..."
kubectl apply -f "$REPO_ROOT/manifests/common/kafka-topic.yaml"
kubectl wait --for=condition=Ready kafkatopic/perf-test-topic -n lab-workloads --timeout=120s || true

echo "=================================================================="
echo "📍 Pod Placement Verification (Both should be on minikube-m02):"
echo "=================================================================="
kubectl get pods -n lab-workloads -o wide --show-labels

echo ""
echo "=================================================================="
echo "✅ Scenario 1 Deployed successfully!"
echo "Run ./scripts/test-conflict.sh (or ./test-conflict.sh) to run the load test."
echo "=================================================================="
