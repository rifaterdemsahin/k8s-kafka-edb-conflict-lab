#!/usr/bin/env bash
set -eo pipefail

echo "=================================================================="
echo "🚀 Initializing Kubernetes Kafka & EDB Conflict Lab in Codespaces"
echo "=================================================================="

# Ensure script permissions
chmod +x scripts/*.sh *.sh 2>/dev/null || true

# Wait for Docker daemon if needed
echo "⏳ Checking Docker daemon..."
timeout 60 bash -c 'until docker info >/dev/null 2>&1; do sleep 2; done' || {
  echo "⚠️ Warning: Docker socket might still be initializing. Starting minikube with standard driver..."
}

# Start multi-node Minikube cluster (Requirement 1: 2 nodes, 4 cpus, 8192 memory)
echo "📦 Starting Minikube multi-node cluster (nodes: 2, cpus: 4, memory: 8192MB)..."
minikube start \
  --nodes 2 \
  --cpus 4 \
  --memory 8192 \
  --driver=docker \
  --wait=all

# Enable metrics-server so 'kubectl top' commands function properly during load tests
echo "📊 Enabling Minikube metrics-server addon..."
minikube addons enable metrics-server

# Verify cluster connectivity
echo "🔍 Cluster status:"
kubectl get nodes -o wide

# Start background dashboard on port 30085 if python3 is present
if command -v python3 >/dev/null 2>&1; then
  echo "🌐 Starting interactive Lab Web Dashboard on port 30085..."
  nohup python3 -m http.server 30085 --directory "$(pwd)" >/tmp/dashboard.log 2>&1 &
fi

echo "=================================================================="
echo "✅ Environment Ready!"
echo "Next steps:"
echo "  1. ./setup-operators.sh          - Install Kafka & EDB Operators"
echo "  2. ./setup-monitoring-kagent.sh  - Install Prometheus & Kagent AI SRE Agent"
echo "  3. ./deploy-scenario-1.sh        - Deploy Workloads on minikube-m02"
echo "  4. ./test-conflict.sh            - Execute Conflict Load Test"
echo "=================================================================="
