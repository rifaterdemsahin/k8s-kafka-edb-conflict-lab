#!/usr/bin/env bash
set -eo pipefail

echo "=================================================================="
echo "🔧 Step 1: Installing Strimzi Kafka Operator & CloudNativePG Operator"
echo "=================================================================="

# Check prerequisites
for cmd in kubectl helm; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "❌ Error: '$cmd' is not installed or not in PATH."
    exit 1
  fi
done

# Verify Kubernetes cluster connection
if ! kubectl cluster-info >/dev/null 2>&1; then
  echo "❌ Error: Kubernetes cluster is not accessible. Run 'minikube start' first."
  exit 1
fi

echo "📦 1. Adding Helm repositories..."
helm repo add strimzi https://strimzi.io/charts/ --force-update
helm repo add cnpg https://cloudnative-pg.github.io/charts --force-update
helm repo update

echo "🚀 2. Installing / Upgrading Strimzi Kafka Operator..."
kubectl create namespace kafka-operator --dry-run=client -o yaml | kubectl apply -f -
helm upgrade --install strimzi-kafka-operator strimzi/strimzi-kafka-operator \
  --namespace kafka-operator \
  --wait \
  --timeout 300s

echo "🚀 3. Installing / Upgrading CloudNativePG (EDB) Operator..."
kubectl create namespace cnpg-system --dry-run=client -o yaml | kubectl apply -f -
helm upgrade --install cnpg-operator cnpg/cloudnative-pg \
  --namespace cnpg-system \
  --wait \
  --timeout 300s

echo "⏳ 4. Waiting for Operator Deployments to become fully Ready..."
kubectl rollout status deployment/strimzi-cluster-operator -n kafka-operator --timeout=180s
kubectl rollout status deployment/cnpg-controller-manager -n cnpg-system --timeout=180s

echo "📋 5. Verifying Operator CRDs..."
kubectl get crd kafkas.kafka.strimzi.io kafkanodepools.kafka.strimzi.io clusters.postgresql.cnpg.io

echo "=================================================================="
echo "✅ Both Strimzi Kafka Operator and CloudNativePG Operator are Ready!"
echo "Next step: Run ./scripts/deploy-scenario-1.sh"
echo "=================================================================="
