#!/usr/bin/env bash
set -eo pipefail

echo "=================================================================="
echo "🧹 Cleaning up Kafka & EDB Conflict Lab Resources"
echo "=================================================================="

echo "1. Deleting load generator jobs..."
kubectl delete job pgbench-load-generator kafka-load-generator -n lab-workloads --ignore-not-found=true

echo "2. Deleting Kafka and EDB Postgres clusters..."
kubectl delete -f manifests/common/kafka-topic.yaml --ignore-not-found=true || true
kubectl delete -f manifests/scenario-2-resolved/ --ignore-not-found=true || true
kubectl delete -f manifests/scenario-1-conflict/ --ignore-not-found=true || true

echo "3. Deleting namespace lab-workloads..."
kubectl delete namespace lab-workloads --ignore-not-found=true

echo "4. Removing benchmark results..."
rm -rf results/*.log results/*.txt 2>/dev/null || true

echo "=================================================================="
echo "✅ Lab environment reset cleanly!"
echo "=================================================================="
