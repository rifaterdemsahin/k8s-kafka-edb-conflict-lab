#!/usr/bin/env bash
set -eo pipefail

echo "=================================================================="
echo "🤖 Querying Kagent AI SRE Incident Response Agent"
echo "=================================================================="

# Check if kagent agent is running
if ! kubectl get pods -n kagent -l app.kubernetes.io/name=kagent-monitoring-agent >/dev/null 2>&1; then
  echo "⚠️ Kagent is not running. Deploying it now..."
  ./scripts/setup-monitoring-kagent.sh
fi

echo "🔍 1. Fetching recent Kagent SRE Agent diagnostics & alert decisions:"
echo "------------------------------------------------------------------"
kubectl logs -n kagent -l app.kubernetes.io/name=kagent-monitoring-agent --tail=20
echo "------------------------------------------------------------------"

echo ""
echo "📊 2. Active Pod Topologies across Minikube nodes:"
kubectl get pods -n lab-workloads -o wide

echo ""
echo "📈 3. Current Node CPU / Memory Consumption (kubectl top):"
kubectl top nodes 2>/dev/null || echo "Metrics API still initializing..."

echo ""
echo "=================================================================="
echo "💡 Kagent Automated Recommendation:"
KAFKA_NODE=$(kubectl get pod -n lab-workloads -l app.kubernetes.io/name=kafka -o jsonpath='{.items[0].spec.nodeName}' 2>/dev/null || echo "")
PG_NODE=$(kubectl get pod -n lab-workloads -l cnpg.io/cluster=edb-postgres -o jsonpath='{.items[0].spec.nodeName}' 2>/dev/null || echo "")

if [ -n "$KAFKA_NODE" ] && [ -n "$PG_NODE" ] && [ "$KAFKA_NODE" == "$PG_NODE" ]; then
  echo "🚨 CRITICAL CONFLICT DETECTED:"
  echo "   Workloads Kafka and EDB Postgres are both scheduled on: $KAFKA_NODE"
  echo "   Action Required: Run './apply-recommendation.sh' to apply podAntiAffinity."
elif [ -n "$KAFKA_NODE" ] && [ -n "$PG_NODE" ] && [ "$KAFKA_NODE" != "$PG_NODE" ]; then
  echo "✅ HEALTHY ISOLATION ACTIVE:"
  echo "   Kafka is on $KAFKA_NODE and EDB Postgres is on $PG_NODE."
  echo "   Cluster compute, page cache, and I/O are properly segregated."
else
  echo "ℹ️ Workloads not fully running. Deploy Scenario 1 with ./deploy-scenario-1.sh"
fi
echo "=================================================================="
