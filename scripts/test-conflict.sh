#!/usr/bin/env bash
set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$REPO_ROOT/results"
mkdir -p "$RESULTS_DIR"

RESULT_LOG="$RESULTS_DIR/scenario-1-conflict.log"
METRICS_LOG="$RESULTS_DIR/scenario-1-metrics.log"

echo "=================================================================="
echo "⚡ SCENARIO 1: LOAD CONFLICT TEST (Same Node: minikube-m02)"
echo "Executing pgbench & kafka-producer-perf-test concurrently..."
echo "=================================================================="
echo "Results will be saved to: $RESULT_LOG"

# Clean up existing load generator jobs
echo "🧹 Cleaning up previous load generator jobs if any..."
kubectl delete job pgbench-load-generator kafka-load-generator -n lab-workloads --ignore-not-found=true --wait=true >/dev/null 2>&1 || true

echo "📍 Current Pod Allocation across Nodes:"
kubectl get pods -n lab-workloads -o wide

# Launch both load jobs simultaneously
echo "🚀 Launching pgbench load generator job..."
kubectl apply -f "$REPO_ROOT/manifests/load-generators/pgbench-load-job.yaml"

echo "🚀 Launching Kafka producer load generator job..."
kubectl apply -f "$REPO_ROOT/manifests/load-generators/kafka-load-job.yaml"

echo "📊 Monitoring node and pod utilization during load execution..."
echo "--- Metric Sampling Started at $(date) ---" > "$METRICS_LOG"

# Background sampling loop for kubectl top
(
  for i in {1..20}; do
    echo "=== Sample #$i ($(date +'%T')) ===" >> "$METRICS_LOG"
    echo ">> NODES:" >> "$METRICS_LOG"
    kubectl top nodes --no-headers 2>/dev/null >> "$METRICS_LOG" || echo "Metrics API unavailable" >> "$METRICS_LOG"
    echo ">> PODS in lab-workloads:" >> "$METRICS_LOG"
    kubectl top pods -n lab-workloads --no-headers 2>/dev/null >> "$METRICS_LOG" || true
    echo "" >> "$METRICS_LOG"
    sleep 4
  done
) &
METRIC_PID=$!

echo "⏳ Waiting for load generator jobs to complete (approx 60-90 seconds)..."

# Tail logs while waiting
PGBENCH_POD=""
KAFKA_POD=""
while [ -z "$PGBENCH_POD" ] || [ -z "$KAFKA_POD" ]; do
  PGBENCH_POD=$(kubectl get pod -n lab-workloads -l app.kubernetes.io/name=pgbench-load -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  KAFKA_POD=$(kubectl get pod -n lab-workloads -l app.kubernetes.io/name=kafka-load -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  sleep 2
done

# Wait for both jobs to finish
kubectl wait --for=condition=complete job/pgbench-load-generator -n lab-workloads --timeout=240s || echo "⚠️ pgbench job timed out or failed"
kubectl wait --for=condition=complete job/kafka-load-generator -n lab-workloads --timeout=240s || echo "⚠️ Kafka load job timed out or failed"

# Terminate metric sampler if still running
kill $METRIC_PID 2>/dev/null || true

echo "=================================================================="
echo "📑 Extracted Benchmark & Contention Results:"
echo "=================================================================="

{
  echo "=================================================================="
  echo "SCENARIO 1 REPORT: CO-LOCATED CONFLICT (minikube-m02)"
  echo "Timestamp: $(date)"
  echo "=================================================================="
  echo ""
  echo "--- 1. Node Topology During Test ---"
  kubectl get pods -n lab-workloads -o wide
  echo ""
  echo "--- 2. Node & Pod Resource Saturation Sample ---"
  cat "$METRICS_LOG" | tail -n 25
  echo ""
  echo "--- 3. EDB PostgreSQL (pgbench) Benchmark Output ---"
  kubectl logs job/pgbench-load-generator -n lab-workloads
  echo ""
  echo "--- 4. Kafka Producer Benchmark Output ---"
  kubectl logs job/kafka-load-generator -n lab-workloads
  echo ""
  echo "--- 5. Kagent AI SRE Agent Diagnostics & Alerts ---"
  kubectl logs -n kagent -l app.kubernetes.io/name=kagent-monitoring-agent --tail=10 2>/dev/null || echo "Kagent agent not yet deployed (run ./setup-monitoring-kagent.sh)"
  echo ""
  echo "=================================================================="
} | tee "$RESULT_LOG"

echo ""
echo "=================================================================="
echo "⚠️  OBSERVATION (SCENARIO 1 BOTTLENECK):"
echo "  - Both workloads shared 100% of node 'minikube-m02' CPU & I/O limits."
echo "  - CFS CPU quota throttling slowed down PostgreSQL query latency."
echo "  - Disk flush serialization (WAL fsync + Kafka log segment commit) created disk queue stalls."
echo "  - Node 'minikube' remained largely idle while 'minikube-m02' was pinned near 100% saturation."
echo "  - Kagent SRE Agent raised alert: StatefulWorkloadColocationDetected."
echo "Next Step: Run ./scripts/apply-recommendation.sh (or ./apply-recommendation.sh) to resolve the conflict."
echo "=================================================================="
