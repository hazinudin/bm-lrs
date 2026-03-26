#!/usr/bin/env bash

set -e

SSH_HOST="${SSH_HOST:-your-remote-server}"
SSH_USER="${SSH_USER:-your-server-user}"
SSH_KEY="${SSH_KEY:-~/.ssh/your-key.pem}"
SSH_PORT="${SSH_PORT:-22}"

METRICS_INTERVAL="${METRICS_INTERVAL:-5}"

GRAFANA_URL="${GRAFANA_URL:-http://localhost:3000}"
GRAFANA_USER="${GRAFANA_USER:-admin}"
GRAFANA_PASSWORD="${GRAFANA_PASSWORD:-admin}"

PROMETHEUS_URL="${PROMETHEUS_URL:-http://localhost:9090}"

SSH_OPTS="-p ${SSH_PORT} -i ${SSH_KEY} -o StrictHostKeyChecking=no"

echo "=== Remote Server Monitoring ==="
echo "Host: ${SSH_USER}@${SSH_HOST}"
echo "Metrics Interval: ${METRICS_INTERVAL}s"
echo ""

monitor_system_metrics() {
    echo "--- System Metrics (via SSH) ---"

    ssh ${SSH_OPTS} ${SSH_USER}@${SSH_HOST} << 'SSHEOF'
        echo "=== CPU Usage ==="
        top -bn1 | grep "Cpu(s)" | awk '{print "CPU: " $2 + $4 "%"}'

        echo ""
        echo "=== Memory Usage ==="
        free -h | awk '/^Mem:/ {printf "Memory: %s / %s (%.1f%%)\n", $3, $2, ($3/$2)*100}'

        echo ""
        echo "=== Load Average ==="
        uptime | awk -F'load average:' '{print "Load: " $2}'

        echo ""
        echo "=== Disk Usage ==="
        df -h / | awk 'NR==2 {printf "Disk: %s / %s (%s)\n", $3, $2, $5}'

        echo ""
        echo "=== Top Processes by CPU ==="
        ps aux --sort=-%cpu | head -6 | awk '{printf "  %s %s%% CPU\n", $11, $3}'

        echo ""
        echo "=== Docker Containers ==="
        if command -v docker &> /dev/null; then
            docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || echo "Docker not available or no containers"
        else
            echo "Docker not installed"
        fi
SSHEOF
}

monitor_service_endpoints() {
    echo ""
    echo "--- Service Endpoints (local) ---"

    echo "Node Exporter (port 9100):"
    curl -s --max-time 2 "http://${SSH_HOST}:9100/metrics" | head -5 | grep -v "^#" | head -3 || echo "  Not reachable"

    echo ""
    echo "Prometheus (port 9090):"
    curl -s --max-time 2 "http://${SSH_HOST}:9090/-/healthy" || echo "  Not reachable"

    echo ""
    echo "Grafana (port 3000):"
    curl -s --max-time 2 "http://${SSH_HOST}:3000/api/health" | python3 -m json.tool 2>/dev/null || echo "  Not reachable"
}

query_prometheus() {
    local query="$1"
    curl -s --max-time 5 "${PROMETHEUS_URL}/api/v1/query?query=$(echo "$query" | jq -s -R -r @uri)" 2>/dev/null
}

monitor_prometheus_metrics() {
    echo ""
    echo "--- Prometheus Metrics (via API) ---"

    local cpu_query='100 - (avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[1m])) * 100)'
    echo "CPU Usage:"
    query_prometheus "$cpu_query" | python3 -c "import sys,json; d=json.load(sys.stdin); [print(f'  {r[\"metric\"].get(\"instance\",\"?\")}: {float(r[\"value\"][1]):.1f}%') for r in d.get('data',{}).get('result',[])]" 2>/dev/null || echo "  Query failed"

    local mem_query='100 * (1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes))'
    echo ""
    echo "Memory Usage:"
    query_prometheus "$mem_query" | python3 -c "import sys,json; d=json.load(sys.stdin); [print(f'  {r[\"metric\"].get(\"instance\",\"?\")}: {float(r[\"value\"][1]):.1f}%') for r in d.get('data',{}).get('result',[])]" 2>/dev/null || echo "  Query failed"

    local load_query='node_load1'
    echo ""
    echo "Load Average (1m):"
    query_prometheus "$load_query" | python3 -c "import sys,json; d=json.load(sys.stdin); [print(f'  {r[\"metric\"].get(\"instance\",\"?\")}: {float(r[\"value\"][1]):.2f}') for r in d.get('data',{}).get('result',[])]" 2>/dev/null || echo "  Query failed"
}

if [[ "$1" == "--once" ]]; then
    monitor_system_metrics
    monitor_service_endpoints
    monitor_prometheus_metrics
else
    echo "Continuous monitoring started. Press Ctrl+C to stop."
    echo ""

    while true; do
        clear
        echo "=== $(date) ==="
        monitor_system_metrics
        monitor_service_endpoints
        monitor_prometheus_metrics
        sleep "${METRICS_INTERVAL}"
    done
fi
