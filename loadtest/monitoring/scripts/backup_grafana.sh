#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MONITORING_DIR="$(dirname "$SCRIPT_DIR")"
BACKUP_DIR="$MONITORING_DIR/backups/$(date +%Y%m%d_%H%M%S)"

mkdir -p "$BACKUP_DIR"

echo "=========================================="
echo "Backing up Grafana Data"
echo "=========================================="

echo "Backup directory: $BACKUP_DIR"

if docker ps | grep -q grafana; then
    echo ""
    echo "Exporting dashboards via Grafana API..."
    
    GRAFANA_URL="${GRAFANA_URL:-http://localhost:3000}"
    GRAFANA_USER="${GRAFANA_USER:-admin}"
    GRAFANA_PASSWORD="${GRAFANA_PASSWORD:-admin}"
    
    DASHBOARD_JSON=$(curl -s -u "$GRAFANA_USER:$GRAFANA_PASSWORD" \
        "$GRAFANA_URL/api/dashboards/uid/bm-lrs-load-test" | \
        jq -r '.dashboard')
    
    if [ "$DASHBOARD_JSON" != "null" ] && [ -n "$DASHBOARD_JSON" ]; then
        echo "$DASHBOARD_JSON" > "$BACKUP_DIR/dashboard.json"
        echo "Dashboard exported: $BACKUP_DIR/dashboard.json"
    else
        echo "Warning: Could not export dashboard"
    fi
    
    echo ""
    echo "Copying provisioning files..."
    cp -r "$MONITORING_DIR/grafana/provisioning/dashboards" "$BACKUP_DIR/"
    cp "$MONITORING_DIR/prometheus/prometheus.yml" "$BACKUP_DIR/"
    
else
    echo "Grafana container not running. Copying static files only..."
    cp -r "$MONITORING_DIR/grafana" "$BACKUP_DIR/"
fi

echo ""
echo "Backup complete: $BACKUP_DIR"
echo "=========================================="