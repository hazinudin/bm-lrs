#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MONITORING_DIR="$(dirname "$SCRIPT_DIR")"

cd "$MONITORING_DIR"

echo "=========================================="
echo "Stopping Prometheus Monitoring Stack"
echo "=========================================="

if docker compose version &> /dev/null; then
    echo "Stopping containers..."
    docker compose down
else
    echo "Stopping containers..."
    docker-compose down
fi

echo ""
echo "=========================================="
echo "Containers stopped"
echo "=========================================="
echo ""
echo "Data volumes preserved. To remove volumes:"
echo "  docker compose down -v"
echo "=========================================="