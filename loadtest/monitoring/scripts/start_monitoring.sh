#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MONITORING_DIR="$(dirname "$SCRIPT_DIR")"

cd "$MONITORING_DIR"

echo "=========================================="
echo "Starting Prometheus Monitoring Stack"
echo "=========================================="

if ! command -v docker &> /dev/null; then
    echo "Error: Docker is not installed"
    exit 1
fi

if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo "Error: Docker Compose is not installed"
    exit 1
fi

if [ ! -f .env ]; then
    echo "Creating .env file from template..."
    cat > .env << 'EOF'
DB_USER=postgres
DB_PASSWORD=postgres
DB_HOST=localhost
DB_PORT=5432
DB_NAME=lrs
GRAFANA_PASSWORD=admin
EOF
    echo ".env file created. Please update it with your database credentials."
fi

echo ""
echo "Loading environment variables..."
source .env

echo ""
echo "Starting containers..."
if docker compose version &> /dev/null; then
    docker compose up -d
else
    docker-compose up -d
fi

echo ""
echo "Waiting for services to be ready..."
sleep 5

echo ""
echo "=========================================="
echo "Services Status"
echo "=========================================="
if docker compose version &> /dev/null; then
    docker compose ps
else
    docker-compose ps
fi

echo ""
echo "=========================================="
echo "Access URLs"
echo "=========================================="
echo "Prometheus:  http://localhost:9090"
echo "Grafana:     http://localhost:3000 (admin/admin)"
echo "Node Exp:    http://localhost:9100"
echo ""
echo "To check logs: docker compose logs -f [service]"
echo "To stop:       ./scripts/stop_monitoring.sh"
echo "=========================================="