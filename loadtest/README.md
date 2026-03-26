# bm-lrs Load Testing

Load testing suite for the bm-lrs REST API using k6.

## Overview

| Item | Details |
|------|---------|
| **Target Endpoint** | `POST /api/v1/calculate-mvalue` |
| **Tool** | k6 (Grafana) |
| **Goal** | Find maximum concurrent users the server can handle |

## Prerequisites

- k6 installed locally: `brew install k6` (macOS) or `sudo apt install k6` (Linux)
- Python 3.7+ with `pyarrow` for data generation
- SSH access to remote server for monitoring
- Docker on remote server for monitoring stack

## Quick Start

### 1. Generate Test Data

```bash
cd loadtest

# Create virtual environment and install dependencies
python3 -m venv .venv
source .venv/bin/activate
pip install pyarrow pandas

# Generate test data (500 samples by default)
python3 generate_test_data.py
```

### 2. Start Monitoring Stack (on remote server)

```bash
# SSH into remote server
ssh user@your-remote-server

# Navigate to monitoring directory
cd ~/loadtest/monitoring

# Copy and edit environment variables
cp .env.example .env
nano .env  # Edit DB credentials

# Start monitoring stack
./scripts/start_monitoring.sh
```

### 3. Run Tests

```bash
# Set base URL
export BASE_URL=http://your-remote-server:8080

# Run smoke test first (verifies system works)
k6 run smoke_test.js

# Run load test (find baseline capacity)
k6 run load_test.js

# Run stress test (find breaking point)
k6 run stress_test.js

# Run spike test (test recovery)
k6 run spike_test.js
```

## Test Scripts

| Script | Purpose | Duration |
|--------|---------|----------|
| `smoke_test.js` | Verify system works before load testing | ~30s |
| `load_test.js` | Find baseline capacity | ~5 min |
| `stress_test.js` | Find breaking point | ~10 min |
| `spike_test.js` | Test recovery from sudden load | ~3 min |

## Output Results

```bash
# Run tests with JSON output for analysis
k6 run load_test.js --out json=load_test_results.json

# Analyze results
python3 analyze_results.py load_test_results.json
```

## Monitoring

Access the monitoring stack at:
- **Grafana**: http://your-remote-server:3000 (admin/admin)
- **Prometheus**: http://your-remote-server:9090

### Remote SSH Monitoring

```bash
# Continuous monitoring
./monitor_remote.sh

# Single snapshot
./monitor_remote.sh --once
```

## Files

```
loadtest/
├── generate_test_data.py    # Extract sample LINKIDs from parquet
├── test_data.json           # Generated GeoJSON payloads
├── smoke_test.js            # Basic verification (1-5 users)
├── load_test.js             # Find baseline & max users
├── stress_test.js           # Push beyond limits
├── spike_test.js            # Sudden load changes
├── monitor_remote.sh        # SSH-based remote server monitoring
├── analyze_results.py       # Parse and correlate metrics
├── README.md                # This file
└── monitoring/              # Docker monitoring stack
    ├── docker-compose.yml
    ├── prometheus/
    ├── grafana/
    └── scripts/
```
