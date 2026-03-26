# Load Test Results - 2026-03-26

## Test Environment

| Component | Details |
|-----------|---------|
| **Remote Server** | <REMOTE_SERVER>:3013 |
| **API Endpoint** | http://localhost:8080/api/v1/calculate-mvalue (via SSH tunnel) |
| **Test Tool** | k6 v1.7.0 |
| **Test Data** | 500 unique LINKIDs from rni_2_2025_original.parquet |
| **Monitoring** | Grafana at <REMOTE_SERVER>:3000 |

## Smoke Test Results

**Date:** 2026-03-26  
**Duration:** 30 seconds  
**VUs:** 1 → 5 ramp-up

### Summary

| Metric | Value |
|--------|-------|
| **Total Requests** | 71 |
| **Requests/sec** | 2.23 |
| **Failed Requests** | 0 (0%) |
| **Checks Passed** | 100% |

### Latency

| Percentile | Latency |
|------------|---------|
| **p50 (median)** | 1.17s |
| **p90** | 1.55s |
| **p95** | 1.56s |
| **p99** | 1.66s |
| **min** | 557ms |
| **max** | 1.75s |

### HTTP Metrics

| Metric | Value |
|--------|-------|
| **http_req_duration avg** | 1.15s |
| **http_req_failed** | 0% |
| **data_received** | 34 kB |
| **data_sent** | 23 kB |

## Monitoring Stack

### Services Running

| Service | Port | Status |
|---------|------|--------|
| Prometheus | 9090 | ✅ Running |
| Grafana | 3000 | ✅ Running |
| Node Exporter | 9100 | ✅ Running |
| Postgres Exporter | 9187 | ✅ Running |

### Prometheus Targets

| Target | Status |
|--------|--------|
| node-exporter (172.19.0.5:9100) | UP |
| postgres-exporter (172.19.0.3:9187) | UP |
| prometheus (localhost:9090) | UP |
| bm-lrs-api (host.docker.internal:8080) | DOWN (expected - no /metrics endpoint) |

## Fixes Applied

1. **Prometheus DNS issue**: Changed from Docker DNS names to IP addresses in prometheus.yml
2. **Grafana dashboard JSON**: Fixed typo `" "w": 8,` → `"w": 8,`
3. **k6 query params**: Changed from `searchParams` option to URL-embedded query params

## Issues Found

1. **k6 searchParams bug**: k6's `searchParams` option wasn't working correctly with POST requests - query params were ignored. Fixed by embedding query params directly in URL.

## Next Steps

1. Run **load_test.js** - Ramp 5 → 50 → 100 VUs (~5 minutes)
2. Run **stress_test.js** - Ramp up to 500 VUs (~10 minutes)
3. Run **spike_test.js** - Sudden load changes to test recovery

## Load Test Results

**Date:** 2026-03-26  
**Duration:** 5 minutes 30 seconds  
**VUs:** 5 → 50 → 100 ramp-up

### Summary

| Metric | Value |
|--------|-------|
| **Total Requests** | 591 |
| **Requests/sec** | 1.79 |
| **Failed Requests** | 12 (2.03%) |
| **Checks Passed** | 97.96% |
| **Max VUs** | 99 |

### Latency

| Percentile | Latency |
|------------|---------|
| **p50 (median)** | 23.03s |
| **p90** | 40.57s |
| **p95** | 49.13s ⚠️ |
| **p99** | 60s (timeout) ⚠️ |
| **min** | 1.57s |
| **max** | 60s (timeout) |

### Thresholds Status

| Threshold | Target | Actual | Status |
|-----------|--------|--------|--------|
| p(95) < 500ms | < 500ms | 49.13s | ❌ FAILED |
| p(99) < 1000ms | < 1000ms | 60s | ❌ FAILED |
| http_req_failed < 1% | < 1% | 2.03% | ❌ FAILED |

### Analysis

1. **Saturation Point**: Around 90-100 VUs, the server started timing out
2. **Error Type**: All failures were "request timeout" errors
3. **Graceful Degradation**: Server did not crash, but became unresponsive under load
4. **Recovery**: After test ended, server recovered (SSH tunnel still active)

### Recommendations

1. **Add more server capacity** - Current setup cannot handle 100 concurrent users
2. **Implement request queuing** - Add backlog handling
3. **Add caching** - Cache frequent query results
4. **Database optimization** - Connection pooling, query optimization
5. **Horizontal scaling** - Add more server instances behind load balancer
6. **Increase timeout** - Current timeout may be too short for complex LRS calculations

## Notes

- Server CPU usage was around 15-20% during smoke test
- API response times are ~1s (acceptable for LRS calculation)
- All requests returned 200 OK with valid GeoJSON responses
- Load test revealed server cannot handle high concurrency - requests time out at ~90+ VUs
- The server's LRS calculation is CPU-intensive, causing bottlenecks under load
