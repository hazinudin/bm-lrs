#!/usr/bin/env python3
import json
import sys
from datetime import datetime
from typing import Dict, List, Any, Optional

try:
    import pandas as pd
except ImportError:
    print("Warning: pandas not installed. Run: pip install pandas")
    pd = None


def parse_k6_json(json_path: str) -> Optional[Dict[str, Any]]:
    try:
        with open(json_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: File not found: {json_path}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in {json_path}: {e}")
        return None


def analyze_k6_results(data: Dict[str, Any]) -> Dict[str, Any]:
    metrics = {
        "total_requests": 0,
        "failed_requests": 0,
        "error_rate": 0.0,
        "latency_p50": 0.0,
        "latency_p95": 0.0,
        "latency_p99": 0.0,
        "latency_avg": 0.0,
        "min_duration": float("inf"),
        "max_duration": 0.0,
        "trend": [],
    }

    if pd is None:
        return analyze_k6_json(data, metrics)

    thresholds = data.get("thresholds", {})
    metrics_data = data.get("metrics", {})

    http_req_duration = metrics_data.get("http_req_duration", {})
    values = http_req_duration.get("values", {})

    metrics["latency_p50"] = values.get("p(50)", 0)
    metrics["latency_p95"] = values.get("p(95)", 0)
    metrics["latency_p99"] = values.get("p(99)", 0)
    metrics["latency_avg"] = values.get("avg", 0)
    metrics["min_duration"] = values.get("min", 0)
    metrics["max_duration"] = values.get("max", 0)

    http_reqs = metrics_data.get("http_reqs", {})
    metrics["total_requests"] = http_reqs.get("values", {}).get("count", 0)

    http_req_failed = metrics_data.get("http_req_failed", {})
    fail_rate = http_req_failed.get("values", {}).get("rate", 0)
    metrics["error_rate"] = fail_rate * 100

    checks = metrics_data.get("checks", {})
    checks_passed = checks.get("values", {}).get("passes", 0)
    checks_failed = checks.get("values", {}).get("fails", 0)

    vus_data = metrics_data.get("vus", {})
    metrics["avg_vus"] = vus_data.get("values", {}).get("avg", 0)
    metrics["max_vus"] = vus_data.get("values", {}).get("max", 0)

    vu_states = metrics_data.get("vus_states", {})
    metrics["vu_states"] = vu_states.get("values", {})

    return metrics


def analyze_k6_json(data: Dict[str, Any], metrics: Dict[str, Any]) -> Dict[str, Any]:
    metrics_data = data.get("metrics", {})

    http_req_duration = metrics_data.get("http_req_duration", {})
    values = http_req_duration.get("values", {})

    metrics["latency_p50"] = values.get("p(50)", 0)
    metrics["latency_p95"] = values.get("p(95)", 0)
    metrics["latency_p99"] = values.get("p(99)", 0)
    metrics["latency_avg"] = values.get("avg", 0)

    http_reqs = metrics_data.get("http_reqs", {})
    metrics["total_requests"] = http_reqs.get("values", {}).get("count", 0)

    return metrics


def correlate_with_server_metrics(
    k6_results: Dict[str, Any], server_metrics: Dict[str, float]
) -> Dict[str, Any]:
    correlation = {
        "findings": [],
        "bottleneck_indicators": [],
        "recommendations": [],
    }

    if k6_results["error_rate"] > 5:
        correlation["findings"].append(
            f"High error rate: {k6_results['error_rate']:.2f}%"
        )
        correlation["bottleneck_indicators"].append("error_rate")

    if k6_results["latency_p95"] > 500:
        correlation["findings"].append(
            f"High p95 latency: {k6_results['latency_p95']:.2f}ms"
        )
        if server_metrics.get("cpu_usage", 0) > 70:
            correlation["recommendations"].append(
                "CPU saturation detected - consider scaling horizontally"
            )

    if server_metrics.get("memory_usage", 0) > 80:
        correlation["findings"].append(
            f"High memory usage: {server_metrics['memory_usage']:.1f}%"
        )
        correlation["bottleneck_indicators"].append("memory")

    if server_metrics.get("disk_iowait", 0) > 20:
        correlation["findings"].append(
            f"High disk I/O wait: {server_metrics['disk_iowait']:.1f}%"
        )
        correlation["bottleneck_indicators"].append("disk_io")

    if not correlation["findings"]:
        correlation["findings"].append("No significant bottlenecks detected")

    if not correlation["recommendations"]:
        correlation["recommendations"].append(
            "System performing within normal parameters"
        )

    return correlation


def print_report(
    test_name: str,
    k6_results: Dict[str, Any],
    correlation: Optional[Dict[str, Any]] = None,
):
    print("=" * 60)
    print(f"Load Test Analysis Report: {test_name}")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    print("\n--- k6 Metrics ---")
    print(f"  Total Requests:     {k6_results['total_requests']:,}")
    print(f"  Error Rate:         {k6_results['error_rate']:.2f}%")
    print(f"  Latency (avg):      {k6_results['latency_avg']:.2f}ms")
    print(f"  Latency (p50):      {k6_results['latency_p50']:.2f}ms")
    print(f"  Latency (p95):      {k6_results['latency_p95']:.2f}ms")
    print(f"  Latency (p99):      {k6_results['latency_p99']:.2f}ms")

    if "avg_vus" in k6_results:
        print(f"  Avg Virtual Users:  {k6_results['avg_vus']:.0f}")
        print(f"  Max Virtual Users:  {k6_results['max_vus']:.0f}")

    if correlation:
        print("\n--- Correlation Analysis ---")
        for finding in correlation["findings"]:
            print(f"  - {finding}")

        if correlation["bottleneck_indicators"]:
            print(
                f"\n  Bottleneck Indicators: {', '.join(correlation['bottleneck_indicators'])}"
            )

        if correlation["recommendations"]:
            print("\n--- Recommendations ---")
            for rec in correlation["recommendations"]:
                print(f"  - {rec}")

    print("\n" + "=" * 60)


def main():
    if len(sys.argv) < 2:
        print(
            "Usage: python3 analyze_results.py <k6-results.json> [server-metrics.json]"
        )
        print("")
        print(
            "  k6-results.json     - JSON output from k6 (k6 run --out json=results.json)"
        )
        print(
            "  server-metrics.json - Optional: JSON with server metrics for correlation"
        )
        print("")
        print("Example:")
        print("  k6 run --out json=load_test_results.json load_test.js")
        print("  python3 analyze_results.py load_test_results.json")
        sys.exit(1)

    k6_file = sys.argv[1]
    server_metrics_file = sys.argv[2] if len(sys.argv) > 2 else None

    test_name = (
        k6_file.replace(".json", "").replace("_results", "").replace("-", " ").title()
    )

    k6_data = parse_k6_json(k6_file)
    if k6_data is None:
        sys.exit(1)

    k6_results = analyze_k6_results(k6_data)

    correlation = None
    if server_metrics_file:
        server_data = parse_k6_json(server_metrics_file)
        if server_data:
            correlation = correlate_with_server_metrics(k6_results, server_data)

    print_report(test_name, k6_results, correlation)

    output_report = k6_file.replace(".json", "_analysis.json")
    with open(output_report, "w") as f:
        json.dump(
            {
                "test_name": test_name,
                "k6_results": k6_results,
                "correlation": correlation,
                "generated_at": datetime.now().isoformat(),
            },
            f,
            indent=2,
        )
    print(f"\nDetailed report saved to: {output_report}")


if __name__ == "__main__":
    main()
