#!/usr/bin/env python3
"""Analyze WebSweep benchmark runs (csv vs duckdb overview backends)."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean, median
from typing import Dict, List, Optional


def _parse_dt(value: str) -> Optional[datetime]:
    value = (value or "").strip()
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def _minute_bucket(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M")


def _ten_minute_bucket(dt: datetime) -> str:
    bucket_minute = (dt.minute // 10) * 10
    return dt.replace(minute=bucket_minute, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M")


def _is_success(status: str) -> bool:
    s = (status or "").strip()
    if not s:
        return False
    if s.isdigit():
        return int(s) == 200
    try:
        return int(float(s)) == 200
    except Exception:
        return False


def _status_code(status: str) -> Optional[int]:
    s = (status or "").strip()
    if not s:
        return None
    if s.isdigit():
        return int(s)
    try:
        return int(float(s))
    except Exception:
        return None


def _categorize_failure(status: str, path: str) -> str:
    s = (status or "").strip().lower()
    p = (path or "").strip().lower()

    code = _status_code(status)
    if code is not None:
        if code == 403:
            return "blocked_403"
        if code == 429:
            return "blocked_429"
        if 500 <= code <= 599:
            return "server_5xx"
        if 400 <= code <= 499:
            return "client_4xx_other"
        if code in {-8, -9}:
            return "timeout"

    text = f"{s} {p}"
    if "website not found" in text or "name or service not known" in text or "nodename nor servname" in text:
        return "dns_or_not_found"
    if "timeout" in text or "timed out" in text:
        return "timeout"
    if "ssl" in text or "certificate" in text:
        return "ssl"
    if "forbidden" in text or "too many requests" in text or "denied" in text or "captcha" in text:
        return "blocked_generic"
    if not s:
        return "empty_status"
    return "other"


def _load_rows_single(run_dir: Path) -> List[Dict[str, str]]:
    duckdb_path = run_dir / "overview_urls.duckdb"
    sqlite_path = run_dir / "overview_urls.db"
    tsv_path = run_dir / "overview_urls.tsv"

    if duckdb_path.exists():
        import duckdb

        con = duckdb.connect(str(duckdb_path))
        try:
            data = con.execute(
                "SELECT domain, identifier, level, url, status, session_date, crawl_date, path FROM Overview"
            ).fetchall()
        finally:
            con.close()

        rows = [
            {
                "domain": str(r[0] or ""),
                "identifier": str(r[1] or ""),
                "level": str(r[2] or ""),
                "url": str(r[3] or ""),
                "status": str(r[4] or ""),
                "session_date": str(r[5] or ""),
                "crawl_date": str(r[6] or ""),
                "path": str(r[7] or ""),
            }
            for r in data
        ]
        return rows

    if sqlite_path.exists():
        import sqlite3

        con = sqlite3.connect(str(sqlite_path))
        cur = con.cursor()
        try:
            data = cur.execute(
                "SELECT domain, identifier, level, url, status, session_date, crawl_date, path FROM Overview"
            ).fetchall()
        finally:
            con.close()

        rows = [
            {
                "domain": str(r[0] or ""),
                "identifier": str(r[1] or ""),
                "level": str(r[2] or ""),
                "url": str(r[3] or ""),
                "status": str(r[4] or ""),
                "session_date": str(r[5] or ""),
                "crawl_date": str(r[6] or ""),
                "path": str(r[7] or ""),
            }
            for r in data
        ]
        return rows

    if tsv_path.exists():
        rows: List[Dict[str, str]] = []
        with tsv_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                row = row or {}
                rows.append(
                    {
                        "domain": str(row.get("domain") or ""),
                        "identifier": str(row.get("identifier") or ""),
                        "level": str(row.get("level") or ""),
                        "url": str(row.get("url") or ""),
                        "status": str(row.get("status") or ""),
                        "session_date": str(row.get("session_date") or ""),
                        # Backward compatibility with historical header names.
                        "crawl_date": str(row.get("crawl_date") or row.get("scrape_date") or ""),
                        "path": str(row.get("path") or ""),
                    }
                )
        return rows

    raise FileNotFoundError(f"No overview file found in {run_dir}")


def _load_rows(run_dir: Path) -> List[Dict[str, str]]:
    cycles_dir = run_dir / "cycles"
    if cycles_dir.exists() and cycles_dir.is_dir():
        rows: List[Dict[str, str]] = []
        for cycle_dir in sorted(cycles_dir.iterdir()):
            if not cycle_dir.is_dir():
                continue
            try:
                cycle_rows = _load_rows_single(cycle_dir)
            except FileNotFoundError:
                continue
            for row in cycle_rows:
                row["__cycle"] = cycle_dir.name
            rows.extend(cycle_rows)
        if rows:
            return rows

    return _load_rows_single(run_dir)


def _linear_slope(values: List[float]) -> float:
    n = len(values)
    if n < 2:
        return 0.0
    xs = list(range(n))
    x_mean = mean(xs)
    y_mean = mean(values)
    num = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, values))
    den = sum((x - x_mean) ** 2 for x in xs)
    if den == 0:
        return 0.0
    return num / den


def analyze_run(run_dir: Path) -> Dict:
    rows = _load_rows(run_dir)
    meta_path = run_dir / "benchmark_meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}

    total = len(rows)
    success = 0
    failed = 0
    status_counter = Counter()
    fail_category_counter = Counter()

    per_minute = defaultdict(lambda: {"total": 0, "success": 0, "failed": 0, "fail_categories": Counter()})
    per_ten_min = defaultdict(lambda: {"total": 0, "success": 0, "failed": 0, "statuses": Counter()})

    timestamps: List[datetime] = []

    for row in rows:
        status = row.get("status", "")
        path = row.get("path", "")
        status_counter[status] += 1

        ok = _is_success(status)
        if ok:
            success += 1
        else:
            failed += 1
            fail_category = _categorize_failure(status, path)
            fail_category_counter[fail_category] += 1

        dt = _parse_dt(row.get("crawl_date", ""))
        if dt is None:
            continue
        timestamps.append(dt)
        minute = _minute_bucket(dt)
        bucket = per_minute[minute]
        bucket["total"] += 1
        if ok:
            bucket["success"] += 1
        else:
            bucket["failed"] += 1
            bucket["fail_categories"][fail_category] += 1

        ten_min = _ten_minute_bucket(dt)
        bucket_10 = per_ten_min[ten_min]
        bucket_10["total"] += 1
        bucket_10["statuses"][status] += 1
        if ok:
            bucket_10["success"] += 1
        else:
            bucket_10["failed"] += 1

    minute_keys = sorted(per_minute.keys())
    ppm = [per_minute[m]["total"] for m in minute_keys]
    minute_fail_rate = [
        (per_minute[m]["failed"] / per_minute[m]["total"]) if per_minute[m]["total"] else 0.0
        for m in minute_keys
    ]

    if ppm:
        q = max(1, len(ppm) // 4)
        first_q_ppm = ppm[:q]
        last_q_ppm = ppm[-q:]
        first_q_fail = minute_fail_rate[:q]
        last_q_fail = minute_fail_rate[-q:]
        first_q_ppm_avg = mean(first_q_ppm)
        last_q_ppm_avg = mean(last_q_ppm)
        first_q_fail_avg = mean(first_q_fail)
        last_q_fail_avg = mean(last_q_fail)
    else:
        first_q_ppm_avg = last_q_ppm_avg = 0.0
        first_q_fail_avg = last_q_fail_avg = 0.0

    startup_seconds = None
    if timestamps and meta.get("start_utc"):
        try:
            # start_utc stored in Zulu format
            start = datetime.strptime(meta["start_utc"], "%Y-%m-%dT%H:%M:%SZ")
            first_ts = min(timestamps)
            diff = (first_ts - start).total_seconds()
            # Guard against date-only timestamps being parsed as midnight.
            if abs(diff) <= 1800:
                startup_seconds = max(0.0, diff)
            else:
                startup_seconds = None
        except Exception:
            startup_seconds = None

    trend_slope = _linear_slope([float(x) for x in ppm])
    drop_ratio = (last_q_ppm_avg / first_q_ppm_avg) if first_q_ppm_avg > 0 else None

    strong_decrease = False
    if drop_ratio is not None and drop_ratio < 0.7:
        strong_decrease = True

    result = {
        "run_dir": str(run_dir.resolve()),
        "meta": meta,
        "total_rows": total,
        "success_rows": success,
        "failed_rows": failed,
        "success_rate": (success / total) if total else 0.0,
        "error_rate": (failed / total) if total else 0.0,
        "minutes_with_activity": len(minute_keys),
        "pages_per_minute": {
            "mean": mean(ppm) if ppm else 0.0,
            "median": median(ppm) if ppm else 0.0,
            "min": min(ppm) if ppm else 0,
            "max": max(ppm) if ppm else 0,
            "trend_slope_pages_per_minute_index": trend_slope,
        },
        "startup_delay_seconds": startup_seconds,
        "first_quartile": {
            "avg_pages_per_minute": first_q_ppm_avg,
            "avg_failure_rate": first_q_fail_avg,
        },
        "last_quartile": {
            "avg_pages_per_minute": last_q_ppm_avg,
            "avg_failure_rate": last_q_fail_avg,
        },
        "throughput_drop_ratio_last_over_first": drop_ratio,
        "strong_decrease_detected": strong_decrease,
        "top_statuses": status_counter.most_common(15),
        "top_failure_categories": fail_category_counter.most_common(15),
        "ten_minute_series": [
            {
                "interval_start": interval,
                "attempted": per_ten_min[interval]["total"],
                "success": per_ten_min[interval]["success"],
                "failed": per_ten_min[interval]["failed"],
                "error_rate": (
                    per_ten_min[interval]["failed"] / per_ten_min[interval]["total"]
                    if per_ten_min[interval]["total"]
                    else 0.0
                ),
                "top_statuses": per_ten_min[interval]["statuses"].most_common(5),
            }
            for interval in sorted(per_ten_min.keys())
        ],
        "minute_series": [
            {
                "minute": m,
                "total": per_minute[m]["total"],
                "success": per_minute[m]["success"],
                "failed": per_minute[m]["failed"],
                "failure_rate": (
                    per_minute[m]["failed"] / per_minute[m]["total"]
                    if per_minute[m]["total"]
                    else 0.0
                ),
            }
            for m in minute_keys
        ],
    }

    return result


def _fmt_pct(x: float) -> str:
    return f"{x * 100:.2f}%"


def _fmt_num(x: float) -> str:
    return f"{x:.2f}"


def build_markdown_report(csv_res: Dict, duckdb_res: Dict) -> str:
    lines: List[str] = []
    lines.append("# WebSweep Backend Benchmark Report")
    lines.append("")
    lines.append("## Summary")

    def row(label: str, a, b) -> None:
        lines.append(f"- {label}: csv={a}, duckdb={b}")

    row("Total pages recorded", csv_res["total_rows"], duckdb_res["total_rows"])
    row("Success rate", _fmt_pct(csv_res["success_rate"]), _fmt_pct(duckdb_res["success_rate"]))
    row("Error rate", _fmt_pct(csv_res["error_rate"]), _fmt_pct(duckdb_res["error_rate"]))
    row(
        "Mean pages/min",
        _fmt_num(csv_res["pages_per_minute"]["mean"]),
        _fmt_num(duckdb_res["pages_per_minute"]["mean"]),
    )
    row(
        "Median pages/min",
        _fmt_num(csv_res["pages_per_minute"]["median"]),
        _fmt_num(duckdb_res["pages_per_minute"]["median"]),
    )
    row(
        "Startup delay (s)",
        _fmt_num(csv_res["startup_delay_seconds"] or 0.0),
        _fmt_num(duckdb_res["startup_delay_seconds"] or 0.0),
    )
    row(
        "Throughput drop ratio (last/first quartile)",
        _fmt_num(csv_res["throughput_drop_ratio_last_over_first"] or 0.0),
        _fmt_num(duckdb_res["throughput_drop_ratio_last_over_first"] or 0.0),
    )
    lines.append("")

    for label, result in (("csv", csv_res), ("duckdb", duckdb_res)):
        lines.append(f"## Details: {label}")
        lines.append("")
        lines.append("### Top statuses")
        for status, cnt in result["top_statuses"][:10]:
            lines.append(f"- {status!r}: {cnt}")
        lines.append("")
        lines.append("### Top failure categories")
        for cat, cnt in result["top_failure_categories"][:10]:
            lines.append(f"- {cat}: {cnt}")
        lines.append("")
        lines.append("### Error Rate Per 10-Min Interval")
        for interval in result["ten_minute_series"]:
            lines.append(
                "- "
                + f"{interval['interval_start']}: "
                + f"attempted={interval['attempted']}, "
                + f"failed={interval['failed']}, "
                + f"error_rate={_fmt_pct(interval['error_rate'])}"
            )
        lines.append("")

    lines.append("## Interpretation")
    lines.append(
        "- Use the backend with higher sustained pages/min and stable failure rate."
    )
    lines.append(
        "- High `blocked_403`/`blocked_429` suggests anti-bot throttling; reduce per-domain pressure and add adaptive waits."
    )
    lines.append(
        "- High `dns_or_not_found` reflects dead domains in source list rather than crawler defects."
    )

    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze csv vs duckdb WebSweep runs")
    parser.add_argument("--csv-run-dir", required=True)
    parser.add_argument("--duckdb-run-dir", required=True)
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--output-md", required=True)
    args = parser.parse_args()

    csv_res = analyze_run(Path(args.csv_run_dir))
    duckdb_res = analyze_run(Path(args.duckdb_run_dir))

    combined = {
        "csv": csv_res,
        "duckdb": duckdb_res,
    }

    output_json = Path(args.output_json)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(combined, indent=2), encoding="utf-8")

    report = build_markdown_report(csv_res, duckdb_res)
    output_md = Path(args.output_md)
    output_md.parent.mkdir(parents=True, exist_ok=True)
    output_md.write_text(report, encoding="utf-8")

    print(f"wrote {output_json}")
    print(f"wrote {output_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
