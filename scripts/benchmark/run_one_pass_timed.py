#!/usr/bin/env python3
"""Run WebSweep crawl+extract for a fixed wall-clock duration."""

from __future__ import annotations

import argparse
import json
import signal
import time
from pathlib import Path

from websweep.crawler.crawler import Crawler
from websweep.utils.source_urls import read_source_urls


class _CycleTimeout(Exception):
    """Raised when a cycle exceeds the remaining benchmark budget."""


def _run_single_cycle(
    source_file: Path,
    cycle_out: Path,
    backend: str,
    max_level: int,
    max_pages_per_domain: int,
    source_limit: int | None,
) -> None:
    urls = read_source_urls(source_file)
    if source_limit is not None and source_limit > 0:
        urls = urls[:source_limit]

    cycle_out.mkdir(parents=True, exist_ok=True)
    Crawler(
        target_folder_path=cycle_out,
        target_temp_folder_path=cycle_out,
        extract=True,
        save_html=False,
        overview_backend=backend,
        max_level=max_level,
        max_pages_per_domain=max_pages_per_domain,
    ).crawl_base_urls(urls)


def _alarm_handler(_signum, _frame):
    raise _CycleTimeout()


def run_timed(args: argparse.Namespace) -> dict:
    source_file = Path(args.source_file).resolve()
    out = Path(args.output_dir).resolve()
    cycles_dir = out / "cycles"
    cycles_dir.mkdir(parents=True, exist_ok=True)

    start_epoch = time.time()
    deadline = start_epoch + float(args.duration_seconds)
    start_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(start_epoch))

    cycle_summaries = []
    cycle_idx = 1
    terminated_any = False
    force_killed_any = False

    while time.time() < deadline:
        cycle_out = cycles_dir / f"cycle_{cycle_idx:03d}"
        cycle_out.mkdir(parents=True, exist_ok=True)
        cycle_start = time.time()
        cycle_start_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(cycle_start))

        terminated = False
        exitcode = 0
        remaining = max(1, int(deadline - time.time()))

        prev_handler = signal.getsignal(signal.SIGALRM)
        signal.signal(signal.SIGALRM, _alarm_handler)
        signal.alarm(remaining)
        try:
            _run_single_cycle(
                source_file=source_file,
                cycle_out=cycle_out,
                backend=args.backend,
                max_level=args.max_level,
                max_pages_per_domain=args.max_pages_per_domain,
                source_limit=args.source_limit,
            )
        except _CycleTimeout:
            terminated = True
            terminated_any = True
            exitcode = -15
        except Exception:
            exitcode = 1
            raise
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, prev_handler)

        cycle_end = time.time()
        cycle_end_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(cycle_end))
        cycle_summaries.append(
            {
                "cycle": cycle_idx,
                "cycle_output_dir": str(cycle_out),
                "start_utc": cycle_start_iso,
                "end_utc": cycle_end_iso,
                "duration_seconds": round(cycle_end - cycle_start, 3),
                "terminated_at_deadline": terminated,
                "force_killed": False,
                "exitcode": exitcode,
            }
        )

        cycle_idx += 1
        if not args.restart_until_deadline:
            break

    end_epoch = time.time()
    end_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(end_epoch))

    meta = {
        "backend": args.backend,
        "source_file": str(source_file),
        "output_dir": str(out),
        "duration_seconds_requested": int(args.duration_seconds),
        "duration_seconds_actual": round(end_epoch - start_epoch, 3),
        "start_utc": start_iso,
        "end_utc": end_iso,
        "max_level": args.max_level,
        "max_pages_per_domain": args.max_pages_per_domain,
        "source_limit": args.source_limit,
        "restart_until_deadline": bool(args.restart_until_deadline),
        "terminated_any_cycle_at_deadline": terminated_any,
        "force_killed_any_cycle": force_killed_any,
        "cycles_run": len(cycle_summaries),
        "cycles": cycle_summaries,
    }

    meta_path = out / "benchmark_meta.json"
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return meta


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run WebSweep benchmark with fixed duration")
    parser.add_argument("--source-file", required=True, help="Source URL file (.csv/.tsv) with url column")
    parser.add_argument("--output-dir", required=True, help="Output directory for this run")
    parser.add_argument("--backend", required=True, choices=["csv", "duckdb", "sqlite"], help="Overview backend")
    parser.add_argument("--duration-seconds", type=int, default=3600, help="Hard runtime limit in seconds")
    parser.add_argument("--max-level", type=int, default=3, help="Crawler max level")
    parser.add_argument(
        "--max-pages-per-domain",
        type=int,
        default=50,
        help="Max pages per domain",
    )
    parser.add_argument(
        "--source-limit",
        type=int,
        default=None,
        help="Optional limit for number of base URLs (for quick tests)",
    )
    parser.add_argument(
        "--restart-until-deadline",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Restart crawl cycles until the full duration elapses.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    meta = run_timed(args)
    print(json.dumps(meta, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
