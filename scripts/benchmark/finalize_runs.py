#!/usr/bin/env python3
"""Run consolidation on benchmark outputs and summarize counts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def _count_lines(path: Path) -> int:
    count = 0
    with path.open("rb") as f:
        for _ in f:
            count += 1
    return count


def consolidate_run(run_dir: Path) -> dict:
    from websweep.consolidator.consolidator import Consolidator

    extracted_dir = run_dir / "extracted_data"
    files = sorted(extracted_dir.glob("*.ndjson"))
    if not files:
        return {
            "run_dir": str(run_dir.resolve()),
            "input_file": None,
            "output_file": None,
            "input_lines": 0,
            "output_lines": 0,
            "status": "no_extracted_files",
        }

    input_file = files[-1]
    out_dir = run_dir / "consolidated_data"
    out_dir.mkdir(parents=True, exist_ok=True)
    output_file = out_dir / "consolidated.ndjson"

    Consolidator(str(input_file)).consolidate(str(output_file))

    return {
        "run_dir": str(run_dir.resolve()),
        "input_file": str(input_file.resolve()),
        "output_file": str(output_file.resolve()),
        "input_lines": _count_lines(input_file),
        "output_lines": _count_lines(output_file),
        "status": "ok",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Consolidate benchmark runs")
    parser.add_argument("--run-dir", action="append", required=True, help="Run directory (repeatable)")
    parser.add_argument("--output-json", required=True)
    args = parser.parse_args()

    results = [consolidate_run(Path(rd)) for rd in args.run_dir]

    output_json = Path(args.output_json)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(json.dumps(results, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
