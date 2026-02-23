#!/usr/bin/env python3
"""Single-run benchmark worker: one crawl+extract pass."""

from __future__ import annotations

import argparse
import os
import asyncio
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

from source_loader import load_urls


def _normalize_probe_url(raw_url: str) -> Optional[str]:
    url = str(raw_url).strip()
    if not url:
        return None
    parsed = urlparse(url)
    if parsed.scheme in {"http", "https"}:
        return url
    if parsed.scheme:
        return None
    return "https://" + url.lstrip("/")


def _warmup_network(urls, max_candidates: int = 20, debug: bool = False) -> bool:
    """
    Warm up DNS/network resolution for this worker process.

    In this environment, a short single-request warmup reduces false
    DNS-resolution failures at crawl startup.
    """
    try:
        import aiohttp
    except Exception:
        return False

    probe_urls = ["https://example.com"]
    for record in urls[:max_candidates]:
        raw = record[0] if isinstance(record, (tuple, list)) else str(record)
        candidate = _normalize_probe_url(raw)
        if candidate is not None and candidate not in probe_urls:
            probe_urls.append(candidate)

    if not probe_urls:
        return False

    async def _run_probe():
        errors = []
        timeout = aiohttp.ClientTimeout(total=20, sock_connect=10, sock_read=10)
        async with aiohttp.ClientSession(timeout=timeout, trust_env=True) as session:
            for url in probe_urls:
                try:
                    async with session.get(url):
                        return True
                except Exception as exc:
                    if debug:
                        errors.append((url, repr(exc)))
                    continue
        if debug and errors:
            print("DEBUG warmup probe errors:")
            for url, err in errors[:5]:
                print(" -", url, err)
        return False

    try:
        return bool(asyncio.run(_run_probe()))
    except Exception:
        return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one WebSweep crawl+extract pass")
    parser.add_argument("--source-file", required=True, help="Source URL file (.csv/.tsv)")
    parser.add_argument("--output-dir", required=True, help="Output directory for this pass")
    parser.add_argument("--backend", required=True, choices=["csv", "duckdb", "sqlite"])
    parser.add_argument("--max-level", type=int, default=3)
    parser.add_argument("--max-pages-per-domain", type=int, default=50)
    parser.add_argument("--source-limit", type=int, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    from websweep.crawler.crawler import Crawler
    if os.environ.get("WEBSWEEP_DEBUG") == "1":
        import websweep
        import websweep.crawler.crawler as crawler_module
        print("DEBUG websweep module:", websweep.__file__)
        print("DEBUG Extractor module:", crawler_module.Extractor.__module__)
        print("DEBUG clean_url module:", crawler_module.clean_url.__module__)

    urls = load_urls(Path(args.source_file))
    if args.source_limit is not None and args.source_limit > 0:
        urls = urls[: args.source_limit]

    debug_mode = os.environ.get("WEBSWEEP_DEBUG") == "1"
    warmup_ok = _warmup_network(urls, debug=debug_mode)
    if os.environ.get("WEBSWEEP_DEBUG") == "1":
        print("DEBUG warmup_ok:", warmup_ok)

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    crawler = Crawler(
        target_folder_path=out,
        target_temp_folder_path=out,
        extract=True,
        save_html=False,
        overview_backend=args.backend,
        max_level=args.max_level,
        max_pages_per_domain=args.max_pages_per_domain,
    )
    crawler.crawl_base_urls(urls)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
