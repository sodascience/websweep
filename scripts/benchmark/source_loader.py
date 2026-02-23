#!/usr/bin/env python3
"""Helpers for loading source URL files used in benchmarks."""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Tuple

from websweep.utils.source_urls import read_source_urls

def load_urls(source_path: Path) -> List[Tuple[str, Optional[str]]]:
    """
    Load URL rows from a CSV/TSV source file.

    Uses the same source parsing logic as the library/CLI.
    """
    return read_source_urls(source_path)
