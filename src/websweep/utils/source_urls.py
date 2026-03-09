import csv
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse

NON_WEB_SCHEMES = ("mailto:", "mail:", "tel:", "javascript:")


def _detect_delimiter(header_line: str) -> str:
    """Infer TSV vs CSV from the first header line."""
    return "\t" if "\t" in header_line else ","


def _pick_key(fieldnames: List[str], candidates: List[str]) -> Optional[str]:
    """Return the first matching column name from a list of normalized candidates."""
    lowered: Dict[str, str] = {
        name.strip().lower(): name for name in fieldnames if name
    }
    for candidate in candidates:
        if candidate in lowered:
            return lowered[candidate]
    return None


def _normalize_source_url(raw_url: str) -> Optional[str]:
    """Normalize one source URL to a crawlable http(s) URL or return ``None``."""
    url = str(raw_url or "").strip()
    if not url:
        return None

    lowered = url.lower()
    if lowered.startswith(NON_WEB_SCHEMES):
        return None

    parsed = urlparse(url)
    if not parsed.scheme:
        url = "https://" + url.lstrip("/")
        parsed = urlparse(url)

    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        return None

    # Keep URL stable for deduplication by removing fragment-only noise.
    parsed = parsed._replace(fragment="")
    return urlunparse(parsed)


def read_source_urls(source_file_path: Path) -> List[Tuple[str, Optional[str]]]:
    """
    Parse a source CSV/TSV and return URLs with optional identifiers.

    Supported headers:
    - url / website / domain
    - identifier / id (optional)

    Input hygiene:
    - auto-detects CSV vs TSV delimiters
    - keeps only level-0 rows when a `level` column exists
    - normalizes URLs and skips non-web schemes
    - removes exact duplicate (url, identifier) pairs while preserving order
    """
    rows: List[Tuple[str, Optional[str]]] = []
    seen = set()

    with source_file_path.open("r", encoding="utf-8", newline="") as handle:
        header_line = handle.readline()
        handle.seek(0)

        delimiter = _detect_delimiter(header_line)
        reader = csv.DictReader(handle, delimiter=delimiter)
        fieldnames = list(reader.fieldnames or [])
        if not fieldnames:
            return rows

        url_key = _pick_key(fieldnames, ["url", "website", "domain"]) or fieldnames[0]
        id_key = _pick_key(fieldnames, ["identifier", "id"])
        level_key = _pick_key(fieldnames, ["level"])

        for raw_row in reader:
            row = raw_row or {}
            if level_key is not None:
                level = str(row.get(level_key) or "").strip()
                if level and level != "0":
                    continue

            url = _normalize_source_url(row.get(url_key))
            if not url:
                continue

            identifier = (row.get(id_key) or "").strip() if id_key else ""
            if identifier.upper() == "NULL":
                identifier = ""

            dedupe_key = (url, identifier)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            rows.append((url, identifier or None))

    return rows
