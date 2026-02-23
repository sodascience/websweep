from pathlib import Path
from typing import Optional


def detect_existing_overview_backend(base_folder: Path) -> Optional[str]:
    """Detect an existing overview store in ``base_folder``."""
    if (base_folder / "overview_urls.duckdb").exists():
        return "duckdb"
    if (base_folder / "overview_urls.db").exists():
        return "sqlite"
    if (base_folder / "overview_urls.tsv").exists():
        return "csv"
    return None


def duckdb_available() -> bool:
    """Return ``True`` when the optional ``duckdb`` dependency can be imported."""
    try:
        import duckdb  # noqa: F401
        return True
    except Exception:
        return False


def resolve_overview_backend(
    base_folder: Path,
    use_database: bool,
    override_backend: Optional[str],
    urls_count: Optional[int] = None,
) -> str:
    """Resolve which overview backend to use (duckdb/sqlite/csv)."""
    if override_backend is not None:
        normalized = str(override_backend).lower().strip()
        if normalized == "tsv":
            normalized = "csv"
        if normalized not in {"duckdb", "sqlite", "csv"}:
            raise ValueError("overview_backend must be one of: duckdb, sqlite, csv/tsv")
        return normalized

    detected_backend = detect_existing_overview_backend(base_folder)
    if detected_backend is not None:
        return detected_backend

    if not use_database:
        return "csv"

    if urls_count is not None and urls_count > 10000:
        return "duckdb" if duckdb_available() else "sqlite"

    # Default DB preference is duckdb (with sqlite fallback).
    return "duckdb" if duckdb_available() else "sqlite"
