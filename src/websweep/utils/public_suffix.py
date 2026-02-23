"""Utilities for loading and refreshing the public suffix list (PSL)."""

import os
import shutil
import tempfile
import time
import urllib.request
from pathlib import Path

PSL_GITHUB_URL = (
    "https://raw.githubusercontent.com/publicsuffix/list/main/public_suffix_list.dat"
)
PSL_PACKAGED_PATH = Path(__file__).with_name("public_suffix_list.dat")
PSL_RUNTIME_PATH = Path(
    os.environ.get(
        "WEBSWEEP_PSL_PATH",
        str(Path(tempfile.gettempdir()) / "websweep_public_suffix_list.dat"),
    )
)
PSL_MAX_AGE_SECONDS = int(
    os.environ.get("WEBSWEEP_PSL_MAX_AGE_SECONDS", str(7 * 24 * 60 * 60))
)
PSL_TIMEOUT_SECONDS = float(os.environ.get("WEBSWEEP_PSL_TIMEOUT_SECONDS", "3.0"))


def _env_bool(name: str, default: bool) -> bool:
    """Parse a boolean-like environment variable with a default fallback."""
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off"}


def _psl_data_valid(data: bytes) -> bool:
    """Basic integrity check for a downloaded public suffix list payload."""
    return b"BEGIN ICANN DOMAINS" in data and b"END PRIVATE DOMAINS" in data


def _should_update(path: Path) -> bool:
    """Return whether the runtime PSL file is missing or stale."""
    if not path.exists():
        return True
    return (time.time() - path.stat().st_mtime) > PSL_MAX_AGE_SECONDS


def _write_bytes_atomically(path: Path, data: bytes) -> None:
    """Write bytes atomically by replacing from a sidecar temporary file."""
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_bytes(data)
    tmp_path.replace(path)


def _copy_packaged_psl_if_needed(path: Path) -> None:
    """Seed the runtime PSL path with the packaged list when needed."""
    if path.exists() or not PSL_PACKAGED_PATH.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(PSL_PACKAGED_PATH, path)


def _update_runtime_psl(path: Path) -> None:
    """Download and persist the latest PSL from GitHub when valid."""
    req = urllib.request.Request(
        PSL_GITHUB_URL,
        headers={"User-Agent": "WebSweep/1.0 (+https://github.com/odissei-data/websweep)"},
    )
    with urllib.request.urlopen(req, timeout=PSL_TIMEOUT_SECONDS) as response:  # nosec B310
        data = response.read()
    if _psl_data_valid(data):
        path.parent.mkdir(parents=True, exist_ok=True)
        _write_bytes_atomically(path, data)


def ensure_public_suffix_list() -> Path:
    """Return a local PSL path, updating it from GitHub when configured."""
    runtime_path = PSL_RUNTIME_PATH
    _copy_packaged_psl_if_needed(runtime_path)

    if _env_bool("WEBSWEEP_PSL_AUTO_UPDATE", True) and _should_update(runtime_path):
        try:
            _update_runtime_psl(runtime_path)
        except Exception:
            pass

    if runtime_path.exists():
        return runtime_path
    return PSL_PACKAGED_PATH


def build_tldextract_extractor(tldextract_module):
    """Build a configured TLDExtract instance backed by the local PSL file."""
    cache_dir = Path(
        os.environ.get(
            "WEBSWEEP_TLDEXTRACT_CACHE",
            str(Path(tempfile.gettempdir()) / "websweep_tldextract_cache"),
        )
    )
    cache_dir.mkdir(parents=True, exist_ok=True)

    psl_path = ensure_public_suffix_list().resolve()
    try:
        return tldextract_module.TLDExtract(
            suffix_list_urls=(psl_path.as_uri(),),
            cache_dir=str(cache_dir),
        )
    except Exception:
        # Fall back to library defaults if file URL parsing fails unexpectedly.
        return tldextract_module.TLDExtract(
            suffix_list_urls=None,
            cache_dir=str(cache_dir),
        )
