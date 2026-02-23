import json
from pathlib import Path
from typing import Iterable

try:
    import orjson
except Exception:
    orjson = None


def json_dumps(obj) -> bytes:
    """Serialize an object to UTF-8 JSON bytes (orjson when available)."""
    if orjson is not None:
        return orjson.dumps(obj)
    return json.dumps(obj, ensure_ascii=False).encode("utf-8")


def json_loads(value):
    """Parse JSON from bytes or text using the active JSON backend."""
    if orjson is not None:
        return orjson.loads(value)
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    return json.loads(value)


def append_jsonl(path, records: Iterable[dict]) -> None:
    """Append dictionaries to a NDJSON file, one JSON object per line."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("ab") as handle:
        for row in records:
            handle.write(json_dumps(row) + b"\n")
