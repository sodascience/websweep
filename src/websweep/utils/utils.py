try:
    import regex as re
except Exception:
    import re
from typing import Iterable, Set
from pathlib import Path
from urllib.parse import urlparse
import json


def create_regex_pattern(keywords, regex):
    """Build a case-insensitive regex from literal keywords and raw regex text."""
    keywords = keywords or []
    regex = regex or ""
    keywords = [keyword.replace(" ", r".*").lower().strip() for keyword in keywords]
    keywords = [keyword for keyword in keywords if keyword]
    regex = regex.strip()

    if keywords and regex != "":
        regex += "|" + "|".join(keywords)
    elif regex == "":
        regex = "|".join(keywords)

    # Compile a never-match regex when no patterns were provided.
    if not regex:
        regex = r"$^"

    return re.compile(regex, re.IGNORECASE)


def _normalize_extension(ext: str):
    """Normalize an extension token to lowercase without a leading dot."""
    ext = str(ext).strip().lower()
    if not ext:
        return None
    if ext.startswith("."):
        ext = ext[1:]
    return ext


def _parse_extensions(values) -> Set[str]:
    """Parse extension values from strings or iterables into a normalized set."""
    if values is None:
        return set()

    if isinstance(values, str):
        raw_values = values.split(",")
    elif isinstance(values, Iterable):
        raw_values = []
        for value in values:
            if isinstance(value, str) and "," in value:
                raw_values.extend(value.split(","))
            else:
                raw_values.append(value)
    else:
        raw_values = [values]

    parsed = set()
    for value in raw_values:
        normalized = _normalize_extension(value)
        if normalized is not None:
            parsed.add(normalized)
    return parsed


def set_regex(classification_file_path=None, allow_extensions=None, block_extensions=None):
    """Load URL classification rules and return compiled regex/extension filters."""
    url_regex_mail = re.compile(r"^mailto:|^tel:", re.IGNORECASE)

    # Load the default regex expressions
    if classification_file_path is None:
        classification_file_path = Path(__file__).with_name("default_regex.json")

    with open(classification_file_path, "r") as file:
        content = file.read()
    default_regex_data = json.loads(content)

    negative_cfg = default_regex_data.get("negative", {})
    url_cfg = default_regex_data.get("url", {})
    files_cfg = default_regex_data.get("files", {})

    # Regex to not download
    negative_regex = create_regex_pattern(
        negative_cfg.get("negative_keywords", []),
        negative_cfg.get("negative_regex", ""),
    )

    # Only download sometimes
    url_regex = create_regex_pattern(
        url_cfg.get("url_keywords", []),
        url_cfg.get("url_regex", ""),
    )

    blocked_extensions = _parse_extensions(files_cfg.get("blocked_extensions", []))
    allowed_extensions = _parse_extensions(files_cfg.get("allowed_extensions", []))

    blocked_extensions.update(_parse_extensions(block_extensions))
    allowed_extensions.update(_parse_extensions(allow_extensions))
    blocked_extensions -= allowed_extensions

    return (
        url_regex_mail,
        negative_regex,
        url_regex,
        allowed_extensions,
        blocked_extensions,
    )


def _url_extension(url: str):
    """Return the lowercase file extension for a URL path without the dot."""
    path = urlparse(url).path
    suffix = Path(path).suffix.lower()
    if suffix.startswith("."):
        suffix = suffix[1:]
    return suffix


def classify_url(
    url,
    level,
    url_regex_mail,
    negative_regex,
    url_regex,
    allowed_extensions=None,
    blocked_extensions=None,
) -> bool:
    """Return whether a URL should be crawled for the given crawl depth."""

    if level == 0:
        return True

    # Taking the path (next step) will remove this part, we need to catch it before
    if re.search(url_regex_mail, url):
        return False

    allowed_extensions = allowed_extensions or set()
    blocked_extensions = blocked_extensions or set()
    ext = _url_extension(url)
    if ext:
        if ext in allowed_extensions:
            return True
        if ext in blocked_extensions:
            return False

    # Classify by path tokens only so domain names do not trigger false negatives.
    url = urlparse(url).path

    # Don't download these.
    if re.search(negative_regex, url):
        return False

    # Crawl all level-1 pages and only selected level-2 pages.
    if level == 1:
        # Drop likely anti-bot or ID-only paths (e.g. /553-504).
        if re.search("^[^a-zA-Z]+$", url):
            return False
        else:
            return True
    if level == 2:
        # Keep only important
        if re.search(url_regex, url):
            return True
        else:
            return False
    else:
        return False


def clean_url(url):
    """Strip scheme and ``www.`` prefix for lightweight URL normalization."""
    return re.sub(r"(https?://)?(www\.)?", "", url)
