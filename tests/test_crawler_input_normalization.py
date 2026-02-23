from websweep.crawler.crawler import _normalize_base_url


def test_normalize_base_url_adds_https_when_missing():
    assert _normalize_base_url("example.com/path") == "https://example.com/path"


def test_normalize_base_url_keeps_http_and_https():
    assert _normalize_base_url("http://example.com") == "http://example.com"
    assert _normalize_base_url("https://example.com") == "https://example.com"


def test_normalize_base_url_skips_non_web_schemes():
    assert _normalize_base_url("mailto:user@example.com") is None
    assert _normalize_base_url("mail:user@example.com") is None
    assert _normalize_base_url("tel:+123456789") is None
    assert _normalize_base_url("javascript:void(0)") is None
