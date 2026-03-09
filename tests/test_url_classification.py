from websweep.utils.utils import classify_url, set_regex


def test_default_classification_blocks_pdf():
    args = set_regex()
    assert classify_url("https://example.com/report.pdf", 1, *args) is False


def test_allow_extensions_overrides_default_blocklist():
    args = set_regex(allow_extensions="pdf,png")
    assert classify_url("https://example.com/report.pdf", 1, *args) is True
    assert classify_url("https://example.com/figure.png", 1, *args) is True


def test_level_two_stays_restrictive_for_non_matching_html():
    args = set_regex()
    assert classify_url("https://example.com/random-page", 2, *args) is False
