from websweep.utils.source_urls import read_source_urls


def test_read_source_urls_with_identifier_column(tmp_path):
    source = tmp_path / "urls.csv"
    source.write_text(
        "url,identifier\nhttps://example.com,example\nhttps://example.org,org\n",
        encoding="utf-8",
    )

    urls = read_source_urls(source)
    assert urls == [
        ("https://example.com", "example"),
        ("https://example.org", "org"),
    ]


def test_read_source_urls_with_only_url_column(tmp_path):
    source = tmp_path / "urls.csv"
    source.write_text(
        "url\nhttps://example.com\nhttps://example.org\n",
        encoding="utf-8",
    )

    urls = read_source_urls(source)
    assert urls == [
        ("https://example.com", None),
        ("https://example.org", None),
    ]


def test_read_source_urls_accepts_website_header(tmp_path):
    source = tmp_path / "urls.csv"
    source.write_text(
        "website,id\nhttps://example.com,ex\n\nhttps://example.org,\n",
        encoding="utf-8",
    )

    urls = read_source_urls(source)
    assert urls == [
        ("https://example.com", "ex"),
        ("https://example.org", None),
    ]


def test_read_source_urls_detects_tsv_and_filters_level_zero(tmp_path):
    source = tmp_path / "overview.tsv"
    source.write_text(
        "domain\tidentifier\tlevel\turl\tstatus\n"
        "a.nl\tid-a\t0\thttps://a.nl\t200\n"
        "a.nl\tid-a\t1\thttps://a.nl/about\t200\n"
        "b.nl\tid-b\t0\thttps://b.nl\t500\n",
        encoding="utf-8",
    )

    urls = read_source_urls(source)
    assert urls == [
        ("https://a.nl", "id-a"),
        ("https://b.nl", "id-b"),
    ]


def test_read_source_urls_normalizes_and_skips_non_web_entries(tmp_path):
    source = tmp_path / "urls.csv"
    source.write_text(
        "url,identifier\n"
        "example.com,a\n"
        "mailto:person@example.com,b\n"
        "tel:+31000000000,c\n"
        "https://example.com#section,a\n",
        encoding="utf-8",
    )

    urls = read_source_urls(source)
    assert urls == [("https://example.com", "a")]


def test_read_source_urls_keeps_same_url_for_different_identifiers(tmp_path):
    source = tmp_path / "urls.csv"
    source.write_text(
        "url,identifier\n"
        "https://example.com,id-1\n"
        "https://example.com,id-2\n",
        encoding="utf-8",
    )

    urls = read_source_urls(source)
    assert urls == [
        ("https://example.com", "id-1"),
        ("https://example.com", "id-2"),
    ]
