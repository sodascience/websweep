import pytest

pytest.importorskip("aiohttp")

from websweep.crawler.crawler import Crawler, _registered_domain
from websweep.utils.utils import clean_url


@pytest.mark.parametrize(
    ("backend", "use_database"),
    [
        ("sqlite", True),
        ("csv", False),
    ],
)
def test_crawler_tracks_downloaded_domains_across_default_backends(
    tmp_path, backend, use_database
):
    crawler = Crawler(
        target_folder_path=tmp_path,
        target_temp_folder_path=tmp_path,
        save_html=False,
        extract=False,
        use_database=use_database,
        overview_backend=backend,
        min_days_between_crawls=30,
    )

    crawler._Crawler__update_overview_file(
        domain="example.com",
        level=0,
        url="https://example.com",
        identifier="example.com",
        status="200",
        path="",
    )

    downloaded = crawler._Crawler__get_downloaded_domains()
    assert clean_url("https://example.com") in downloaded


def test_crawler_tracks_downloaded_domains_with_duckdb(tmp_path):
    pytest.importorskip("duckdb")

    crawler = Crawler(
        target_folder_path=tmp_path,
        target_temp_folder_path=tmp_path,
        save_html=False,
        extract=False,
        use_database=True,
        overview_backend="duckdb",
        min_days_between_crawls=30,
    )

    crawler._Crawler__update_overview_file(
        domain="example.com",
        level=0,
        url="https://example.com",
        identifier="example.com",
        status="200",
        path="",
    )

    downloaded = crawler._Crawler__get_downloaded_domains()
    assert clean_url("https://example.com") in downloaded


def test_registered_domain_handles_multipart_suffix_with_psl():
    assert _registered_domain("https://foo.bar.example.co.uk/path") == "example.co.uk"
