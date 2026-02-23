import datetime

import pytest

from websweep.crawler.crawler import Crawler


def _seed_overview_rows(crawler: Crawler) -> None:
    crawler._Crawler__update_overview_file(
        domain="ok.example",
        level=0,
        url="https://ok.example",
        identifier="ok-id",
        status="200",
        path="",
    )
    crawler._Crawler__update_overview_file(
        domain="failed.example",
        level=0,
        url="https://failed.example",
        identifier="failed-id",
        status="500",
        path="",
    )
    crawler._Crawler__update_overview_file(
        domain="nf.example",
        level=0,
        url="https://nf.example",
        identifier="nf-id",
        status="Website not found",
        path="",
    )
    crawler._Crawler__update_overview_file(
        domain="child.example",
        level=1,
        url="https://failed.example/about",
        identifier="failed-id",
        status="404",
        path="",
    )


@pytest.mark.parametrize(
    ("backend", "use_database"),
    [
        ("csv", False),
        ("sqlite", True),
    ],
)
def test_crawl_complement_recrawls_failed_level0_urls_only(tmp_path, backend, use_database):
    crawler = Crawler(
        target_folder_path=tmp_path,
        target_temp_folder_path=tmp_path,
        save_html=False,
        extract=False,
        use_database=use_database,
        overview_backend=backend,
    )

    _seed_overview_rows(crawler)

    captured = {}

    def _capture_urls(urls):
        captured["urls"] = list(urls)

    crawler.crawl_base_urls = _capture_urls  # type: ignore[assignment]

    complement_date = datetime.date.fromisoformat(crawler.crawler_session_date)
    crawler.crawl_complement_base_urls(complement_date)

    assert "urls" in captured
    # Only failed level-0 URLs should be retried.
    assert sorted(captured["urls"]) == sorted(
        [
            ("https://failed.example", "failed-id"),
            ("https://nf.example", "nf-id"),
        ]
    )
    # Ensure tuple layout stays (url, identifier).
    assert all(url.startswith("http") for url, _ in captured["urls"])


def test_crawl_complement_with_duckdb(tmp_path):
    pytest.importorskip("duckdb")

    crawler = Crawler(
        target_folder_path=tmp_path,
        target_temp_folder_path=tmp_path,
        save_html=False,
        extract=False,
        use_database=True,
        overview_backend="duckdb",
    )
    _seed_overview_rows(crawler)

    captured = {}

    def _capture_urls(urls):
        captured["urls"] = list(urls)

    crawler.crawl_base_urls = _capture_urls  # type: ignore[assignment]

    complement_date = datetime.date.fromisoformat(crawler.crawler_session_date)
    crawler.crawl_complement_base_urls(complement_date)

    assert sorted(captured["urls"]) == sorted(
        [
            ("https://failed.example", "failed-id"),
            ("https://nf.example", "nf-id"),
        ]
    )
