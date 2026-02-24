import pytest
from types import MethodType

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


def test_adaptive_domain_wait_backoff_and_decay(tmp_path):
    crawler = Crawler(
        target_folder_path=tmp_path,
        target_temp_folder_path=tmp_path,
        save_html=False,
        extract=False,
        use_database=False,
        overview_backend="csv",
    )

    key = "example.com"
    crawler._update_domain_wait(key, "429", retry_after_seconds=None)
    first_wait = crawler.waits[key]
    assert first_wait >= crawler.domain_wait_on_429
    assert first_wait <= crawler.max_domain_wait

    crawler._update_domain_wait(key, "200", retry_after_seconds=None)
    assert crawler.waits[key] < first_wait

    crawler._update_domain_wait(key, "429", retry_after_seconds=999)
    assert crawler.waits[key] == crawler.max_domain_wait


def test_parse_retry_after_seconds_numeric(tmp_path):
    crawler = Crawler(
        target_folder_path=tmp_path,
        target_temp_folder_path=tmp_path,
        save_html=False,
        extract=False,
        use_database=False,
        overview_backend="csv",
    )
    assert crawler._parse_retry_after_seconds("7") == 7.0
    assert crawler._parse_retry_after_seconds("") is None
    assert crawler._parse_retry_after_seconds(None) is None


def test_duckdb_batched_writer_keeps_unique_rows(tmp_path):
    pytest.importorskip("duckdb")
    import duckdb

    crawler = Crawler(
        target_folder_path=tmp_path,
        target_temp_folder_path=tmp_path,
        save_html=False,
        extract=False,
        use_database=True,
        overview_backend="duckdb",
        min_days_between_crawls=30,
    )
    crawler._duckdb_batch_size = 2
    crawler._open_duckdb_overview_writer()
    try:
        for _ in range(3):
            crawler._Crawler__update_overview_file(
                domain="example.com",
                level=0,
                url="https://example.com",
                identifier="example.com",
                status="200",
                path="",
            )
    finally:
        crawler._close_duckdb_overview_writer()

    con = duckdb.connect(str(tmp_path / "overview_urls.duckdb"), read_only=True)
    try:
        total_rows = con.execute("SELECT count(*) FROM Overview").fetchone()[0]
    finally:
        con.close()
    # DuckDB default is append-only for speed/size; dedupe can be enabled explicitly.
    assert total_rows == 3

    downloaded = crawler._Crawler__get_downloaded_domains()
    assert downloaded == {clean_url("https://example.com")}


class _AllowAllRobots:
    def can_fetch(self, *_args, **_kwargs):
        return True


def test_duckdb_crawl_uses_persistent_writer_and_closes_it(tmp_path):
    pytest.importorskip("duckdb")

    crawler = Crawler(
        target_folder_path=tmp_path,
        target_temp_folder_path=tmp_path,
        save_html=False,
        extract=False,
        use_database=True,
        overview_backend="duckdb",
        max_level=1,
    )
    crawler._duckdb_batch_size = 3

    async def _fake_test_domain_robots(self, url, domain, identifier):
        return url, _registered_domain(url), _AllowAllRobots()

    async def _fake_fetch_one_url_wrapped(self, domain, url, identifier, level, session, state_key=None):
        return [], "200", "", None

    crawler._Crawler__test_domain_robots = MethodType(_fake_test_domain_robots, crawler)
    crawler._Crawler__fetch_one_url_wrapped = MethodType(_fake_fetch_one_url_wrapped, crawler)

    urls = [
        ("https://site-a.example/path", "id-a"),
        ("https://site-b.example/path", "id-b"),
        ("https://site-c.example/path", "id-c"),
        ("https://site-d.example/path", "id-d"),
        ("https://site-e.example/path", "id-e"),
    ]
    crawler.crawl_base_urls(urls)

    # Writer connection is scoped to crawl run and must be closed afterwards.
    assert crawler._duckdb_connection is None

    downloaded = crawler._Crawler__get_downloaded_domains()
    assert downloaded == {
        clean_url("https://site-a.example/path"),
        clean_url("https://site-b.example/path"),
        clean_url("https://site-c.example/path"),
        clean_url("https://site-d.example/path"),
        clean_url("https://site-e.example/path"),
    }


def test_duckdb_deduplicate_option_preserves_unique_behavior(tmp_path):
    pytest.importorskip("duckdb")
    import duckdb

    crawler = Crawler(
        target_folder_path=tmp_path,
        target_temp_folder_path=tmp_path,
        save_html=False,
        extract=False,
        use_database=True,
        overview_backend="duckdb",
        duckdb_deduplicate=True,
    )
    crawler._duckdb_batch_size = 10
    crawler._open_duckdb_overview_writer()
    try:
        for _ in range(3):
            crawler._Crawler__update_overview_file(
                domain="example.com",
                level=0,
                url="https://example.com",
                identifier="example.com",
                status="200",
                path="",
            )
    finally:
        crawler._close_duckdb_overview_writer()

    con = duckdb.connect(str(tmp_path / "overview_urls.duckdb"), read_only=True)
    try:
        total_rows = con.execute("SELECT count(*) FROM Overview").fetchone()[0]
    finally:
        con.close()
    assert total_rows == 1


def test_archive_domain_folder_moves_zip_to_storage_path(tmp_path):
    fast_root = tmp_path / "fast"
    storage_root = tmp_path / "archive"
    crawler = Crawler(
        target_folder_path=fast_root,
        target_temp_folder_path=fast_root,
        save_html=True,
        extract=False,
        use_database=False,
        overview_backend="csv",
        storage_path=storage_root,
    )

    domain = "example.com"
    page_file = (
        fast_root
        / "crawled_data"
        / domain
        / domain
        / "2026-02-24"
        / "index"
    )
    page_file.parent.mkdir(parents=True, exist_ok=True)
    page_file.write_text("<html>ok</html>", encoding="utf-8")

    crawler._archive_domain_folder_sync(domain)

    assert not (fast_root / "crawled_data" / domain / domain / "2026-02-24" / "index").exists()
    assert not (fast_root / "crawled_data" / f"{domain}.zip").exists()
    assert (storage_root / "crawled_data" / f"{domain}.zip").exists()


def test_overview_file_stays_in_instance_folder_when_temp_folder_differs(tmp_path):
    instance_root = tmp_path / "instance"
    temp_root = tmp_path / "temp"
    crawler = Crawler(
        target_folder_path=instance_root,
        target_temp_folder_path=temp_root,
        save_html=False,
        extract=False,
        use_database=False,
        overview_backend="csv",
    )

    assert crawler.overview_path == str(instance_root / "overview_urls.tsv")
    assert (instance_root / "overview_urls.tsv").exists()
    assert not (temp_root / "overview_urls.tsv").exists()
