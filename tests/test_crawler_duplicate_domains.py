import csv
from pathlib import Path
from types import MethodType

from websweep.crawler.crawler import Crawler, _registered_domain


class _AllowAllRobots:
    def can_fetch(self, *_args, **_kwargs):
        return True


def test_duplicate_domain_base_urls_do_not_collide_in_state(tmp_path):
    crawler = Crawler(
        target_folder_path=tmp_path,
        target_temp_folder_path=tmp_path,
        save_html=False,
        extract=False,
        use_database=False,
        overview_backend="csv",
        max_level=1,
    )

    async def _fake_test_domain_robots(self, url, domain, identifier):
        return url, _registered_domain(url), _AllowAllRobots()

    async def _fake_fetch_one_url_wrapped(self, domain, url, identifier, level, session, state_key=None):
        return [], "200", ""

    crawler._Crawler__test_domain_robots = MethodType(_fake_test_domain_robots, crawler)
    crawler._Crawler__fetch_one_url_wrapped = MethodType(_fake_fetch_one_url_wrapped, crawler)

    urls = [
        ("https://shared.example/path-a", "id-a"),
        ("https://shared.example/path-b", "id-b"),
    ]
    crawler.crawl_base_urls(urls)

    overview_path = Path(tmp_path) / "overview_urls.tsv"
    with overview_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle, delimiter="\t"))

    assert len(rows) == 2
    assert all(row["status"] == "200" for row in rows)
