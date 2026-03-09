from websweep.main import _has_crawled_data


def test_has_crawled_data_true_for_target_folder(tmp_path):
    target = tmp_path / "target"
    crawled = target / "crawled_data"
    crawled.mkdir(parents=True, exist_ok=True)
    (crawled / "example.com.zip").write_text("zip", encoding="utf-8")

    assert _has_crawled_data(target_folder=target) is True


def test_has_crawled_data_false_when_no_artifacts(tmp_path):
    target = tmp_path / "target"
    (target / "crawled_data").mkdir(parents=True, exist_ok=True)

    assert _has_crawled_data(target_folder=target) is False
