from pathlib import Path

from websweep.main import _has_crawled_data


def test_has_crawled_data_true_for_target_folder(tmp_path):
    target = tmp_path / "target"
    crawled = target / "crawled_data"
    crawled.mkdir(parents=True, exist_ok=True)
    (crawled / "example.com.zip").write_text("zip", encoding="utf-8")

    assert _has_crawled_data(target_folder=target, storage_path=None) is True


def test_has_crawled_data_true_for_storage_path(tmp_path):
    target = tmp_path / "target"
    storage = tmp_path / "storage"
    (target / "crawled_data").mkdir(parents=True, exist_ok=True)
    crawled_storage = storage / "crawled_data"
    crawled_storage.mkdir(parents=True, exist_ok=True)
    (crawled_storage / "example.com.zip").write_text("zip", encoding="utf-8")

    assert _has_crawled_data(target_folder=target, storage_path=storage) is True


def test_has_crawled_data_false_when_no_artifacts(tmp_path):
    target = tmp_path / "target"
    storage = tmp_path / "storage"
    (target / "crawled_data").mkdir(parents=True, exist_ok=True)
    (storage / "crawled_data").mkdir(parents=True, exist_ok=True)

    assert _has_crawled_data(target_folder=target, storage_path=storage) is False
