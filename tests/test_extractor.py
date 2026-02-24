from pathlib import Path
import time
import zipfile

import pytest

pytest.importorskip("bs4")

from websweep.extractor.extractor import Extractor, FileExtractor
try:
    import re2 as re
except Exception:
    import regex as re


def _sample_info():
    base = Path(__file__).resolve().parent / "assets" / "crawled_data"
    archived_page = (
        base
        / "aaschroefpalen.nl"
        / "aaschroefpalen.nl"
        / "2023-06-20"
        / "aaschroefpalen.nl"
    )
    return [
        "https://aaschroefpalen.nl",
        "00000000",
        0,
        "https://aaschroefpalen.nl",
        "2023-06-12 20:53:59",
        str(archived_page),
    ]


def test_file_extractor_default_fields_do_not_include_contact_data():
    unit = FileExtractor(_sample_info())
    metadata = unit.extracting()

    assert set(metadata["zipcode"]) == {"6581 KZ"}
    assert set(metadata["address"]) == {"Steiger 10"}
    assert "phone" not in metadata
    assert "email" not in metadata
    assert "fax" not in metadata


class FaxAddonExtractor(FileExtractor):
    def _extract_fax(self) -> list:
        pattern = re.compile(
            r"(?is)\b(?:faxnumber|fax|f)\b[^0-9\+]{0,12}"
            r"([\+]?[0-9][0-9\-\s\(\)]{7,20})\b"
        )
        faxs = set(re.findall(pattern, str(self.soup)))
        return list(set([_.strip() for _ in faxs]))


def test_file_extractor_custom_addon_method_extracts_fax():
    unit = FaxAddonExtractor(_sample_info())
    metadata = unit.extracting()
    assert "024 - 378 07 77" in set(metadata["fax"])


class SlowExtractor(FileExtractor):
    def extracting(self):
        time.sleep(2.0)
        return super().extracting()


def test_process_level_timeout_marks_timeout(tmp_path):
    extractor = Extractor(
        target_folder_path=tmp_path,
        workers=1,
        extract_timeout_seconds=1,
        file_extractor=SlowExtractor,
    )
    result = list(extractor._iter_chunk_results([_sample_info()]))
    assert len(result) == 1
    assert result[0]["path"] == "Timeout extracting"


def test_firmbackbone_addon_outside_core_package():
    from addons.firmbackbone_extractor import FirmBackBoneFileExtractor

    unit = FirmBackBoneFileExtractor(_sample_info())
    metadata = unit.extracting()
    assert "email" in metadata
    assert "phone" in metadata


def test_file_extractor_resolves_zip_from_archive_roots(tmp_path):
    archive_root = tmp_path / "archive"
    zip_dir = archive_root / "crawled_data"
    zip_dir.mkdir(parents=True, exist_ok=True)
    zip_path = zip_dir / "example.com.zip"
    member = "example.com/2026-02-24/index"
    html = "<html><body><p>Archive fallback works</p></body></html>"

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_LZMA, allowZip64=True) as zf:
        zf.writestr(member, html)

    missing_path = (
        tmp_path
        / "missing_base"
        / "crawled_data"
        / "example.com"
        / "example.com"
        / "2026-02-24"
        / "index"
    )
    info = [
        "example.com",
        "id-1",
        0,
        "https://example.com",
        "2026-02-24",
        str(missing_path),
    ]

    unit = FileExtractor(info, zip_search_roots=[archive_root])
    metadata = unit.extracting()
    assert "Archive fallback works" in metadata["text"]
