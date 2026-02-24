from pathlib import Path

from websweep.main import _load_file_extractor_class
from websweep.extractor.extractor import FileExtractor


def _write_module(path: Path, source: str) -> None:
    path.write_text(source, encoding="utf-8")


def test_loader_returns_none_when_no_path():
    assert _load_file_extractor_class(None) is None


def test_loader_returns_none_for_missing_file(tmp_path):
    missing = tmp_path / "missing_addon.py"
    assert _load_file_extractor_class(missing) is None


def test_loader_returns_custom_file_extractor_subclass(tmp_path):
    addon_file = tmp_path / "addon.py"
    _write_module(
        addon_file,
        (
            "from websweep.extractor.extractor import FileExtractor\n\n"
            "class CustomFaxExtractor(FileExtractor):\n"
            "    pass\n"
        ),
    )

    extractor_cls = _load_file_extractor_class(addon_file)
    assert extractor_cls is not None
    assert issubclass(extractor_cls, FileExtractor)
    assert extractor_cls.__name__ == "CustomFaxExtractor"


def test_loader_requires_single_subclass(tmp_path):
    addon_file = tmp_path / "addon_many.py"
    _write_module(
        addon_file,
        (
            "from websweep.extractor.extractor import FileExtractor\n\n"
            "class FirstExtractor(FileExtractor):\n"
            "    pass\n\n"
            "class SecondExtractor(FileExtractor):\n"
            "    pass\n"
        ),
    )

    assert _load_file_extractor_class(addon_file) is None


def test_loader_requires_subclass_presence(tmp_path):
    addon_file = tmp_path / "addon_invalid.py"
    _write_module(
        addon_file,
        "class NotAnExtractor:\n    pass\n",
    )

    assert _load_file_extractor_class(addon_file) is None
