"""Sync the featured example notebook into the Sphinx source tree."""

from pathlib import Path
import shutil


ROOT_DIR = Path(__file__).resolve().parents[2]
NOTEBOOK_SRC = ROOT_DIR / "examples" / "example_scraper_extractor.ipynb"
NOTEBOOK_DST = ROOT_DIR / "docs" / "source" / "example_scraper_extractor.ipynb"


def main() -> None:
    if not NOTEBOOK_SRC.exists():
        raise FileNotFoundError(
            f"Missing featured notebook source file: {NOTEBOOK_SRC}"
        )
    NOTEBOOK_DST.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(NOTEBOOK_SRC, NOTEBOOK_DST)


if __name__ == "__main__":
    main()
