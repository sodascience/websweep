"""Post-process sphinx-apidoc output to avoid duplicate autodoc symbols."""

from pathlib import Path
import re


SOURCE_DIR = Path(__file__).resolve().parents[1] / "source"

# Remove the auto-generated "Module contents" section, which duplicates
# symbols already documented in explicit submodule sections.
MODULE_CONTENTS_BLOCK = re.compile(
    r"\nModule contents\n-+\n\n\.\. automodule:: [^\n]+\n(?:   :[^\n]+\n)*",
    flags=re.MULTILINE,
)
UNDOC_MEMBERS_LINE = re.compile(r"^   :undoc-members:\n", flags=re.MULTILINE)


def _flatten_top_package_heading(text: str) -> str:
    """Flatten the top-level API page title and section scaffolding."""
    text = text.replace(
        "websweep package\n================\n",
        "websweep\n========\n",
        1,
    )
    text = text.replace("\nSubpackages\n-----------\n\n", "\n", 1)
    text = text.replace("\nSubmodules\n----------\n\n", "\n", 1)
    return text


def main() -> None:
    for rst_path in SOURCE_DIR.glob("websweep*.rst"):
        original = rst_path.read_text(encoding="utf-8")
        updated = MODULE_CONTENTS_BLOCK.sub("\n", original)
        updated = UNDOC_MEMBERS_LINE.sub("", updated)
        if rst_path.name == "websweep.rst":
            updated = _flatten_top_package_heading(updated)
        if updated != original:
            rst_path.write_text(updated, encoding="utf-8")


if __name__ == "__main__":
    main()
