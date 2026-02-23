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


def main() -> None:
    for rst_path in SOURCE_DIR.glob("websweep*.rst"):
        original = rst_path.read_text(encoding="utf-8")
        updated = MODULE_CONTENTS_BLOCK.sub("\n", original)
        updated = UNDOC_MEMBERS_LINE.sub("", updated)
        if updated != original:
            rst_path.write_text(updated, encoding="utf-8")


if __name__ == "__main__":
    main()
