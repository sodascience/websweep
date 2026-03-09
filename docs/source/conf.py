import sys
from importlib.metadata import PackageNotFoundError, version as metadata_version
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


project = "WebSweep"
copyright = "2026, ODISSEI Social Data Science"
author = "ODISSEI Social Data Science"
try:
    release = metadata_version("websweep")
except PackageNotFoundError:
    release = "0.0.0"
version = ".".join(release.split(".")[:2])

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "nbsphinx",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "**.ipynb_checkpoints"]
autodoc_member_order = "bysource"
autodoc_typehints = "description"
napoleon_google_docstring = True
napoleon_numpy_docstring = True
nbsphinx_execute = "never"

html_theme = "sphinx_rtd_theme"
html_theme_options = {
    "navigation_depth": 3,
    "collapse_navigation": False,
    "style_external_links": True,
    "style_nav_header_background": "#1f2937",
}
html_context = {
    "display_github": True,
    "github_user": "sodascience",
    "github_repo": "websweep",
    "github_version": "main",
    "conf_py_path": "/docs/source/",
}
html_sidebars = {
    "**": [
        "about.html",
        "navigation.html",
        "relations.html",
        "searchbox.html",
    ]
}
html_static_path = []
