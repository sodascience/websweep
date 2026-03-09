# WebSweep Docs Workflow

## Stack

- Sphinx config: `docs/source/conf.py`
- Build entry points: `Makefile`, `make.bat`
- Read the Docs config: `.readthedocs.yml`

## Local build

From repository root:

```bash
make docs
```

This runs:

1. `sphinx-apidoc` against `src/websweep`
2. HTML build into `docs/build/html`

## CI docs build

Workflow: `.github/workflows/docs.yml`

Trigger paths:

- `src/**`
- `docs/source/**`
- `examples/**`
- `README.md`
- `Makefile`
- `make.bat`
- `pyproject.toml`
- `.github/workflows/docs.yml`

Workflow behavior:

- syncs docs dependencies with `uv`
- validates notebook example integrity
- builds docs
- uploads `docs/build/html` artifact
- uploads `examples/example_scraper_extractor.ipynb` artifact

## Read the Docs

RTD uses `.readthedocs.yml` and installs:

- package from repository root
- docs extra dependencies

No local absolute paths should appear in docs pages.
