# WebSweep

[![CI](https://github.com/sodascience/websweep/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/sodascience/websweep/actions/workflows/ci.yml)
[![Docs](https://readthedocs.org/projects/websweep/badge/?version=latest)](https://websweep.readthedocs.io/en/latest/)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.18980899.svg)](https://doi.org/10.5281/zenodo.18980899)
[![Python 3.10-3.13](https://img.shields.io/badge/python-3.10--3.13-blue.svg)](https://pypi.org/project/websweep/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

WebSweep is a Python library for high-throughput web scraping for researchers.
It is designed to stay simple for beginners while still handling large URL lists.
The primary objective is to run effectively on a single computer (laptop or workstation),
without requiring cloud infrastructure or distributed orchestration.

It is built for projects that start with a list of websites and need a workflow
that is easy to rerun, inspect, and extend:

- crawl websites from a list of base URLs
- follow only within-domain links up to a bounded depth
- extract page-level text and metadata
- consolidate results back to one record per domain
- (if desired) repeat the same sweep monthly or quarterly with the same configured instance

The goal is research infrastructure, not cloud orchestration. WebSweep is meant
to work well on a laptop or workstation, with intermediate outputs that are easy
to archive, validate, and analyse later.

## What WebSweep Is Good For

WebSweep fits best when you want to study many websites in a comparable,
repeatable way.

Typical research uses:

- track how organisations discuss a topic over time across many domains
- build corpora from university, company, NGO, or government websites
- monitor recurring updates on the same website lists every few months
- extract page text plus a few structured fields for downstream analysis

It is especially useful when the unit of analysis is the domain or organisation,
not one single large site.

WebSweep is probably **not** the right tool when:

- you need to interact with JavaScript-heavy websites
- you want to scrape one very complex website with highly custom logic
- you need browser automation rather than HTML crawling

For those cases, tools such as [Scrapy](https://github.com/scrapy/scrapy) or
[Selenium](https://pypi.org/project/selenium/) may be a better fit.

## Install

```bash
pip install websweep
```


## What You Need

WebSweep needs a list of URLs.

- CLI mode: CSV or TSV file with header (`url`, optional `identifier`)
- Library mode: Python list of URLs or `(url, identifier)` tuples


Example CSV:

```csv
url,identifier
https://example.com,example
https://example2.org,example_org
```

## Choose Your Mode

- **CLI mode** (`websweep ...`): easiest way to run repeatable instance-based
  crawls from a CSV/TSV source file.
- **Library mode** (`from websweep import ...`): best when you need custom
  Python logic (custom extractors, analysis loops, notebooks).

## Workflow

![WebSweep pipeline workflow](docs/source/_static/pipeline_workflow.svg)

```text
Input URLs
  -> Crawler
     In:  URL list + crawl settings
     Out:
        crawled_data/*.zip (zipped pages per domain)
        overview_urls.{duckdb|db|tsv} (per-page crawl status overview)
  -> Extractor
     In:  overview file + crawled_data/*.zip
     Out: extracted_data/*.ndjson (extracted data per web page)
  -> Consolidator
     In:  extracted_data/*.ndjson
     Out: consolidated_data/*.ndjson (consolidated to domain level)
```

One-pass mode (lower disk usage):

```text
Input URLs -> Crawler(extract=True, save_html=False) -> extracted_data/*.ndjson
```

### What Each Step Does

- `Crawler`:
  starts from base URLs (one domain per row), downloads pages, follows only
  within-domain links, applies exclusion rules (for example blocked extensions),
  and stops at depth `max_level` (default: `3`).
- `Extractor`:
  reads crawled pages and extracts structured page-level fields such as cleaned
  text (`text`), metadata (`meta_*`), and location fields (`zipcode`, `address`).
- `Consolidator`:
  merges page-level records back to one record per domain, keeping concatenated domain text and aggregated information (e.g., `zipcode` frequencies, where the most frequent can be
  treated as the main postcode and others as additional postcodes)


## Quickstart (Python)

```python
from pathlib import Path
from websweep import Crawler, Extractor, Consolidator

urls = [
    "https://www.dggrootverbruik.nl/",
    "https://www.gosliga.nl/",
    "https://www.heeren2.nl/",
]

out = Path("./research_output")

# 1) Crawl
Crawler(target_folder_path=out).crawl_base_urls(urls)

# 2) Extract
Extractor(target_folder_path=out).extract_urls()

# 3) Consolidate
Consolidator(target_folder_path=out).consolidate()
```

## Quickstart (CLI)

```bash
websweep init --headless
websweep crawl
websweep extract
websweep consolidate
```

For lower disk usage:

```bash
websweep crawl --extract
websweep consolidate
```

Optional extractor add-on file (CLI):
set it during `websweep init` when prompted for a custom extractor add-on path.
Leave it empty/No for the default (`None`).
When provided, WebSweep copies the add-on into the instance folder (next to
`settings.ini`) so extraction does not depend on the original source location.

Using `target_temp_folder_path` (CLI and library):

- Use this when you want in-progress crawl files on a fast local disk.
- Raw page files are staged under `target_temp_folder_path/crawled_data/...`
  while crawling.
- Final domain zip files are written to `target_folder_path/crawled_data/*.zip`.
- The overview file (`overview_urls.duckdb` / `.db` / `.tsv`) is always kept in
  `target_folder_path`.
- After each domain is archived, staged raw files are removed from the temp path.

## Core Options (Library)

Most users only need these options:

- `Crawler(...)`
  - `max_level`: depth of within-domain link following (default `3`)
  - `max_pages_per_domain`: cap pages per domain
  - `extract=True` and `save_html=False`: one-pass crawl+extract mode
  - `allow_extensions` / `block_extensions`: file type filtering
  - `target_temp_folder_path`: optional temp folder for in-progress raw crawl files
- `Extractor(...)`
  - `workers`: extraction process count
  - `start_date`, `end_date`: session-date window for extraction
  - `file_extractor`: custom extractor subclass for add-on fields
- `Consolidator(...)`
  - `target_folder_path`: use default extracted input and standard consolidated output
  - `chunk_size`: consolidation chunk size for large extracted files

Advanced parameters are available in the API docs and User Guide.

Advanced example (explicit files):

```python
Consolidator(
    input_file=out / "extracted_data" / "extracted_data_2026-02-23_0-1000000.ndjson",
    output_file=out / "consolidated_data" / "custom_consolidated.ndjson",
).consolidate()
```

## Custom Extraction Add-ons

By default, core `FileExtractor` keeps extraction conservative:

- metadata (`meta_*`)
- cleaned text (`text`)
- zipcode/address (`zipcode`, `address`)

It does **not** extract `phone`, `email`, or `fax` unless you add custom
methods.

Create a custom add-on by subclassing `FileExtractor` and adding methods named
`_extract_<fieldname>`:

```python
from pathlib import Path
import re
from websweep import Extractor
from websweep.extractor.extractor import FileExtractor

class ResearchFileExtractor(FileExtractor):
    def _extract_fax(self) -> list:
        pattern = re.compile(
            r"(?is)\b(?:faxnumber|fax|f)\b[^0-9\+]{0,12}"
            r"([\+]?[0-9][0-9\-\s\(\)]{7,20})\b"
        )
        return sorted({m.strip() for m in re.findall(pattern, str(self.soup))})

Extractor(
    target_folder_path=Path("./research_output"),
    file_extractor=ResearchFileExtractor,
).extract_urls()
```

Repository add-on example:

- `addons/firmbackbone_extractor.py`

CLI usage with the same add-on:

```bash
websweep init --headless
# answer the add-on question with:
# addons/firmbackbone_extractor.py
websweep extract
```

The add-on path is optional and defaults to `None` (no add-on extractor).
Once configured in the instance, `websweep extract` and one-pass
`websweep crawl --extract` use it automatically.

## Choosing Which Files to Block/Allow

Rules are defined in:

- `src/websweep/utils/default_regex.json`
- `classify_url(...)` in `src/websweep/utils/utils.py`

CLI overrides:

```bash
websweep crawl --allow-extensions pdf,png
websweep crawl --block-extensions pdf,png,zip
websweep crawl --classification-file /path/to/rules.json
```

## Notebook Example

Featured end-to-end notebook:

- [`examples/example_scraper_extractor.ipynb`](examples/example_scraper_extractor.ipynb)

## Backend Selection

Overview storage backends:

- DuckDB (preferred for larger runs)
- SQLite
- TSV

Choose backend mode during `websweep init`:

- `use_database = True` (database mode in `settings.ini`) uses DuckDB when available
  and falls back to SQLite if needed.
- `use_database = False` uses TSV.

WebSweep also reuses any existing overview file in the instance
(`overview_urls.duckdb`, `overview_urls.db`, or `overview_urls.tsv`), so backend
selection is instance-level setup, not a required `websweep crawl` argument.

## Recurring CLI Runs (Every X Months)

WebSweep CLI does **not** schedule recurring crawls by itself.
You run the commands manually, or schedule them with your own tool
(for example cron, systemd timers, Windows Task Scheduler, or GitHub Actions).

For periodic updates, keep one configured instance and run:

```bash
END_DATE=$(uv run python -c "from datetime import date; print(date.today().isoformat())")
START_DATE=$(uv run python -c "from datetime import date, timedelta; print((date.today()-timedelta(days=90)).isoformat())")
websweep crawl
websweep extract --start-date "$START_DATE" --end-date "$END_DATE"
websweep consolidate
```

This keeps crawling simple while extracting only a rolling recent window
(last 90 days in this example). Adjust `timedelta(days=90)` as needed.

Example (Linux cron, first day of every 3rd month at 02:00):

```cron
0 2 1 */3 * cd /path/to/websweep && END_DATE=$(HOME=/path/to/home uv run python -c "from datetime import date; print(date.today().isoformat())") && START_DATE=$(HOME=/path/to/home uv run python -c "from datetime import date, timedelta; print((date.today()-timedelta(days=90)).isoformat())") && HOME=/path/to/home uv run websweep crawl && HOME=/path/to/home uv run websweep extract --start-date "$START_DATE" --end-date "$END_DATE" && HOME=/path/to/home uv run websweep consolidate
```

To retry failed base URLs from a specific crawl session date:

```bash
websweep crawl --complement 2026-04-01
```

## Extractor Date Windows (CLI)

Use date filters when extracting to limit processing to specific crawl
sessions:

```bash
websweep extract --start-date 2026-02-01 --end-date 2026-02-28
```

These filters apply to `session_date` in `overview_urls.*` and include only
successful crawl rows (`status == 200`). The date flags are per-run CLI
arguments and are not persisted automatically in `settings.ini`.

## Documentation

- Docs source: `docs/`
- Build locally: `make docs`
- Read the Docs config: `.readthedocs.yml`

## Troubleshooting Crawl Statuses

Common non-`200` statuses in `overview_urls.*`:

- `DNS lookup failed`: domain cannot be resolved from current network/DNS.
- `Connection failed`: host resolved, but TCP/SSL connection failed.
- `Request timeout`: remote host did not respond in time.
- `Robots unavailable in __test_domain_robots`: `robots.txt` check failed; crawler falls back to allow-all robots policy for that base URL.

For historical domain lists, many failures can be expected because domains may have expired or moved.

## Development

```bash
uv sync --group test --group docs --group dev
uv run pytest -q
uv run make docs
```

## Contributing

Contributions are what make the open source community an amazing place
to learn, inspire, and create. Any contributions you make are **greatly
appreciated**.

Please refer to the
[DEVELOPMENT](https://github.com/sodascience/websweep/blob/main/DEVELOPMENT.md)
file for more information on how to run the library without installing and how to install it from source.

Please refer to the
[CONTRIBUTING](https://github.com/sodascience/websweep/blob/main/CONTRIBUTING.md)
file for more information on issues and pull requests.

## License and citation

The package `websweep` is published under an MIT license. When using `websweep` for academic work, please cite:

    ODISSEI Social Data Science (SoDa). (2026). WebSweep (v0.1) [Software].
    Zenodo. https://doi.org/10.5281/zenodo.18980899

Zenodo record:

- https://zenodo.org/records/18980899


## Contact

This project is developed and maintained by the [ODISSEI Social Data
Science (SoDa)](https://odissei-data.nl/nl/soda/) team and the [FIRMBACKBONE](https://firmbackbone.nl/) Project.

<img src="soda_logo.png" alt="SoDa logo" width="250px"/>


FIRMBACKBONE is an organically growing longitudinal data-infrastructure with information on Dutch companies for scientific research. Once it is ready, it will become available for researchers and students affiliated with member universities in the Netherlands through [ODISSEI](https://odissei-data.nl/nl/), the Open Data Infrastructure for Social Science and Economic Innovations.

FIRMBACKBONE is an initiative of Utrecht University and the Vrije Universiteit Amsterdam funded by [PDI-SSH](https://pdi-ssh.nl/nl/home/), the Platform Digital Infrastructure-Social Sciences and Humanities, for the period 2020-2025.


Do you have questions, suggestions, or remarks? File an issue in the issue
tracker or feel free to contact the team via
https://odissei-data.nl/en/using-soda/.
