# WebSweep

WebSweep is a Python library for high-throughput, research-focused web scraping.
It is designed to stay simple for beginners while still handling large URL lists.
The primary objective is to run effectively on a single computer (laptop or workstation),
without requiring cloud infrastructure or distributed orchestration.

## Install

```bash
pip install websweep
```

`pip install websweep` uses `google-re2` on supported Python versions (3.10+),
with automatic fallback to `regex` when unavailable.
WebSweep also installs and uses `lxml` as the default HTML parser for faster
page parsing in crawling/extraction.

## What You Need

WebSweep needs a list of URLs.

- CLI mode: CSV or TSV file with header (`url`, optional `identifier`)
- Library mode: Python list of URLs or `(url, identifier)` tuples
- If your source file is an old `overview_urls.*` export (with a `level`
  column), WebSweep automatically keeps only `level == 0` base URLs and
  drops exact duplicate `(url, identifier)` rows before crawling.

Example CSV:

```csv
url,identifier
https://example.com,example
https://example.org,example_org
```

## Workflow

```text
Input URLs
  -> Crawler
     In:  URL list + crawl settings
     Out: crawled_data/*.zip + overview_urls.{duckdb|db|tsv}
  -> Extractor
     In:  overview file + crawled_data/*.zip
     Out: extracted_data/*.ndjson
  -> Consolidator
     In:  extracted_data/*.ndjson
     Out: consolidated_data/*.ndjson
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
  merges page-level records back to one record per domain, keeping aggregated
  postcode information (`zipcode` frequencies, where the most frequent can be
  treated as the main postcode and others as additional postcodes) and
  concatenated domain text.

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
input_file = sorted((out / "extracted_data").glob("*.ndjson"))[0]
Consolidator(str(input_file)).consolidate(str(out / "consolidated_data" / "consolidated.ndjson"))
```

## Quickstart (CLI)

```bash
websweep init --headless
websweep crawl
websweep extract
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

You can force a backend:

```bash
websweep crawl --overview-backend duckdb
```

## Expected Throughput (Pages/Hour)

On a single machine, expected **downloaded pages/hour** (not domains) varies
mainly with how many domains are still online and reachable.

- Healthy list (mostly reachable domains): typically `~1,000-6,000` pages/hour
- Mixed historical list (many dead/blocked domains): typically `~70-900` pages/hour

Observed on the SIDN-style benchmark list after source normalization:

- around `~700` downloaded pages/hour in the first 100-row sample
- around `~70` downloaded pages/hour in a 20-row sample with many dead domains

Use DuckDB for larger runs and keep the input as base URLs (one row per site)
to maximize sustained throughput and reduce avoidable DNS errors.

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
pip install uv
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

    XXX


## Contact

This project is developed and maintained by the [ODISSEI Social Data
Science (SoDa)](https://odissei-data.nl/nl/soda/) team and the [FIRMBACKBONE](https://firmbackbone.nl/) Project.

<img src="soda_logo.png" alt="SoDa logo" width="250px"/>


FIRMBACKBONE is an organically growing longitudinal data-infrastructure with information on Dutch companies for scientific research. Once it is ready, it will become available for researchers and students affiliated with member universities in the Netherlands through [ODISSEI](https://odissei-data.nl/nl/), the Open Data Infrastructure for Social Science and Economic Innovations.

FIRMBACKBONE is an initiative of Utrecht University and the Vrije Universiteit Amsterdam funded by [PDI-SSH](https://pdi-ssh.nl/nl/home/), the Platform Digital Infrastructure-Social Sciences and Humanities, for the period 2020-2025.


Do you have questions, suggestions, or remarks? File an issue in the issue
tracker or feel free to contact the team via
https://odissei-data.nl/en/using-soda/.
