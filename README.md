# WebSweep

WebSweep is a Python library for high-throughput, research-focused web scraping.
It is designed to stay simple for beginners while still handling large URL lists.
The primary objective is to run effectively on a single computer (laptop or workstation),
without requiring cloud infrastructure or distributed orchestration.

<b>Real-World Use Cases</b>
- Tracking Corporate Climate Responsibility: With a list of corporate websites, you can use WebSweep to efficiently analyze how frequently and positively they mention green energies, helping you gauge their commitment to climate responsibility.
- Analyzing Academic Collaboration Networks: WebSweep can be utilized to extract data from university websites and research databases, allowing you to identify patterns in academic collaboration, map research networks, and discover emerging interdisciplinary research fields.
- Tracking Public Health Information: By scraping data from government websites, health organizations, and medical journals, WebSweep can help you monitor the spread of diseases, evaluate the effectiveness of public health campaigns, and analyze the impact of healthcare policies on population health.

## Side-by-side comparison of WebSweep and Scrapy
 
- Are you looking to download lots of information from one domain --> You may want to use [Scrapy](https://github.com/scrapy/scrapy)
- Are you looking to download information from websites that require JavaScript --> You may want to use [selenium](https://pypi.org/project/selenium/)
- Are you looking to download and analyze HTML code from many pages --> WebSweep is for you


|                                       | WebSweep                                         | Scrapy                                                        |
|---------------------------------------|-----------------------------------------------------|---------------------------------------------------------------|
| Main use case                         | Download full HTML of many (up to 10,000,000) sites | Download specific elements of few websites (e.g. crawl Ebay)  |
| Intended use                          | Research                                            | Any                                                           |
| Use as beginner                    | Simple                                              | Complicated                                                   |
| Processing of HTML                    | During or after crawling                            | Typically during crawling                                     |
| Asynchronous                          |  Yes                                                | Yes                                                           |
| Speed (consumer laptop/home internet) | ~50,000 pages/hour                           | ?                                                             |
| JavaScript allowed                    | No                                                  | No (but extensions exist)                                     |
| Consolidates results at domain level  | Yes                                                 | No                                                            |



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

```text
Input URLs
  -> Crawler
     In:  URL list + crawl settings
     Out: 
        crawled_data/*.zip (a zipped file with the downloaded pages)  overview_urls.{duckdb|db|tsv} (database keeping track of what has been downloaded)
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

`target_folder_path` + `storage_path` mode (CLI):
- During `websweep init`, you can set an optional `storage path`.
- WebSweep keeps overview DB and default output in the instance folder
  (`target_folder_path`).
- If you pass `--target-temp-folder-path` during crawl, in-progress raw files
  are staged there.
- Completed domain `.zip` crawl files are moved to the large storage path
  where archived files reside.
- If unset, all files stay in the instance folder (default behavior).

## Core Options (Library)

Most users only need these options:

- `Crawler(...)`
  - `max_level`: depth of within-domain link following (default `3`)
  - `max_pages_per_domain`: cap pages per domain
  - `extract=True` and `save_html=False`: one-pass crawl+extract mode
  - `allow_extensions` / `block_extensions`: file type filtering
  - `storage_path`: optional large-storage location for completed `.zip` files
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

- `use_database = True` (SQL mode in `settings.ini`) uses DuckDB when available
  and falls back to SQLite if needed.
- `use_database = False` uses TSV.

WebSweep also reuses any existing overview file in the instance
(`overview_urls.duckdb`, `overview_urls.db`, or `overview_urls.tsv`), so backend
selection is instance-level setup, not a required `websweep crawl` argument.

## Recurring CLI Runs (Every X Months)

For periodic updates, keep one instance and run:

```bash
websweep crawl
websweep extract --start-date 2026-04-01 --end-date 2026-04-30
websweep consolidate
```

This keeps crawling simple and lets you extract only the new crawl sessions.

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
