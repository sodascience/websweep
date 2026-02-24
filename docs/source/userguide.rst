.. _userguide:

User Guide
==========

WebSweep can be used in two ways:

- Python library (recommended for reproducible research code and notebooks)
- CLI (``websweep ...``) for instance-based runs driven by a source file

Use the library when you want full control in Python code (custom extractors,
custom loops, programmatic analysis). Use the CLI when you want a simple,
repeatable command-line workflow.


Quickstart
----------

.. code-block:: text

   Input URLs
     -> Crawler
        Output: crawled_data/*.zip + overview_urls.{duckdb|db|tsv}
     -> Extractor
        Output: extracted_data/*.ndjson
     -> Consolidator
        Output: consolidated_data/*.ndjson

Disk-saving one-pass mode:

.. code-block:: text

   Input URLs -> Crawler(extract=True, save_html=False) -> extracted_data/*.ndjson

What each component does
------------------------

- ``Crawler``: starts from base URLs (one domain per row), downloads pages,
  follows only within-domain links, applies URL/file exclusion rules, and
  stops at depth ``max_level`` (default ``3``).
- ``Extractor``: reads crawled pages and extracts page-level fields such as
  cleaned text (``text``), metadata (``meta_*``), and location fields
  (``zipcode``, ``address``).
- ``Consolidator``: merges page-level records into one domain-level record,
  keeping aggregated postcode counts (the most frequent can be treated as the
  main postcode, with the others as additional postcodes) and concatenated
  domain text.


Library Quickstart
------------------

.. code-block:: python

   from pathlib import Path
   from websweep import Crawler, Extractor

   urls = [
       "https://www.dggrootverbruik.nl/",
       "https://www.gosliga.nl/",
       "https://www.heeren2.nl/",
   ]

   output_dir = Path("./research_output")

   crawler = Crawler(target_folder_path=output_dir)
   crawler.crawl_base_urls(urls)

   extractor = Extractor(target_folder_path=output_dir)
   extractor.extract_urls()


Library Workflow (Detailed)
---------------------------

Standard 3-step run:

.. code-block:: python

   from pathlib import Path
   from websweep import Crawler, Extractor, Consolidator

   urls = ["https://example.com", "https://example.org"]
   out = Path("./research_output")

   Crawler(target_folder_path=out).crawl_base_urls(urls)
   Extractor(target_folder_path=out).extract_urls()
   Consolidator(target_folder_path=out).consolidate()

One-pass run (crawl + extract together, less disk usage):

.. code-block:: python

   from pathlib import Path
   from websweep import Crawler

   urls = ["https://example.com", "https://example.org"]
   Crawler(
       target_folder_path=Path("./research_output"),
       save_html=False,
       extract=True,
   ).crawl_base_urls(urls)

Common library options (most used)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For most projects, these are the key options to learn first:

- ``Crawler(...)``
  - ``max_level``: how deep within-domain links are followed (default ``3``)
  - ``max_pages_per_domain``: cap per-domain crawl volume
  - ``extract=True`` with ``save_html=False``: one-pass crawl+extract
  - ``allow_extensions`` / ``block_extensions``: file filtering
- ``Extractor(...)``
  - ``workers``: number of extraction worker processes
  - ``start_date`` and ``end_date``: extract only selected crawl sessions
  - ``file_extractor``: custom add-on extraction fields
- ``Consolidator(...)``
  - ``target_folder_path``: use default extracted input and standard consolidated output
  - ``chunk_size``: memory/performance tradeoff for large inputs

Full constructor signatures remain available in the API reference.

Explicit-file example (advanced):

.. code-block:: python

   Consolidator(
       input_file=out / "extracted_data" / "extracted_data_2026-02-23_0-1000000.ndjson",
       output_file=out / "consolidated_data" / "custom_consolidated.ndjson",
   ).consolidate()


CLI Workflow (Detailed)
-----------------------

Input file format for CLI runs:

- CSV or TSV
- required column: ``url`` (or ``website`` / ``domain``)
- optional column: ``identifier`` (or ``id``)

Initialize an instance:

.. code-block:: bash

   websweep init --headless

Backend setup (done during ``init``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

During ``websweep init``, the CLI asks whether to use a database backend or TSV
(``use_database`` in ``settings.ini``):

- ``use_database = True``: database overview storage
- ``use_database = False``: TSV overview storage

When database mode is enabled, WebSweep resolves the concrete backend
automatically per instance:

- if an overview file already exists, it reuses that backend
  (``overview_urls.duckdb`` / ``overview_urls.db`` / ``overview_urls.tsv``)
- otherwise it prefers DuckDB, with SQLite fallback if DuckDB is unavailable

This means backend choice is configured at init/config level for the instance,
not as a required per-run ``websweep crawl`` argument.

How CLI configuration works
~~~~~~~~~~~~~~~~~~~~~~~~~~~

When you run ``websweep init``, WebSweep writes two configuration files:

- Application config file (global pointer to the active instance): ``config.ini``
- Instance settings file (settings for that instance): ``<your_instance_folder>/settings.ini``

The application config file is created automatically during ``websweep init`` and
stores where the active instance lives.

Example ``config.ini``:

.. code-block:: ini

   [Instance]
   location = /path/to/your/websweep_instance

Example ``settings.ini``:

.. code-block:: ini

   [Instance]
   location = /path/to/your/websweep_instance

   [Source]
   source_file = /path/to/overview_urls.tsv

   [Extractor]
   extractor_delete_files = False

   [Database]
   use_database = True

You can inspect or update these values with:

.. code-block:: bash

   websweep config

CLI commands and common options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Core workflow:

.. code-block:: bash

   websweep crawl
   websweep extract
   websweep consolidate

Common options by command:

- ``websweep init --headless``
  Run setup without GUI prompts.
- ``websweep config --source-file-path /path/to/urls.tsv``
  Point the active instance to a new source file.
- ``websweep config --delete-processed-files`` / ``--no-delete-processed-files``
  Toggle whether extractor removes crawled HTML after processing.
- ``websweep crawl --extract``
  Run crawl + extract in one pass (lower disk usage).
- ``websweep crawl --classification-file /path/to/rules.json``
  Use custom URL/file filtering rules.
- ``websweep crawl --allow-extensions pdf,png``
  Allow specific file extensions.
- ``websweep crawl --block-extensions pdf,png,zip``
  Block specific file extensions.
- ``websweep crawl --sock-connect 180``
  Change connection timeout.
- ``websweep crawl --target-temp-folder-path /tmp/websweep_tmp``
  Use a temporary crawl staging folder for in-progress raw files.
  Final domain ``.zip`` files and overview DB/TSV remain in the instance folder.
- ``websweep crawl --complement 2026-02-20``
  Re-crawl failed base URLs from a prior crawl date.
- ``websweep extract --workers 8``
  Use 8 extraction worker processes.
- ``websweep extract --start-date YYYY-MM-DD --end-date YYYY-MM-DD``
  Extract only successful pages from crawl sessions in that date window.
- ``websweep init`` (prompt: custom extractor add-on file)
  Set optional add-on path once per instance (default: None).
  The selected file is copied into the instance folder (next to
  ``settings.ini``) to avoid accidental loss from external file moves/deletes.

How ``target_temp_folder_path`` works
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``target_temp_folder_path`` is a staging location, not the final output location.

- In-progress raw pages are written to ``<target_temp_folder_path>/crawled_data/...``
- When a domain finishes, crawler creates ``<target_folder_path>/crawled_data/<domain>.zip``
- Staged raw domain files are removed from the temp folder after zipping
- ``overview_urls.{duckdb|db|tsv}`` always stays in ``target_folder_path``

This lets you use a small fast local disk for active crawling while keeping the
final outputs in one stable instance folder.
- ``websweep consolidate --input-file /path/to/extracted.ndjson``
  Consolidate a specific extracted file.
- ``websweep consolidate --output-file /path/to/consolidated.ndjson``
  Write consolidated output to a custom destination.
- ``websweep consolidate --chunk-size 20000``
  Set consolidation chunk size.

Extractor date windows (how dates are used)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   websweep extract --start-date 2026-02-01 --end-date 2026-02-28

Date filters are applied against ``session_date`` in
``overview_urls.{duckdb|db|tsv}`` and include only rows with ``status == 200``.

- ``session_date`` is the crawl run date (one date per crawl session)
- only pages from sessions inside the given window are extracted

Important: ``--start-date`` and ``--end-date`` are per-run options. They are
not persisted to ``settings.ini`` automatically. If you want to avoid
re-extracting older crawl sessions, pass the date window each time (or run
one-pass ``websweep crawl --extract``).

Recurring CLI pattern (every X months)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Keep one configured instance and run the same sequence on each update cycle:

.. code-block:: bash

   websweep crawl
   websweep extract --start-date 2026-04-01 --end-date 2026-04-30
   websweep consolidate

This pattern keeps recrawling simple while limiting extraction to the target
time window.

To retry previously failed base URLs from a specific crawl date:

.. code-block:: bash

   websweep crawl --complement 2026-04-01


Custom Extraction Add-ons
-------------------------

The core extractor is intentionally conservative. By default it keeps:

- metadata (`meta_*`)
- cleaned page text (`text`)
- zipcode/address (`zipcode`, `address`)

It does not include `phone`, `email`, or `fax` unless you add them.

You can add custom fields by subclassing ``FileExtractor`` and defining methods
named ``_extract_<fieldname>``:

.. code-block:: python

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

   extractor = Extractor(
       target_folder_path=Path("./research_output"),
       file_extractor=ResearchFileExtractor,
   )
   extractor.extract_urls()

Repository add-on example:

- ``addons/firmbackbone_extractor.py``

CLI usage with add-on file path:

.. code-block:: bash

   websweep init
   # when prompted for "custom extractor add-on file", set:
   # addons/firmbackbone_extractor.py

For one-pass mode:

.. code-block:: bash

   websweep crawl --extract

Once configured in the instance, the add-on is applied automatically by both
``websweep extract`` and one-pass ``websweep crawl --extract``.


URL Filtering Rules
-------------------

Crawler URL filtering is configured by:

- ``src/websweep/utils/default_regex.json``
- ``classify_url(...)`` in ``src/websweep/utils/utils.py``

Behavior summary:

- level 0 seed URLs: always crawled
- ``mailto:`` and ``tel:``: always skipped
- blocked extensions: skipped
- level 1: broad crawl
- level 2: filtered by ``url.url_regex``
- levels 3+: skipped by default

CLI overrides:

.. code-block:: bash

   websweep crawl --allow-extensions pdf,png
   websweep crawl --block-extensions pdf,png,zip
   websweep crawl --classification-file /path/to/rules.json


Troubleshooting Statuses
------------------------

``overview_urls.{duckdb|db|tsv}`` stores per-page crawl status values. Common
non-``200`` values:

- ``DNS lookup failed``: domain resolution failed from current network.
- ``Connection failed``: host resolved, but connection/SSL handshake failed.
- ``Request timeout``: remote host did not respond within timeout.
- ``Robots unavailable in __test_domain_robots``: robots check failed; crawler
  continues with allow-all robots policy for that base URL.

For large historical URL lists, high failure rates are often expected because
many domains are no longer online.
