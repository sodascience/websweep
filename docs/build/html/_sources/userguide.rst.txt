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

   extracted = sorted((out / "extracted_data").glob("*.ndjson"))[0]
   Consolidator(str(extracted)).consolidate(
       str(out / "consolidated_data" / "consolidated.ndjson")
   )

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


CLI Workflow (Detailed)
-----------------------

Input file format for CLI runs:

- CSV or TSV
- required column: ``url`` (or ``website`` / ``domain``)
- optional column: ``identifier`` (or ``id``)

Initialize an instance:

.. code-block:: bash

   websweep init --headless

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

Crawl and extract:

.. code-block:: bash

   websweep crawl
   websweep extract

Helpful flags:

.. code-block:: bash

   websweep crawl --overview-backend duckdb
   websweep crawl --extract
   websweep extract --workers 8


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
