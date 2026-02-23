.. _examples:

Examples
========

Featured notebook
-----------------

Primary end-to-end example (crawler -> extractor -> consolidator):

- https://github.com/sodascience/websweep/blob/main/examples/example_scraper_extractor.ipynb

The notebook uses real websites and shows:

- input URLs
- crawler output
- extractor output sample
- consolidator output sample
- custom ``FileExtractor`` add-on usage


CLI Examples
------------

Initialize:

.. code-block:: bash

   websweep init --headless

Crawl:

.. code-block:: bash

   websweep crawl

Crawl and extract in one go (lower disk usage):

.. code-block:: bash

   websweep crawl --extract

Extract:

.. code-block:: bash

   websweep extract

Backend and filtering controls:

.. code-block:: bash

   websweep crawl --overview-backend duckdb
   websweep crawl --allow-extensions pdf,png
   websweep crawl --block-extensions pdf,png,zip


Python Examples
---------------

Basic pipeline:

.. code-block:: python

   from pathlib import Path
   from websweep import Crawler, Extractor, Consolidator

   urls = ["https://example.com", "https://example.org"]
   out = Path("./research_output")

   Crawler(target_folder_path=out).crawl_base_urls(urls)
   Extractor(target_folder_path=out).extract_urls()

   extracted = sorted((out / "extracted_data").glob("*.ndjson"))[0]
   Consolidator(str(extracted)).consolidate(str(out / "consolidated_data" / "consolidated.ndjson"))

One-pass crawling + extraction:

.. code-block:: python

   from pathlib import Path
   from websweep import Crawler

   urls = ["https://example.com", "https://example.org"]
   Crawler(target_folder_path=Path("./research_output"), save_html=False, extract=True).crawl_base_urls(urls)

