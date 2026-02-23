.. _examples:

Examples
========

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

Featured Notebook (Parsed)
--------------------------

Primary end-to-end example (crawler -> extractor -> consolidator), rendered
directly in the documentation and synced from
``examples/example_scraper_extractor.ipynb``:

.. toctree::
   :maxdepth: 1

   example_scraper_extractor

The notebook uses real websites and shows:

- input URLs
- how the crawler downloads pages and follows within-domain links up to
  ``max_level=3`` by default (with exclusions)
- how the extractor keeps page-level fields (text, metadata, postcode/address)
- how the consolidator merges page-level rows into one domain-level row with
  postcode counts and concatenated text
- crawler output
- extractor output sample
- consolidator output sample
- custom ``FileExtractor`` add-on usage
