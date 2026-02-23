WebSweep Documentation
======================

WebSweep is a research-oriented scraping library for collecting text at scale
from many websites with a simple pipeline:

.. code-block:: text

   URL list
     -> Crawler
     -> Extractor
     -> Consolidator

You can also run crawler + extractor in one pass to save disk space:
``Crawler(extract=True, save_html=False)``.

What each step does:

- ``Crawler``: starts from base URLs (one domain per row), downloads pages,
  follows within-domain links, applies exclusion rules, and stops at depth
  ``max_level`` (default ``3``).
- ``Extractor``: reads crawled pages and extracts page-level fields such as
  cleaned text (``text``), metadata (``meta_*``), and location fields
  (``zipcode``, ``address``).
- ``Consolidator``: merges page-level records into one domain-level record,
  with aggregated postcode counts (the most frequent can be treated as the
  main postcode, the rest as additional postcodes) plus concatenated text.

Quick links
-----------

- PyPI: https://pypi.org/project/websweep/
- Source: https://github.com/sodascience/websweep
- Issues: https://github.com/sodascience/websweep/issues
- Featured example notebook:
  https://github.com/sodascience/websweep/blob/main/examples/example_scraper_extractor.ipynb

Real-world use cases
--------------------

- Track climate-related language on corporate websites.
- Build collaboration networks from university and lab websites.
- Monitor public-health communication from official web sources.

Documentation map
-----------------

.. toctree::
   :maxdepth: 2

   installation
   userguide
   examples
   modules
   contribute
   contact
