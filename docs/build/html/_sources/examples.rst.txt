.. _examples:

Examples
========

This page provides examples of how to use `websweep` both as a command line interface (CLI) tool and as a Python package.

Command Line Interface Examples
-------------------------------

Initialize a new WebSweep instance:

.. code-block:: bash

   $ websweep init
     --headless

Restore an existing WebSweep instance:

.. code-block:: bash

   $ websweep restore
     --headless

Configure WebSweep settings:

.. code-block:: bash

   $ websweep config
     --delete-processed-files
     --target-folder-path /path/to/new/output
     --source-file-path /path/to/new/source/file.csv

View active WebSweep instance:

.. code-block:: bash

   $ websweep instance

Start crawling process:

.. code-block:: bash

   $ websweep crawl
     --sock-connect 150

Start extracting process:

.. code-block:: bash

   $ websweep extract
     --start-date YYYY-MM-DD
     --end-date YYYY-MM-DD


Python Package Examples
----------------------

Scraping and extracting at the same time (no HTML saved):

.. code-block:: python

   from websweep import Crawler

   urls = ['https://firmbackbone.nl', 'https://uu.nl/', 'https://odissei-soda.nl']

   crawler_unit = Crawler(target_folder_path=Path("your/data/folder"), save_html=False, extract=True)
   crawler_unit.crawl_base_urls(urls)

Only scraping (saving HTML):

.. code-block:: python

   from websweep import Crawler

   urls = ['https://firmbackbone.nl', 'https://uu.nl/', 'https://odissei-soda.nl']

   crawler_unit = Crawler(target_folder_path=Path("your/data/folder"))
   crawler_unit.crawl_base_urls(urls)

Only extracting (using HTML):

.. code-block:: python

   from websweep import FileExtractor

   extractor = FileExtractor(target_folder_path=Path("your/data/folder"))
   extractor.extract_urls()

Only extracting with custom Extractor methods (using HTML):

.. code-block:: python

   from websweep import FileExtractor
   from pathlib import Path
   import re

   class MyCustomFileExtractor(FileExtractor):
       def __init__(self, *args, **kwargs):
           super().__init__(*args, **kwargs)

       def _extract_something(self) -> list:
           """
           Extract something from the input file, and add found somethings to self.something in set form
           """
           pattern = re.compile(r'\b\d{5,}\b', re.VERBOSE)
           somethings = list(set(re.findall(pattern, self.text)))
           return somethings

   extractor_unit = MyCustomFileExtractor(target_folder_path=Path("your/data/folder"))
   extractor_unit.extract_urls()