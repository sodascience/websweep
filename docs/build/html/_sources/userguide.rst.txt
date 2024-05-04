.. _userguide:

User's Guide
============

The `websweep` tool can be used as a Python library or through its command line interface (CLI). Below are the basic usage instructions for both methods.

Using as a Library
------------------

To use `websweep` as a library, you can import it into your Python script and call its functions directly. Here is an example of how to use `websweep` in your code:

.. code-block:: python

   # Example Python code that uses websweep as a library
   from websweep import Crawler

   # Initialize the Scraper
   crawler = Crawler(target_folder_path = Path("your/data/folder"))
   # Use the Crawler to download HTML content
   crawler.crawl_base_urls(['https://firmbackbone.nl', 'https://uu.nl/'])

   # Process the content...
   # (your code here)

Command Line Interface (CLI)
----------------------------

`websweep` also provides a command line interface for easy access to its features directly from your terminal.


Options
^^^^^^^^

.. code-block:: none

   -v, --version            Show the application's version and exit.
   --install-completion     Install completion for the current shell.
   --show-completion        Show completion for the current shell, to copy it or
                            customize the installation.
   --help                   Show this message and exit.

Commands
^^^^^^^^^

.. code-block:: none

   $ websweep config        Alter WebSweep configuration settings.
   $ websweep crawl         Start caching websites.
   $ websweep extract       Start extracting data from the fetched files.
   $ websweep init          Initialise a new WebSweep instance.
   $ websweep instance      Open configured WebSweep instance folder.
   $ websweep restore       Restore configuration of existing WebSweep instance.


Initialise WebSweep
^^^^^^^^^^^^^^^^^^^^^^

Before using WebSweep, you need to initialize a new instance or you need to restore an instance. Note that only one instance can be active at any given time. However, you can switch between instances by restoring such sessions.
To initialise a new instance, you can use the `init` command:

.. code-block:: bash

   $ websweep init

Options:
  --headless
                                  Run without GUI elements
  --no-headless
                                  Run with GUI elements  [default: no-headless]

The initialisation process allows for the following configurations:

- WebSweep instance folder location
- Crawl source file (csv)
- Remove raw files after data extraction
- Usage of SQL or CSV database 


Restore WebSweep Instance
^^^^^^^^^^^^^^^^^^^^^^

As mentioned, you may also configure WebSweep with an existing instance folder instead of creating a new one. When you restore a session, you should not initialise a new session with `init`. You can restore a session with the `restore` command:

.. code-block:: bash

   $ websweep restore

Options:
  --headless
                                  Run without GUI elements
  --no-headless
                                  Run with GUI elements  [default: no-headless]



Reconfigure WebSweep
^^^^^^^^^^^^^^^^^^^^^^

You may alter some configurations of the restored or created WebSweep instance. You can do this with the `config` command:

.. code-block:: bash

   $ websweep config

Options:
  --delete-processed-files
                                  Delete extractor processed raw files
  --no-delete-processed-files
                                  Not-Delete extractor processed raw files
  --target-folder-path TEXT       Set new path for crawled data output
  --source-file-path TEXT         Set new path for csv source file


Other configurations such as the usage of database type and instance location cannot be altered.


Inspect WebSweep Instance
^^^^^^^^^^^^^^^^^^^^^^

You can view the active WebSweep session. You can do this with the `instance` command:

.. code-block:: bash

   $ websweep instance


Using WebSweep: Crawling 
^^^^^^^^^^^^^^^^^^^^^^

Only when WebSweep is configured with a new or restored instance, the crawling functionality can be used.
The `crawl` command will start the crawling process of the urls provided in the source file and outputs the crawled data to the WebSweep instance folder under `crawled_data`.

Options:
  --complement TEXT           Complement the folder with failed pages, takes
                              the crawl date as argument
  --sock-connect INTEGER      Timeout value (ms) for establishing a connection
                              to remote server  [default: 120]
  --extract                   Extract files directly after crawl instead of saving HTML
  --no-extract                Save HTML  [default: no-extract]
  --classification-file PATH  Use a custom classification file with page title
                              terms (plain .txt with ';' delimitation)

Using WebSweep: Extracting
^^^^^^^^^^^^^^^^^^^^^^

Only when WebSweep is configured with a new or restored instance and the instance folder is populated with crawled data, the extracting functionality can be used.
The `extract` command will start the extracting process of the crawled html files and outputs the extracted data to the WebSweep instance folder under `extracted_data.

Options:
  --start-date TEXT  Date on which the files are retreieved and extracted
                     should start extracting
  --end-date TEXT    Date on which the files are retreieved and extractor
                     should stop extracting
