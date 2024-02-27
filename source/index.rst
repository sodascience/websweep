.. websweep documentation master file, created by
   sphinx-quickstart on Wed Dec 20 02:24:29 2023.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. WebSweep documentation master file

.. |Licence| image:: https://img.shields.io/badge/Licence-MIT-purple?labelColor=gray&style=flat
   :target: #license-and-citation
   :alt: Licence

.. |PyPIBadge| image:: https://img.shields.io/badge/PyPI-v1.0-orange?labelColor=gray&style=flat
   :target: https://pypi.org/project/websweep/
   :alt: PyPI


Welcome to WebSweep's Documentation
====================================

|PyPIBadge| |Licence|

WebSweep is a powerful Python library crafted for web scraping projects that prioritize simplicity, modularity, and impressive speed.

What is WebSweep?
-----------------
WebSweep is a joint initiative of `FIRMBACKBONE <https://firmbackbone.nl>`_ and `ODISSEI Social Data Science <https://odissei-data.nl/en/>`_. WebSweep offers a suite of tools designed to make the extraction of information from the web as straightforward and efficient as possible. With WebSweep, developers and researchers can harness the power of web scraping to transform unstructured web data into valuable insights.

Real-World Use Cases
^^^^^^^^^^^^^^^^^^^^

- **Tracking Corporate Climate Responsibility**: WebSweep enables analysts to efficiently evaluate corporate websites, measuring the frequency and sentiment of mentions related to green energies. This provides a quantifiable metric to gauge a company's commitment to climate responsibility.

- **Analyzing Academic Collaboration Networks**: By extracting data from various academic and research-oriented websites, WebSweep assists in uncovering patterns of academic collaboration. It serves as a tool for mapping research networks and identifying emerging trends in interdisciplinary studies.

- **Tracking Public Health Information**: Utilizing WebSweep's scraping capabilities on government and health organization websites, as well as medical journals, can significantly contribute to public health monitoring. It allows for real-time tracking of disease spread, assessment of public health initiatives, and analysis of the effects of healthcare policies on community wellness.

Project Background
------------------

FIRMBACKBONE is an organically growing longitudinal data-infrastructure with information on Dutch companies for scientific research and education. Once it is ready, it will become available for researchers and students affiliated with Dutch member universities through the Open Data Infrastructure for Social Science and Economic Innovations (ODISSEI).

FIRMBACKBONE is an initiative of Utrecht University (UU) and the Vrije Universiteit Amsterdam (VU Amsterdam) funded by the Platform Digital Infrastructure-Social Sciences and Humanities (PDI-SSH) for the period 2020-2025.

Features
--------

- **High-Volume Crawling**: WebSweep is designed to handle large-scale web scraping tasks, capable of downloading the full HTML content from a vast number of websites, scaling up to 10 million sites.

- **Research-Driven**: The primary intended use of WebSweep is research, facilitating the collection and analysis of web data across various domains and disciplines.

- **User-Friendly for Beginners**: With a focus on simplicity, WebSweep is accessible to beginners, offering an easy-to-use interface for web scraping tasks.

- **HTML Processing Flexibility**: Users have the option to process HTML content either during the crawl or post-crawl, providing flexibility in how data is handled and analyzed.

- **Asynchronous Operation**: WebSweep operates asynchronously, allowing for efficient utilization of resources and faster execution of scraping tasks.

- **Impressive Speed**: On a consumer-grade laptop with a home internet connection, WebSweep can process between 50,000 and 100,000 pages per hour, demonstrating remarkable efficiency.

- **JavaScript Limitation**: Currently, WebSweep does not process JavaScript, ensuring a focus on the raw HTML content for scraping tasks.

- **Domain-Level Consolidation**: Results can be consolidated at the domain level, enabling a more organized and structured approach to data aggregation and analysis.


License and citation
--------------------

The package websweep is published under an MIT license. When using websweep for academic work, please cite:

.. code-block:: text

   XXXXX


Contents
--------

.. toctree::
   :maxdepth: 2

   installation
   userguide
   examples
   

.. Contents
.. - WebSweep
.. - Installation
.. - Start Guide
.. - User's Guide

.. Project
.. - Contribute
.. - Contact
.. - PyPI Releases
.. - Source Code
.. - Issue Tracker




.. .. toctree::
..     :hidden:
..     :glob:

..     *




.. .. toctree::
..    :maxdepth: 2
..    :caption: Contents:



.. Indices and tables
.. ==================

.. * :ref:`genindex`
.. * :ref:`modindex`
.. * :ref:`search`
