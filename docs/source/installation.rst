Installation
============

Requirements
------------

- Python 3.10+
- pip

Install from PyPI
-----------------

.. code-block:: bash

   pip install websweep

By default, ``pip install websweep`` installs ``google-re2`` on supported
Python versions (3.10+). If unavailable, WebSweep falls back to ``regex``.
WebSweep also installs and uses ``lxml`` as the default HTML parser for faster
crawling/extraction parsing, with runtime fallback to ``html.parser`` when
``lxml`` is unavailable.

Verify installation:

.. code-block:: bash

   websweep --version


Install from Source (Developers)
--------------------------------

.. code-block:: bash

   git clone https://github.com/sodascience/websweep.git
   cd websweep
   pip install uv
   uv sync --group test --group docs --group dev

Run tests:

.. code-block:: bash

   uv run pytest -q

Build docs:

.. code-block:: bash

   uv run make docs
