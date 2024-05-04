# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os
import sys
sys.path.insert(0, os.path.abspath('../src'))


project = 'WebSweep'
copyright = '2023, ODISSEI Social Data Science'
author = 'ODISSEI Social Data Science'
release = '1.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc'
]

templates_path = ['_templates']
exclude_patterns = []

autodoc_member_order = 'bysource'  # Or 'alphabetical', or 'groupwise'


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'alabaster'

html_sidebars = {
    '**': [
        'about.html',
        'contents.html',
        'searchbox.html',
    ]
}

html_theme_options = {
    'sidebar_caption': 'Contents',
    'description': 'A powerful Python library for web scraping',
    'show_powered_by': False,
    'fixed_sidebar': True,
    'extra_nav_links': {
        "Contribute": "contribute.html",
        "Contact": "contact.html",
        "PyPI Releases": "https://pypi.org/project/websweep/",
        "Source Code": "https://github.com/sodascience/websweep",
        "Issue Tracker": "https://github.com/sodascience/websweep/issues",
    }
}

html_static_path = ['_static']
