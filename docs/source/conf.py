from __future__ import annotations

import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

DOCS_SOURCE = Path(__file__).resolve().parent
DOCS_DIR = DOCS_SOURCE.parent
REPO_ROOT = DOCS_DIR.parent

PYTHON_SOURCE = REPO_ROOT / "python"

# Point Sphinx to the root of your Python source code directory.
sys.path.insert(0, str(PYTHON_SOURCE.resolve()))


# ---------------------------------------------------------------------------
# Project Metadata
# ---------------------------------------------------------------------------

project = "ScyllaDB Python RS Driver"
copyright = "2026, ScyllaDB"
author = "ScyllaDB"
version = "0.1.0"
release = "0.1.0"
html_title = "ScyllaDB Python RS Driver Documentation"
html_short_title = "Python RS Driver Docs"

# Master document setting
master_doc = "contents"


# ---------------------------------------------------------------------------
# Extensions
# ---------------------------------------------------------------------------

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "myst_parser",
    "sphinx_scylladb_theme",
    "sphinx_autodoc_typehints",
]


# ---------------------------------------------------------------------------
# ScyllaDB Official Theme Configuration
# ---------------------------------------------------------------------------

html_theme = "sphinx_scylladb_theme"

html_theme_options = {  # type: ignore
    "conf_py_path": "docs/source/",
    "github_repository": "scylladb-zpp-2025-python-rs-driver/python-rs-driver",
    "github_issues_repository": "scylladb-zpp-2025-python-rs-driver/python-rs-driver",
    "hide_edit_this_page_button": False,
    "hide_feedback_buttons": False,
    "hide_version_dropdown": [],
}

# Explicitly wire up the custom sidebar layout from the theme
html_sidebars = {"**": ["side-nav.html"]}

# Hide parent class names in sidebar for cleaner navigation trees
toc_object_entries_show_parents = "hide"


# ---------------------------------------------------------------------------
# Source formats
# ---------------------------------------------------------------------------

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}


# ---------------------------------------------------------------------------
# Autodoc configuration
# ---------------------------------------------------------------------------

autodoc_default_options = {
    "members": True,
    "undoc-members": False,
    "show-inheritance": True,
}

autodoc_mock_imports = ["scylla._rust"]
