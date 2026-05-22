# Configuration file for the Sphinx documentation builder.
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import sys
from pathlib import Path

# Make the package importable without a full install.
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# ── Project information ───────────────────────────────────────────────────────

project = "eurocontrolpy"
copyright = "2024, Enrico Spinielli, Quinten Goens"
author = "Enrico Spinielli, Quinten Goens"
release = "0.0.1"

# ── Extensions ────────────────────────────────────────────────────────────────

extensions = [
    "sphinx.ext.autodoc",    # Pull docstrings from source
    "sphinx.ext.napoleon",   # Parse NumPy / Google docstring styles
    "sphinx.ext.viewcode",   # Add [source] links next to each item
    "sphinx.ext.intersphinx",# Cross-ref Python / pandas docs
    "sphinx_copybutton",     # Add copy button to code blocks
    "myst_parser",           # Allow Markdown (.md) source files
    # NOTE: sphinx_autodoc_typehints is intentionally excluded.
    # Sphinx 7+ ships autodoc_typehints = "description" which does the same job
    # without the extension, and using both causes duplicate type blocks.
]

# ── Mock heavy optional dependencies so autodoc doesn't fail on import ────────

autodoc_mock_imports = [
    # Only mock packages that are NOT installed by `pip install -e ".[docs]"`.
    # Mocking an *installed* package races with the real sys.modules entry and
    # can produce confusing mock objects in rendered signatures.
    #
    # pyspark / geopandas / h3 / geopy / shapely are optional extras that are
    # NOT installed during the docs build, so they need mocking.
    # sqlalchemy and oracledb ARE installed (core deps) but only imported lazily
    # inside method bodies — autodoc never triggers those imports, so no mock needed.
    "pyspark",
    "geopandas",
    "h3",
    "geopy",
    "shapely",
]

# ── Autodoc settings ──────────────────────────────────────────────────────────

autodoc_default_options = {
    "members": True,
    "undoc-members": False,
    "private-members": False,
    "special-members": "__init__",
    "inherited-members": False,
    "show-inheritance": True,
}
autodoc_member_order = "groupwise"  # group by type (methods, attributes, …)
autodoc_typehints = "description"   # render types in the description block (Sphinx 7+ built-in)

# ── Napoleon (NumPy-style docstrings) ─────────────────────────────────────────

napoleon_numpy_docstring = True
napoleon_google_docstring = False
napoleon_use_param = True
napoleon_use_rtype = True
napoleon_preprocess_types = True

# ── Intersphinx mappings ──────────────────────────────────────────────────────

intersphinx_mapping = {
    "python":  ("https://docs.python.org/3", None),
    "pandas":  ("https://pandas.pydata.org/docs", None),
}

# ── MyST (Markdown) settings ──────────────────────────────────────────────────

myst_enable_extensions = ["colon_fence", "deflist"]
source_suffix = {".rst": "restructuredtext", ".md": "markdown"}

# ── HTML output ───────────────────────────────────────────────────────────────

html_theme = "furo"
html_title = "eurocontrolpy"
html_static_path = ["_static"]

html_theme_options = {
    "sidebar_hide_name": False,
    "navigation_with_keys": True,
    "source_repository": "https://github.com/eurocontrol/eurocontrolpy",
    "source_branch": "main",
    "source_directory": "docs/",
    "footer_icons": [
        {
            "name": "GitHub",
            "url": "https://github.com/eurocontrol/eurocontrolpy",
            "html": """
                <svg stroke="currentColor" fill="currentColor" stroke-width="0"
                    viewBox="0 0 16 16" height="1em" width="1em">
                    <path fill-rule="evenodd" d="M8 0C3.58 0 0 3.58 0 8c0 3.54
                    2.29 6.53 5.47 7.59.4.07.55-.17.55-.38
                    0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01
                    1.08.58 1.23.82.72 1.21 1.87.87
                    2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95
                    0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0
                    .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0
                    1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16
                    1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87
                    3.75-3.65 3.95.29.25.54.73.54 1.48 0
                    1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0 0 16 8c0-4.42-3.58-8-8-8z"/>
                </svg>
            """,
            "class": "",
        },
    ],
}

# ── Miscellaneous ─────────────────────────────────────────────────────────────

exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]
nitpicky = False  # don't error on missing cross-references to mocked modules
