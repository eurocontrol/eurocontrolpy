eurocontrolpy
=============

**eurocontrolpy** is a Python client for the EUROCONTROL PRISME / Network Manager (NM)
Oracle database. It provides two interchangeable backends that expose an identical API:

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Class
     - Backend
   * - :class:`~eurocontrolpy.EUROCONTROLpy`
     - `SQLAlchemy <https://www.sqlalchemy.org/>`_ + `oracledb <https://python-oracledb.readthedocs.io/>`_. Returns **pandas DataFrames**. Best for notebooks and interactive work.
   * - :class:`~eurocontrolpy.EUROCONTROLSpark`
     - PySpark + Oracle JDBC. Returns **Spark DataFrames**. Best for large-scale distributed processing.

Both classes share the same SQL-building logic via the abstract base class
:class:`~eurocontrolpy._base._EUROCONTROLBase`, so query results are always
structurally identical regardless of the backend you choose.

.. note::

   eurocontrolpy is a Python port of the
   `eurocontrol R package <https://github.com/eurocontrol/eurocontrol>`_
   originally designed by **Enrico Spinielli**.
   The Python package was developed by **Quinten Goens** with Enrico as co-author.
   See :doc:`authors` for details.

.. code-block:: python

   from eurocontrolpy import EUROCONTROLpy

   ec = EUROCONTROLpy()                          # reads PRU_DEV_* env vars
   flights = ec.flights_tidy("2024-01-01", "2024-01-02")
   print(flights.shape)


.. toctree::
   :maxdepth: 2
   :caption: User Guide

   installation
   quickstart
   backends

.. toctree::
   :maxdepth: 3
   :caption: API Reference

   api/index

.. toctree::
   :maxdepth: 1
   :caption: Project

   authors
   changelog
