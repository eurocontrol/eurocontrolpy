Installation
============

Requirements
------------

* Python ≥ 3.8
* Access to the EUROCONTROL PRISME / NM Oracle database
* Oracle credentials exposed as environment variables (see :ref:`credentials`)

Core install
------------

.. code-block:: bash

   pip install eurocontrolpy

This installs the SQLAlchemy backend (:class:`~eurocontrolpy.EUROCONTROLpy`) and all
required runtime dependencies (``pandas``, ``sqlalchemy``, ``oracledb``, ``requests``).

Optional extras
---------------

Install additional extras as needed:

.. list-table::
   :widths: 20 30 50
   :header-rows: 1

   * - Extra
     - Command
     - Enables
   * - ``geo``
     - ``pip install "eurocontrolpy[geo]"``
     - Accurate geodesic distances in :meth:`~eurocontrolpy._base._EUROCONTROLBase.generate_so6` via `geopy <https://geopy.readthedocs.io/>`_ (falls back to Haversine otherwise).
   * - ``geo_shapes``
     - ``pip install "eurocontrolpy[geo_shapes]"``
     - Airspace geometry methods: :meth:`~eurocontrolpy.EUROCONTROLpy.acc_sf`, :meth:`~eurocontrolpy.EUROCONTROLpy.ansp_sf`, :meth:`~eurocontrolpy.EUROCONTROLpy.es_sf`, :meth:`~eurocontrolpy.EUROCONTROLpy.fir_sf`.
   * - ``h3``
     - ``pip install "eurocontrolpy[h3]"``
     - H3 hexagon polyfill via :meth:`~eurocontrolpy._base._EUROCONTROLBase.polyfill_h3`.
   * - ``spark``
     - ``pip install "eurocontrolpy[spark]"``
     - :class:`~eurocontrolpy.EUROCONTROLSpark` PySpark backend.
   * - ``dev``
     - ``pip install "eurocontrolpy[dev]"``
     - Development tools: ``ruff`` linter/formatter and all docs dependencies.

Combine extras with commas:

.. code-block:: bash

   pip install "eurocontrolpy[geo,geo_shapes,h3]"

Installing from source
----------------------

.. code-block:: bash

   git clone https://github.com/eurocontrol/eurocontrolpy.git
   cd eurocontrolpy
   pip install -e ".[dev]"

.. _credentials:

Environment variables
---------------------

The library reads Oracle credentials from environment variables.  The prefix
(``PRU_DEV`` by default) can be changed via the ``schema`` parameter of
:class:`~eurocontrolpy.EUROCONTROLpy` or :func:`~eurocontrolpy.build_sqlalchemy_oracle_engine`.

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Variable
     - Value
   * - ``PRU_DEV_USR``
     - Oracle username
   * - ``PRU_DEV_PWD``
     - Oracle password
   * - ``PRU_DEV_DBNAME``
     - Full Oracle connection string in the form ``{host}:{port}/{service}``

Set them in your shell or ``.env`` file before running any queries:

.. code-block:: bash

   export PRU_DEV_USR="myuser"
   export PRU_DEV_PWD="mypassword"
   export PRU_DEV_DBNAME="oracle.example.int:1521/PRISME"
