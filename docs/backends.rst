Backends
========

Both backends inherit all data-access logic from
:class:`~eurocontrolpy._base._EUROCONTROLBase`.  The only difference is
*how* a query is executed and *what type* is returned.

.. list-table::
   :widths: 30 35 35
   :header-rows: 1

   * -
     - :class:`~eurocontrolpy.EUROCONTROLpy`
     - :class:`~eurocontrolpy.EUROCONTROLSpark`
   * - **Connection**
     - SQLAlchemy engine (oracledb thin mode)
     - Oracle JDBC over Spark
   * - **Return type**
     - ``pandas.DataFrame``
     - ``pyspark.sql.DataFrame``
   * - **Geometry methods**
     - ✓ acc_sf / ansp_sf / es_sf / fir_sf
     - ✗ (use EUROCONTROLpy then pass GeoDataFrame to polyfill_h3)
   * - **polyfill_h3**
     - Pure-Python h3 per row
     - PySpark UDF (runs on workers)
   * - **Best for**
     - Notebooks, interactive, moderate data sizes
     - Large-scale production pipelines

How the shared base works
-------------------------

.. code-block:: text

   _EUROCONTROLBase (abstract)
   │  _build_flights_sql(...)        ← SQL built once, used by both
   │  _build_airspace_profiles_sql(...)
   │  ... (all _build_* methods)
   │
   │  flights_tidy(...)              ← calls _execute_query(_build_flights_sql(...))
   │  airlines_tidy(...)
   │  ... (all public data methods)
   │
   │  generate_so6(trajectory)       ← pure pandas, backend-independent
   │  polyfill_h3(gdf, resolution)   ← default pandas impl (overridden in Spark)
   │
   ├── EUROCONTROLpy
   │    _execute_query(sql)  → pd.read_sql(text(sql), engine.connect())
   │    _execute_table(tbl)  → pd.read_sql("SELECT * FROM tbl", ...)
   │    polyfill_h3(...)      → pandas (inherited default)
   │    + acc_sf / ansp_sf / es_sf / fir_sf
   │
   └── EUROCONTROLSpark
        _execute_query(sql)  → spark.read.jdbc(url, f"({sql}) T", props)
        _execute_table(tbl)  → spark.read.jdbc(url, tbl, props)
        _select_columns(df, cols) → df.select(*cols)
        _fillna_trajectory(df) → withColumn(...when isNull...)
        airspace_profiles_tidy(...)      ← overrides with two-query Spark join
        flights_airspace_profiles_tidy(...) ← same
        polyfill_h3(...)                 ← overrides with @F.udf

Switching backends
------------------

Because both classes expose the same method signatures, you can swap them
with a single line change:

.. code-block:: python

   # Development / notebook
   from eurocontrolpy import EUROCONTROLpy
   ec = EUROCONTROLpy()

   # Production (Spark)
   # from eurocontrolpy import EUROCONTROLSpark, build_spark_oracle_session
   # spark, url, props = build_spark_oracle_session()
   # ec = EUROCONTROLSpark(spark, url, props)

   # Everything below works unchanged:
   flights = ec.flights_tidy("2024-01-01", "2024-01-02")
   so6     = ec.generate_so6(flights.toPandas() if hasattr(flights, "toPandas") else flights)
