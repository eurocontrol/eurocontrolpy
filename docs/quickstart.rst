Quick start
===========

Both backends share an identical method surface.  Swap ``EUROCONTROLpy`` for
``EUROCONTROLSpark`` (and set up Spark first) whenever you need distributed
processing — the method calls are the same.

Connecting
----------

.. code-block:: python

   from eurocontrolpy import EUROCONTROLpy

   # Reads PRU_DEV_USR / PRU_DEV_PWD / PRU_DEV_DBNAME from the environment.
   ec = EUROCONTROLpy()

   # Use a different schema prefix (PRU_READ_* vars):
   ec = EUROCONTROLpy(schema="PRU_READ")

   # Pass an existing SQLAlchemy engine explicitly:
   from eurocontrolpy import build_sqlalchemy_oracle_engine
   engine = build_sqlalchemy_oracle_engine(schema="PRU_READ")
   ec = EUROCONTROLpy(engine=engine)

Querying flights
----------------

.. code-block:: python

   # Scheduled and non-scheduled flights for one day (default filters applied).
   flights = ec.flights_tidy("2024-06-01", "2024-06-02")

   # Include extra equipment columns:
   flights = ec.flights_tidy(
       "2024-06-01",
       "2024-06-02",
       extra_cols=["ICAO_EQPT", "COM_EQPT"],
   )

   # Fetch specific flights by SAM ID:
   flights = ec.flights_tidy("2024-06-01", "2024-06-02", ids=[12345678, 87654321])

   # ADRR format (Aviation Data Repository for Research):
   adrr = ec.adrr_flights_tidy("2024-06-01", "2024-06-02")

Airspace profiles
-----------------

.. code-block:: python

   # All FIR segments intersecting a 3-day window:
   profiles = ec.airspace_profiles_tidy("2024-06-01", "2024-06-04", airspace="FIR")

   # Flights whose profile segments intersect the window:
   flights_fir = ec.flights_airspace_profiles_tidy(
       "2024-06-01", "2024-06-04", airspace="FIR", profile="CTFM"
   )

Point profiles and SO6
-----------------------

.. code-block:: python

   # Point trajectories for one day:
   points = ec.point_profiles_tidy("2024-06-01", "2024-06-02")

   # Restrict to a bounding box:
   bbox = {"xmin": 5.0, "xmax": 10.0, "ymin": 47.0, "ymax": 52.0}
   points = ec.point_profiles_tidy("2024-06-01", "2024-06-02", bbox=bbox)

   # Convert to SO6 segment format:
   so6 = ec.generate_so6(points)

Airlines and airport data
--------------------------

.. code-block:: python

   # Airline info with EU membership flag:
   airlines = ec.airlines_tidy()

   # Latest airport list from OurAirports (no DB needed):
   airports = ec.airports_oa()

Airspace geometries
-------------------

Requires ``geopandas`` — install with ``pip install "eurocontrolpy[geo_shapes]"``.

.. code-block:: python

   # FIR polygons for AIRAC 517 as a GeoDataFrame:
   firs = ec.fir_sf(cfmu_airac=517)

   # ACC/OAC airspace:
   accs = ec.acc_sf(cfmu_airac=517)

   # Write to GeoJSON:
   firs.to_file("firs-517.geojson", driver="GeoJSON")

H3 polyfill
-----------

Requires ``h3`` — install with ``pip install "eurocontrolpy[h3,geo_shapes]"``.

.. code-block:: python

   firs = ec.fir_sf(cfmu_airac=517)

   # One row per FIR, h3_index is a list of H3 cell strings at resolution 4:
   h3_df = ec.polyfill_h3(firs, resolution=4)
   print(h3_df[["name", "h3_index"]].head())

   # Exploded — one row per H3 cell:
   h3_exploded = ec.polyfill_h3(firs, resolution=4, explode=True)

IATA seasons
------------

No database connection required.

.. code-block:: python

   from eurocontrolpy import season_iata, iata_season_for_date

   start, end = season_iata(2024, "summer")
   print(start, "→", end)
   # 2024-03-31 00:00:00+00:00 → 2024-10-26 00:00:00+00:00

   print(iata_season_for_date("2024-11-15"))
   # winter-2024

Using the Spark backend
-----------------------

Install PySpark first: ``pip install "eurocontrolpy[spark]"``

.. code-block:: python

   from eurocontrolpy import EUROCONTROLSpark, build_spark_oracle_session

   spark, url, props = build_spark_oracle_session(jar_path="jars/ojdbc8.jar")
   ec = EUROCONTROLSpark(spark=spark, url=url, props=props)

   # All methods are identical to EUROCONTROLpy but return Spark DataFrames:
   flights = ec.flights_tidy("2024-06-01", "2024-06-02")
   flights.show(5)

   # polyfill_h3 on Spark uses a UDF that runs on workers:
   from eurocontrolpy import EUROCONTROLpy
   ec_py  = EUROCONTROLpy()
   firs   = ec_py.fir_sf(517)                        # fetch geometry (pandas)
   spark_h3 = ec.polyfill_h3(firs, resolution=4)     # polyfill on Spark
   spark_h3.show(5)
