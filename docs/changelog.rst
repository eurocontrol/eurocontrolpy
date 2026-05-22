Changelog
=========

0.0.1 (2024)
------------

Initial release.

* :class:`~eurocontrolpy.EUROCONTROLSpark` — PySpark / JDBC backend.
* :class:`~eurocontrolpy.EUROCONTROLpy` — SQLAlchemy / pandas backend.
* Shared abstract base :class:`~eurocontrolpy._base._EUROCONTROLBase` with all
  SQL-building logic.
* Data methods: :meth:`~eurocontrolpy._base._EUROCONTROLBase.flights_tidy`,
  :meth:`~eurocontrolpy._base._EUROCONTROLBase.adrr_flights_tidy`,
  :meth:`~eurocontrolpy._base._EUROCONTROLBase.airlines_tidy`,
  :meth:`~eurocontrolpy._base._EUROCONTROLBase.apdf_tidy`,
  :meth:`~eurocontrolpy._base._EUROCONTROLBase.airspace_profiles_tidy`,
  :meth:`~eurocontrolpy._base._EUROCONTROLBase.flights_airspace_profiles_tidy`,
  :meth:`~eurocontrolpy._base._EUROCONTROLBase.point_profiles_tidy`,
  :meth:`~eurocontrolpy._base._EUROCONTROLBase.export_model_trajectory`,
  :meth:`~eurocontrolpy._base._EUROCONTROLBase.export_airports`.
* Geometry methods (SQLAlchemy only): :meth:`~eurocontrolpy.EUROCONTROLpy.acc_sf`,
  :meth:`~eurocontrolpy.EUROCONTROLpy.ansp_sf`,
  :meth:`~eurocontrolpy.EUROCONTROLpy.es_sf`,
  :meth:`~eurocontrolpy.EUROCONTROLpy.fir_sf`.
* :meth:`~eurocontrolpy._base._EUROCONTROLBase.polyfill_h3` H3 hexagon fill
  (pandas + Spark UDF variants).
* :meth:`~eurocontrolpy._base._EUROCONTROLBase.generate_so6` SO6 segment
  conversion.
* :meth:`~eurocontrolpy._base._EUROCONTROLBase.airports_oa` OurAirports loader.
* IATA season helpers: :func:`~eurocontrolpy.season_iata`,
  :func:`~eurocontrolpy.iata_season_for_date`.
* Bundled :data:`~eurocontrolpy.member_state` dataset.
