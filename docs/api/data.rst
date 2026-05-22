Bundled datasets
================

These objects are loaded at import time from CSV files bundled with the package.

.. autodata:: eurocontrolpy.member_state

   A :class:`pandas.DataFrame` of EUROCONTROL Member / Agreement States.

   Columns:

   .. list-table::
      :widths: 20 80
      :header-rows: 1

      * - Column
        - Description
      * - ``name``
        - Country name (e.g. ``"Italy"``)
      * - ``iso3c``
        - ISO 3166-1 alpha-3 code (e.g. ``"ITA"``)
      * - ``iso2c``
        - ISO 3166-1 alpha-2 code (e.g. ``"IT"``) — used by :meth:`~eurocontrolpy._base._EUROCONTROLBase.airlines_tidy`
      * - ``icao``
        - ICAO 2-letter prefix (e.g. ``"LI"``)
      * - ``iso3n``
        - ISO 3166-1 numeric code (e.g. ``"380"``)
      * - ``date``
        - Date of membership status change
      * - ``status``
        - ``"M"`` (Member), ``"C"`` (Comprehensive Agreement), or ``NaN`` (Kosovo)

   .. note::
      ``aircraft_type`` and ``aircraft_model`` datasets from the R package are
      not bundled here due to size and update frequency.  Fetch them directly
      from the ICAO Aircraft Type Designators list or supply your own CSV.
