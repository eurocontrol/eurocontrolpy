"""
EUROCONTROL PRISME/NM Oracle clients.

Two concrete backends share all SQL logic via _EUROCONTROLBase:

  EUROCONTROLSpark  – PySpark + Oracle JDBC.  Returns Spark DataFrames.
  EUROCONTROLpy     – SQLAlchemy + oracledb.  Returns pandas DataFrames.
                      Also provides airspace geometry methods (acc_sf, etc.)
                      that depend on Oracle Spatial functions and geopandas.

Both classes expose identical data-access methods; only the return type differs.
Use EUROCONTROLpy for interactive / notebook work and EUROCONTROLSpark for
large-scale distributed processing.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Union

import pandas as pd

from ._base import _EUROCONTROLBase

# ─────────────────────────────────────────────────────────────────────────────
# Spark session factory (kept as a standalone function for backwards compat)
# ─────────────────────────────────────────────────────────────────────────────

def build_spark_oracle_session(
    executor_memory: str = "5g",
    driver_memory: str = "2g",
    executor_cores: str = "1",
    executor_instances: str = "10",
    shuffle_partitions: str = "100",
    default_parallelism: str = "100",
    max_records_per_batch: str = "10000",
    jar_path: str = "jars/ojdbc8.jar",
    oracle_fetch_size: int = 1000,
):
    """
    Create (or reuse) a SparkSession configured for Oracle JDBC.

    Returns
    -------
    tuple[SparkSession, str, dict]
        The Spark session, JDBC URL, and JDBC connection properties.

    Notes
    -----
    Requires the environment variables ``PRU_DEV_USR`` (username),
    ``PRU_DEV_PWD`` (password), and ``PRU_DEV_DBNAME``
    (``host:port/service``).

    Download ``ojdbc8.jar`` from the Oracle JDBC downloads page and place it
    at *jar_path*.
    """
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("oracle-jdbc")
        .config("spark.jars", jar_path)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions",   "-Duser.timezone=UTC -Doracle.jdbc.timezoneAsRegion=false")
        .config("spark.executor.extraJavaOptions",  "-Duser.timezone=UTC -Doracle.jdbc.timezoneAsRegion=false")
        .config("spark.executor.memory",            executor_memory)
        .config("spark.driver.memory",              driver_memory)
        .config("spark.executor.cores",             executor_cores)
        .config("spark.executor.instances",         executor_instances)
        .config("spark.sql.shuffle.partitions",     shuffle_partitions)
        .config("spark.default.parallelism",        default_parallelism)
        .config("spark.serializer",                 "org.apache.spark.serializer.KryoSerializer")
        .config("spark.rpc.message.maxSize",        "512")
        .config("spark.sql.execution.arrow.enabled",           "true")
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", max_records_per_batch)
        .getOrCreate()
    )

    dbname = os.getenv("PRU_DEV_DBNAME")
    user   = os.getenv("PRU_DEV_USR")
    pwd    = os.getenv("PRU_DEV_PWD")

    url   = f"jdbc:oracle:thin:@//{dbname}"
    props = {
        "user":      user,
        "password":  pwd,
        "driver":    "oracle.jdbc.driver.OracleDriver",
        "fetchSize": str(oracle_fetch_size),
    }
    return spark, url, props


# ─────────────────────────────────────────────────────────────────────────────
# EUROCONTROLSpark
# ─────────────────────────────────────────────────────────────────────────────

class EUROCONTROLSpark(_EUROCONTROLBase):
    """
    PySpark client for EUROCONTROL PRISME/NM Oracle data via JDBC.

    All methods return Spark DataFrames unless noted otherwise.

    Parameters
    ----------
    spark : SparkSession, optional
        Existing session. If omitted, one is created via build_spark_oracle_session.
    url : str, optional
        JDBC URL.  Required if ``spark`` is provided.
    props : dict, optional
        JDBC connection properties.  Required if ``spark`` is provided.
    """

    def __init__(
        self,
        spark=None,
        url: Optional[str] = None,
        props: Optional[Dict[str, str]] = None,
    ) -> None:
        if spark and url and props:
            self.spark, self.url, self.props = spark, url, props
        else:
            self.spark, self.url, self.props = build_spark_oracle_session()

    # ── execution layer ───────────────────────────────────────────────────────

    def _execute_query(self, sql: str):
        """Execute an Oracle SQL string via JDBC and return a Spark DataFrame."""
        return self.spark.read.jdbc(
            url=self.url,
            table=f"({sql}) T",
            properties=self.props,
        )

    def _execute_table(self, table: str):
        """Return a full Oracle table as a Spark DataFrame via JDBC."""
        return self.spark.read.jdbc(
            url=self.url,
            table=table,
            properties=self.props,
        )

    def _select_columns(self, df, cols: list[str]):
        return df.select(*cols)

    def _fillna_trajectory(self, df):
        from pyspark.sql.functions import when, col, lit
        return (
            df.withColumn(
                "POINT_ID",
                when(col("POINT_ID").isNull(), lit("NO_POINT")).otherwise(col("POINT_ID")),
            ).withColumn(
                "AIR_ROUTE",
                when(col("AIR_ROUTE").isNull(), lit("NO_ROUTE")).otherwise(col("AIR_ROUTE")),
            )
        )

    # ── Spark-optimised multi-step implementation for large airspace queries ──

    def airspace_profiles_tidy(
        self,
        wef: Union[str, datetime],
        til: Union[str, datetime],
        airspace: str = "FIR",
        profile: str = "CTFM",
    ):
        """
        Airspace profile segments intersecting [wef, til).

        Uses two separate JDBC reads + Spark join for better performance
        on large datasets (avoids pushing a large IN-subquery over JDBC).
        """
        w_dt = self._as_utc(wef)
        t_dt = self._as_utc(til)
        wef_before = self._fmt(w_dt - timedelta(hours=28))
        til_after  = self._fmt(t_dt + timedelta(hours=24))
        wef_str    = self._fmt(w_dt)
        til_str    = self._fmt(t_dt)

        ids_sql = f"""
            SELECT DISTINCT ID
            FROM SWH_FCT.V_FAC_FLIGHT_MS
            WHERE TO_DATE('{wef_before}', 'YYYY-MM-DD HH24:MI:SS') <= LOBT
              AND LOBT < TO_DATE('{til_after}', 'YYYY-MM-DD HH24:MI:SS')
        """
        from pyspark.sql.functions import col
        ids_df = self._execute_query(ids_sql).select(col("ID").alias("ID_JOIN"))

        prf_sql = f"""
            SELECT
                SAM_ID, SEQ_ID,
                ENTRY_TIME, ENTRY_LON, ENTRY_LAT, ENTRY_FL,
                EXIT_TIME,  EXIT_LON,  EXIT_LAT,  EXIT_FL,
                AIRSPACE_ID, AIRSPACE_TYPE, MODEL_TYPE
            FROM FSD.ALL_FT_ASP_PROFILE
            WHERE TO_DATE('{wef_before}', 'YYYY-MM-DD HH24:MI:SS') <= LOBT
              AND LOBT < TO_DATE('{til_after}', 'YYYY-MM-DD HH24:MI:SS')
              AND MODEL_TYPE    = '{profile}'
              AND AIRSPACE_TYPE = '{airspace}'
              AND ENTRY_LON  IS NOT NULL
              AND ENTRY_LAT  IS NOT NULL
              AND ENTRY_TIME IS NOT NULL
              AND ENTRY_FL   IS NOT NULL
              AND EXIT_LON   IS NOT NULL
              AND EXIT_LAT   IS NOT NULL
              AND EXIT_TIME  IS NOT NULL
              AND EXIT_FL    IS NOT NULL
              AND ENTRY_TIME <= TO_DATE('{til_str}', 'YYYY-MM-DD HH24:MI:SS')
              AND TO_DATE('{wef_str}', 'YYYY-MM-DD HH24:MI:SS') < EXIT_TIME
        """
        prf_df = self._execute_query(prf_sql).withColumnRenamed("SAM_ID", "ID_JOIN")
        out = prf_df.join(ids_df.dropDuplicates(["ID_JOIN"]), on="ID_JOIN", how="inner")
        return out.withColumnRenamed("ID_JOIN", "ID")

    def flights_airspace_profiles_tidy(
        self,
        wef: Union[str, datetime],
        til: Union[str, datetime],
        airspace: str = "FIR",
        profile: str = "CTFM",
    ):
        """
        Flights whose airspace segments intersect [wef, til).

        Returns the same columns as flights_tidy.
        Uses two-step Spark approach for performance.
        """
        w_dt = self._as_utc(wef)
        t_dt = self._as_utc(til)
        wef_before = self._fmt(w_dt - timedelta(hours=28))
        til_after  = self._fmt(t_dt + timedelta(hours=24))

        prf_ids = (
            self.airspace_profiles_tidy(wef, til, airspace, profile)
            .select("ID")
            .dropDuplicates()
        )

        flt_sql = f"""
            SELECT
                flt.ID, flt.LOBT, flt.AIRCRAFT_ID, flt.ADEP,
                apt_adep.PRU_DASHBOARD_AP_NAME AS NAME_ADEP,
                apt_adep.COUNTRY_CODE          AS COUNTRY_CODE_ADEP,
                apt_adep.COUNTRY_NAME          AS COUNTRY_NAME_ADEP,
                flt.ADES,
                apt_ades.PRU_DASHBOARD_AP_NAME AS NAME_ADES,
                apt_ades.COUNTRY_CODE          AS COUNTRY_CODE_ADES,
                apt_ades.COUNTRY_NAME          AS COUNTRY_NAME_ADES,
                flt.ADES_FILED,
                apt_ades_f.PRU_DASHBOARD_AP_NAME AS NAME_ADES_FILED,
                apt_ades_f.COUNTRY_CODE          AS COUNTRY_CODE_ADES_FILED,
                apt_ades_f.COUNTRY_NAME          AS COUNTRY_NAME_ADES_FILED,
                flt.SENSITIVE,
                flt.EXMP_RSN_LH AS SPECIAL_EXEMPT,
                frl.RULE_NAME,
                flt.FLT_UID, flt.IOBT, flt.FLT_RULES, flt.ICAO_FLT_TYPE,
                flt.REGISTRATION, flt.AIRCRAFT_ADDRESS, flt.AIRCRAFT_TYPE_ICAO_ID,
                flt.WK_TBL_CAT, flt.AIRCRAFT_OPERATOR,
                aog.AO_ISO_CTRY_CODE, aog.AO_GRP_CODE, aog.AO_GRP_NAME,
                flt.EOBT_1, flt.ARVT_1, flt.TAXI_TIME_1,
                flt.AOBT_3, flt.ARVT_3, flt.TAXI_TIME_3,
                flt.FLT_DUR_1, flt.FLT_DUR_3,
                flt.RTE_LEN_1, flt.RTE_LEN_3, flt.FLT_TOW
            FROM SWH_FCT.V_FAC_FLIGHT_MS flt
            LEFT JOIN SWH_FCT.DIM_FLIGHT_TYPE_RULE frl
                ON flt.SK_FLT_TYPE_RULE_ID = frl.SK_FLT_TYPE_RULE_ID
            LEFT JOIN PRUDEV.V_COVID_DIM_AO aog
                ON flt.AIRCRAFT_OPERATOR = aog.AO_CODE
            LEFT JOIN PRUDEV.V_COVID_REL_AIRPORT_AREA apt_adep
                ON flt.ADEP = apt_adep.CFMU_AP_CODE
            LEFT JOIN PRUDEV.V_COVID_REL_AIRPORT_AREA apt_ades
                ON flt.ADES = apt_ades.CFMU_AP_CODE
            LEFT JOIN PRUDEV.V_COVID_REL_AIRPORT_AREA apt_ades_f
                ON flt.ADES_FILED = apt_ades_f.CFMU_AP_CODE
            WHERE flt.LOBT >= TO_DATE('{wef_before}', 'YYYY-MM-DD HH24:MI:SS')
              AND flt.LOBT <  TO_DATE('{til_after}',  'YYYY-MM-DD HH24:MI:SS')
        """
        flt_df = self._execute_query(flt_sql)
        return (
            flt_df.join(prf_ids, flt_df["ID"] == prf_ids["ID"], how="inner")
            .drop(prf_ids["ID"])
            .dropDuplicates()
        )

    # ── H3 polyfill (Spark UDF) ───────────────────────────────────────────────

    def polyfill_h3(self, gdf: Any, resolution: int, explode: bool = False):
        """
        Fill airspace polygons with H3 cells and return a Spark DataFrame.

        Parameters
        ----------
        gdf : geopandas.GeoDataFrame
            Output from ``EUROCONTROLpy.*_sf`` methods.
        resolution : int
            H3 resolution (0–15).
        explode : bool, default False
            If True, one row per H3 cell (other columns duplicated).
            If False, one row per airspace with ``h3_index`` as ArrayType.

        Returns
        -------
        pyspark.sql.DataFrame
            The geometry column is dropped; all other properties are kept
            alongside ``h3_index`` (ArrayType of StringType, or a single
            StringType when exploded).

        Notes
        -----
        Requires ``h3-py`` (``pip install h3``) on the driver **and** every
        Spark worker node (or included in ``spark.archives`` / a conda env).
        """
        import json
        from shapely.geometry import mapping as shapely_mapping

        from pyspark.sql import functions as F
        from pyspark.sql.types import ArrayType, StringType

        res = resolution  # captured by the UDF closure — only a plain int

        @F.udf(returnType=ArrayType(StringType()))
        def _h3_udf(geojson_str: str):
            if geojson_str is None:
                return []
            import json as _json
            try:
                import h3 as _h3
            except ImportError:
                return []

            geojson = _json.loads(geojson_str)

            def _polyfill(poly):
                try:
                    return list(_h3.geo_to_cells(poly, res=res))   # h3 >= 4
                except AttributeError:
                    return list(_h3.polyfill_geojson(poly, res))    # h3 3.x

            gt = geojson.get("type", "")
            if gt == "Polygon":
                return _polyfill(geojson)
            if gt == "MultiPolygon":
                cells: set = set()
                for poly_coords in geojson["coordinates"]:
                    cells.update(_polyfill({"type": "Polygon", "coordinates": poly_coords}))
                return sorted(cells)
            return []

        gdf = gdf.copy()
        gdf["_geojson"] = gdf["geometry"].apply(
            lambda g: json.dumps(shapely_mapping(g))
        )
        pdf = gdf.drop(columns=["geometry"])
        sdf = self.spark.createDataFrame(pdf)
        sdf = (
            sdf.withColumn("h3_index", _h3_udf(F.col("_geojson")))
               .drop("_geojson")
        )
        if explode:
            sdf = sdf.withColumn("h3_index", F.explode(F.col("h3_index")))
        return sdf


# ─────────────────────────────────────────────────────────────────────────────
# SQLAlchemy engine factory
# ─────────────────────────────────────────────────────────────────────────────

def build_sqlalchemy_oracle_engine(schema: str = "PRU_DEV"):
    """
    Build a SQLAlchemy Engine for Oracle using oracledb (thin mode).

    Reads credentials from environment variables:
      ``{schema}_USR``, ``{schema}_PWD``, ``{schema}_DBNAME``

    Parameters
    ----------
    schema : str, default 'PRU_DEV'
        Prefix for the environment variable names.

    Returns
    -------
    sqlalchemy.engine.Engine
    """
    from sqlalchemy import create_engine
    import oracledb

    oracledb.version = "8.3.0"  # present the driver as cx_Oracle 8.x to SQLAlchemy
    import sqlalchemy.dialects.oracle  # noqa: F401 – ensure dialect is registered

    user   = os.getenv(f"{schema}_USR")
    pwd    = os.getenv(f"{schema}_PWD")
    dbname = os.getenv(f"{schema}_DBNAME")  # {host}:{port}/{service}

    if not all([user, pwd, dbname]):
        raise EnvironmentError(
            f"Missing environment variables: {schema}_USR, {schema}_PWD, {schema}_DBNAME"
        )

    # oracledb thin-mode DSN: host:port/service
    host, rest = dbname.split(":", 1)
    port, service = rest.split("/", 1)
    engine = create_engine(
        f"oracle+oracledb://{user}:{pwd}@{host}:{port}/{service}",
        connect_args={"mode": oracledb.DEFAULT_AUTH},
    )
    return engine


# ─────────────────────────────────────────────────────────────────────────────
# EUROCONTROLpy  (SQLAlchemy backend)
# ─────────────────────────────────────────────────────────────────────────────

class EUROCONTROLpy(_EUROCONTROLBase):
    """
    SQLAlchemy / pandas client for EUROCONTROL PRISME/NM Oracle data.

    All methods return pandas DataFrames unless noted otherwise.
    In addition to the methods inherited from ``_EUROCONTROLBase``, this class
    provides ``acc_sf``, ``ansp_sf``, ``es_sf``, and ``fir_sf`` for airspace
    geometries (returned as geopandas GeoDataFrames).

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine, optional
        Existing SQLAlchemy engine. If omitted, one is created using
        ``build_sqlalchemy_oracle_engine(schema)``.
    schema : str, default 'PRU_DEV'
        Environment variable prefix used when auto-creating the engine.
        Reads ``{schema}_USR``, ``{schema}_PWD``, ``{schema}_DBNAME``
        from the environment.
    """

    def __init__(self, engine=None, schema: str = "PRU_DEV") -> None:
        if engine is not None:
            self.engine = engine
        else:
            self.engine = build_sqlalchemy_oracle_engine(schema)

    # ── execution layer ───────────────────────────────────────────────────────

    def _execute_query(self, sql: str) -> pd.DataFrame:
        from sqlalchemy import text
        with self.engine.connect() as conn:
            return pd.read_sql(text(sql), conn)

    def _execute_table(self, table: str) -> pd.DataFrame:
        return self._execute_query(f"SELECT * FROM {table}")

    # _select_columns and _fillna_trajectory use the pandas defaults from base.

    # ── airspace geometry methods (SQLAlchemy only) ───────────────────────────

    @staticmethod
    def _read_geojson_result(engine, sql: str, params: dict) -> pd.DataFrame:
        """
        Execute a query that returns a single-row CLOB containing GeoJSON and
        parse it into a geopandas GeoDataFrame.
        """
        try:
            import geopandas as gpd
        except ImportError:
            raise ImportError(
                "geopandas is required for airspace geometry methods. "
                "Install it with: pip install geopandas"
            )
        import io
        from sqlalchemy import text

        with engine.connect() as conn:
            result = conn.execute(text(sql), params)
            row = result.fetchone()
            if row is None or row[0] is None:
                raise ValueError("Query returned no geometry data.")
            geojson_str = str(row[0])  # LOB → str

        return gpd.read_file(io.StringIO(geojson_str))

    def acc_sf(self, cfmu_airac: Union[str, int]) -> pd.DataFrame:
        """
        ACC/OAC airspace geometries for the given CFMU AIRAC cycle.

        Parameters
        ----------
        cfmu_airac : str | int
            CFMU AIRAC cycle number, e.g. ``517``.

        Returns
        -------
        geopandas.GeoDataFrame
            Columns: AC_ID, AV_AIRSPACE_ID, MIN_FLIGHT_LEVEL, MAX_FLIGHT_LEVEL,
            NAME, CODE, AIRSPACE_TYPE, geometry.
        """
        sql = """
            WITH AIRSPACE_NAMES AS (
                SELECT
                    ID,
                    CODE,
                    NAME,
                    PRU_ATC_TYPE AS AIRSPACE_TYPE
                FROM PRU_STAT_AUA
                WHERE PRU_ATC_TYPE IN ('ACC', 'OAC')
            )
            SELECT
              '{ "type": "FeatureCollection", "features": [' ||
              RTRIM(
                SWH_MAP.clobagg(
                  '{ "type": "Feature", "geometry": '
                  || SWH_MAP.SDO2GEOJSON(SHAPE, 3, 0, 0)
                  || ', "properties": {'
                  || '"AC_ID": '               || A.AC_ID           || ', '
                  || '"AV_AIRSPACE_ID": "'     || A.AIRSPACE_ID     || '", '
                  || '"MIN_FLIGHT_LEVEL": '    || A.MIN_FLIGHT_LEVEL || ', '
                  || '"MAX_FLIGHT_LEVEL": '    || A.MAX_FLIGHT_LEVEL || ', '
                  || '"NAME": "'               || B.NAME            || '", '
                  || '"CODE": "'               || B.CODE            || '", '
                  || '"AIRSPACE_TYPE": "'      || B.AIRSPACE_TYPE   || '"'
                  || '}}' || ',' || CHR(13)
                ), ',' || CHR(13)
              ) || ']}'
            FROM ENV_SP.AIRSPACE A
            INNER JOIN AIRSPACE_NAMES B
              ON (A.AIRSPACE_ID = B.ID AND AIRSPACE_KIND = 'STAT_AUA')
            WHERE A.AC_ID = :cfmu_airac
              AND A.SHAPE IS NOT NULL
        """
        return self._read_geojson_result(
            self.engine, sql, {"cfmu_airac": str(cfmu_airac)}
        )

    def ansp_sf(self, cfmu_airac: Union[str, int]) -> pd.DataFrame:
        """
        ACE ANSP airspace geometries for the given CFMU AIRAC cycle.

        Excludes entries with CODE in ('AIRPORT', 'UNKNOWN', 'MILITARY').

        Returns
        -------
        geopandas.GeoDataFrame
            Columns: airac_cfmu, id, code, name, ace_code, min_fl, max_fl,
            airspace_type, geometry.
        """
        sql = """
            WITH AIRSPACE_NAMES AS (
                SELECT DISTINCT
                    A.AIRSPACE_ID,
                    A.AIRSPACE_TYPE,
                    P.NAME,
                    P.ACE_CODE,
                    P.CODE
                FROM ENV_SP.AIRSPACE A
                JOIN PRU_CFMU_ANSP P
                  ON A.AIRSPACE_ID = P.ID
                 AND A.AIRSPACE_KIND = 'ANSP'
                 AND A.AC_ID = :cfmu_airac
            )
            SELECT
              '{ "type": "FeatureCollection", "features": [' ||
              RTRIM(
                SWH_MAP.clobagg(
                  '{ "type": "Feature", "geometry": '
                  || SWH_MAP.SDO2GEOJSON(SHAPE, 3, 0, 0)
                  || ', "properties": {'
                  || '"AC_ID": '             || A.AC_ID                  || ', '
                  || '"AV_AIRSPACE_ID": "'   || A.AIRSPACE_ID            || '", '
                  || '"MIN_FLIGHT_LEVEL": '  || A.MIN_FLIGHT_LEVEL       || ', '
                  || '"MAX_FLIGHT_LEVEL": '  || A.MAX_FLIGHT_LEVEL       || ', '
                  || '"NAME": "'             || AN.NAME                  || '", '
                  || '"ACE_CODE": "'         || AN.ACE_CODE              || '", '
                  || '"CODE": "'             || AN.CODE                  || '", '
                  || '"AIRSPACE_TYPE": "'    || A.AIRSPACE_TYPE          || '"'
                  || '}}' || ',' || CHR(13)
                ), ',' || CHR(13)
              ) || ']}'
            FROM ENV_SP.AIRSPACE A
            LEFT JOIN AIRSPACE_NAMES AN
              ON A.AIRSPACE_ID = AN.AIRSPACE_ID
             AND A.AIRSPACE_TYPE = AN.AIRSPACE_TYPE
            WHERE A.AC_ID = :cfmu_airac
              AND A.AIRSPACE_TYPE = 'ANSP'
              AND A.SHAPE IS NOT NULL
        """
        gdf = self._read_geojson_result(
            self.engine, sql, {"cfmu_airac": str(cfmu_airac)}
        )
        gdf = gdf[~gdf["CODE"].isin(["AIRPORT", "UNKNOWN", "MILITARY"])]
        return gdf.rename(columns={
            "AC_ID":            "airac_cfmu",
            "AV_AIRSPACE_ID":   "id",
            "CODE":             "code",
            "NAME":             "name",
            "ACE_CODE":         "ace_code",
            "MIN_FLIGHT_LEVEL": "min_fl",
            "MAX_FLIGHT_LEVEL": "max_fl",
            "AIRSPACE_TYPE":    "airspace_type",
        })[["airac_cfmu", "id", "code", "name", "ace_code", "min_fl", "max_fl",
            "airspace_type", "geometry"]]

    def es_sf(self, cfmu_airac: Union[str, int]) -> pd.DataFrame:
        """
        Elementary Sector airspace geometries for the given CFMU AIRAC cycle.

        Returns
        -------
        geopandas.GeoDataFrame
            Columns: AC_ID, AV_AIRSPACE_ID, MIN_FLIGHT_LEVEL, MAX_FLIGHT_LEVEL,
            NAME, CODE, AIRSPACE_TYPE, geometry.
        """
        sql = """
            WITH AIRSPACE_NAMES AS (
                SELECT DISTINCT
                    A.AIRSPACE_ID,
                    A.AIRSPACE_TYPE,
                    P.NAME,
                    P.CODE
                FROM ENV_SP.AIRSPACE A
                JOIN PRU_CFMU_ES P
                  ON A.AIRSPACE_ID = P.ID
                 AND A.AIRSPACE_KIND = 'ES'
                 AND A.AC_ID = :cfmu_airac
            )
            SELECT
              '{ "type": "FeatureCollection", "features": [' ||
              RTRIM(
                SWH_MAP.clobagg(
                  '{ "type": "Feature", "geometry": '
                  || SWH_MAP.SDO2GEOJSON(SHAPE, 3, 0, 0)
                  || ', "properties": {'
                  || '"AC_ID": '             || A.AC_ID           || ', '
                  || '"AV_AIRSPACE_ID": "'   || A.AIRSPACE_ID     || '", '
                  || '"MIN_FLIGHT_LEVEL": '  || A.MIN_FLIGHT_LEVEL || ', '
                  || '"MAX_FLIGHT_LEVEL": '  || A.MAX_FLIGHT_LEVEL || ', '
                  || '"NAME": "'             || AN.NAME           || '", '
                  || '"CODE": "'             || AN.CODE           || '", '
                  || '"AIRSPACE_TYPE": "'    || A.AIRSPACE_TYPE   || '"'
                  || '}}' || ',' || CHR(13)
                ), ',' || CHR(13)
              ) || ']}'
            FROM ENV_SP.AIRSPACE A
            LEFT JOIN AIRSPACE_NAMES AN
              ON A.AIRSPACE_ID = AN.AIRSPACE_ID
             AND A.AIRSPACE_TYPE = AN.AIRSPACE_TYPE
            WHERE A.AC_ID = :cfmu_airac
              AND A.AIRSPACE_TYPE = 'ES'
              AND A.SHAPE IS NOT NULL
        """
        return self._read_geojson_result(
            self.engine, sql, {"cfmu_airac": str(cfmu_airac)}
        )

    def fir_sf(self, cfmu_airac: Union[str, int]) -> pd.DataFrame:
        """
        FIR (Flight Information Region) airspace geometries for the given CFMU AIRAC cycle.

        An ``icao`` column (first 2 characters of ``code``) is derived automatically.

        Returns
        -------
        geopandas.GeoDataFrame
            Columns: airac_cfmu, icao, id, code, name, min_fl, max_fl,
            airspace_type, geometry.
        """
        sql = """
            WITH AIRSPACE_NAMES AS (
                SELECT DISTINCT
                    A.AIRSPACE_ID,
                    A.AV_TYPE AS AIRSPACE_TYPE,
                    P.NAME,
                    P.CODE,
                    A.ID AS ID
                FROM ENV_SP.AIRSPACE_VOLUME A
                JOIN PRU_CFMU_FIR P
                  ON A.AIRSPACE_ID = P.CODE
                 AND A.AV_TYPE = 'FIR'
                 AND A.AC_ID = :cfmu_airac
            )
            SELECT
              '{ "type": "FeatureCollection", "features": [' ||
              RTRIM(
                SWH_MAP.clobagg(
                  '{ "type": "Feature", "geometry": '
                  || SWH_MAP.SDO2GEOJSON(SHAPE, 3, 0, 0)
                  || ', "properties": {'
                  || '"AC_ID": '             || A.AC_ID           || ', '
                  || '"AV_AIRSPACE_ID": "'   || A.AIRSPACE_ID     || '", '
                  || '"MIN_FLIGHT_LEVEL": '  || A.MIN_FLIGHT_LEVEL || ', '
                  || '"MAX_FLIGHT_LEVEL": '  || A.MAX_FLIGHT_LEVEL || ', '
                  || '"NAME": "'             || AN.NAME           || '", '
                  || '"CODE": "'             || AN.CODE           || '", '
                  || '"ID": "'               || AN.ID             || '", '
                  || '"AIRSPACE_TYPE": "'    || A.AV_TYPE         || '"'
                  || '}}' || ',' || CHR(13)
                ), ',' || CHR(13)
              ) || ']}'
            FROM ENV_SP.AIRSPACE_VOLUME A
            LEFT JOIN AIRSPACE_NAMES AN
              ON A.AIRSPACE_ID = AN.AIRSPACE_ID
             AND A.AV_TYPE = AN.AIRSPACE_TYPE
            WHERE A.AC_ID = :cfmu_airac
              AND A.AV_TYPE = 'FIR'
              AND A.SHAPE IS NOT NULL
        """
        gdf = self._read_geojson_result(
            self.engine, sql, {"cfmu_airac": str(cfmu_airac)}
        )
        gdf = gdf.rename(columns={
            "AC_ID":            "airac_cfmu",
            "AV_AIRSPACE_ID":   "code",       # note: R renames to 'code' from AV_AIRSPACE_ID
            "ID":               "id",
            "CODE":             "code_orig",
            "NAME":             "name",
            "MIN_FLIGHT_LEVEL": "min_fl",
            "MAX_FLIGHT_LEVEL": "max_fl",
            "AIRSPACE_TYPE":    "airspace_type",
        })
        # Derive 'icao' from first 2 chars of code_orig (matches R behaviour)
        gdf["icao"] = gdf["code_orig"].str[:2]
        gdf = gdf.rename(columns={"code_orig": "code"})
        return gdf[["airac_cfmu", "icao", "id", "code", "name",
                    "min_fl", "max_fl", "airspace_type", "geometry"]]
