import os
import shutil
import stat
import subprocess
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, Optional, Union

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit
import pyspark.sql.functions as F
try:
    from geopy.distance import geodesic
except ImportError:
    geodesic = None


# ============================== Oracle via Spark (JDBC) ============================== #
def build_spark_oracle_session(
    executor_memory: str = "5g",
    driver_memory: str = "2g",
    executor_cores: str = "1",
    executor_instances: str = "10",
    shuffle_partitions: str = "100",
    default_parallelism: str = "100",
    max_records_per_batch: str = "10000",
    jar_path: str = "jars/ojdbc8.jar",
    oracle_fetch_size: int = 1000
) -> tuple[SparkSession, str, Dict[str, str]]:
    """
    Create (or reuse) a SparkSession configured for Oracle JDBC and return
    the Spark session, JDBC URL, and JDBC properties.

    Parameters
    ----------
    executor_memory : str, default "5g"
        Memory allocated per executor.
    driver_memory : str, default "2g"
        Memory allocated for the driver.
    executor_cores : str, default "1"
        Number of cores per executor.
    executor_instances : str, default "10"
        Number of executor instances.
    shuffle_partitions : str, default "100"
        Number of shuffle partitions for Spark SQL operations.
    default_parallelism : str, default "100"
        Default parallelism level for Spark.
    max_records_per_batch : str, default "10000"
        Maximum number of records per Arrow batch.
    jar_path : str, default "jars/ojdbc8.jar"
        Path to the Oracle JDBC driver JAR.
    oracle_fetch_size : int, default 1000
        JDBC fetch size for database queries.

    Environment Variables Required
    ------------------------------
    PRU_DEV_DBNAME : str
        Full PRU_DEV Oracle connection string in the form {host}:{port}/{service}.
    PRU_DEV_USR : str
        PRU_DEV Oracle database username.
    PRU_DEV_PWD : str
        PRU_DEV Oracle database password.

    JARs Required
    ------------------------------
    Download ojdbc8.jar jar from https://www.oracle.com/europe/database/technologies/appdev/jdbc-downloads.html and place it in jar_path. 
    
    Returns
    -------
    tuple[SparkSession, str, dict]
        The Spark session, JDBC URL, and a dictionary of JDBC connection properties.
    """

    spark = (
        SparkSession.builder.appName("oracle-jdbc")
        .config("spark.jars", jar_path) \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC -Doracle.jdbc.timezoneAsRegion=false") \
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC -Doracle.jdbc.timezoneAsRegion=false") \
        .config("spark.executor.memory", executor_memory) \
        .config("spark.driver.memory", driver_memory) \
        .config("spark.executor.cores", executor_cores) \
        .config("spark.executor.instances", executor_instances) \
        .config("spark.sql.shuffle.partitions", shuffle_partitions) \
        .config("spark.default.parallelism", default_parallelism) \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.rpc.message.maxSize", "512") \
        .config("spark.sql.execution.arrow.enabled", "true") \
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", max_records_per_batch) \
        .getOrCreate()
    )

    dbname = os.getenv("PRU_DEV_DBNAME") # PRU_DEV_DBNAME should contain the full {host}:{port}/{service} path
    user = os.getenv("PRU_DEV_USR")
    password = os.getenv("PRU_DEV_PWD")

    url = f"jdbc:oracle:thin:@//{dbname}"
    props = {
        "user": user,
        "password": password,
        "driver": "oracle.jdbc.driver.OracleDriver",
        "fetchSize": oracle_fetch_size,
    }
    return spark, url, props


# ---------------------------------------- Main class ---------------------------------------- #
class EUROCONTROLSpark:
    """
    Spark (PySpark) client for interacting with EUROCONTROL PRISME / NM
    trajectory and airspace profile data stored in Oracle via JDBC.

    The methods return Spark DataFrames where possible. For downstream
    processing that requires Pandas, call `.toPandas()` explicitly.

    Parameters
    ----------
    spark : pyspark.sql.SparkSession, optional
        Existing Spark session. If omitted, a session is created.
    url : str, optional
        JDBC URL for Oracle. If omitted, constructed from environment.
    props : dict, optional
        JDBC properties (user, password, driver, fetchSize). If omitted,
        constructed from environment.

    Notes
    -----
    - Uses Oracle SQL directly via `dbtable="(SELECT ... ) alias"` to leverage
      predicate pushdown and avoid full-table scans where possible.
    - All time handling uses UTC. Provide `wef`/`til` as strings (ISO-8601 or
      'YYYY-MM-DD HH:MM:SS') or as timezone-aware datetimes.
    """

    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        url: Optional[str] = None,
        props: Optional[Dict[str, str]] = None,
    ) -> None:
        self.spark, self.url, self.props = (
            (spark, url, props) if spark and url and props else build_spark_oracle_session()
        )

    # ------------------ Private helpers ------------------ #
    @staticmethod
    def _as_utc(dt: Union[str, datetime]) -> datetime:
        """
        Convert a string or datetime into a timezone-aware UTC datetime.

        Parameters
        ----------
        dt : str | datetime.datetime
            ISO-8601 string (e.g., '2020-01-01T00:00:00Z' or
            '2020-01-01 00:00:00') or a datetime object.

        Returns
        -------
        datetime.datetime
            Timezone-aware UTC datetime.
        """
        if isinstance(dt, str):
            try:
                out = datetime.fromisoformat(dt.replace("Z", "+00:00"))
            except ValueError:
                try:
                    out = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    out = datetime.strptime(dt, "%Y-%m-%d")
        else:
            out = dt
        return out if out.tzinfo else out.replace(tzinfo=timezone.utc)

    @staticmethod
    def _fmt(dt: datetime) -> str:
        """
        Format a datetime to 'YYYY-MM-DD HH:MM:SS' in UTC.

        Parameters
        ----------
        dt : datetime.datetime

        Returns
        -------
        str
            Formatted timestamp string.
        """
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _distance_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        Compute geodesic distance in nautical miles between two coordinates.

        Uses geopy if available; otherwise uses a Haversine approximation.

        Parameters
        ----------
        lat1 : float
            Latitude of the first point in decimal degrees.
        lon1 : float
            Longitude of the first point in decimal degrees.
        lat2 : float
            Latitude of the second point in decimal degrees.
        lon2 : float
            Longitude of the second point in decimal degrees.

        Returns
        -------
        float
            Distance in nautical miles.
        """
        if geodesic is not None:
            return geodesic((lat1, lon1), (lat2, lon2)).nautical
        from math import asin, cos, radians, sin, sqrt

        r_km = 6371.0088
        d_lat = radians(lat2 - lat1)
        d_lon = radians(lon2 - lon1)
        a_val = (
            sin(d_lat / 2) ** 2
            + cos(radians(lat1)) * cos(radians(lat2)) * sin(d_lon / 2) ** 2
        )
        c_val = 2 * asin(sqrt(a_val))
        km = r_km * c_val
        return km * 0.539956803

    def _jdbc_read_sql(self, sql: str) -> DataFrame:
        """
        Execute a SQL string via Oracle JDBC by passing a subquery to dbtable.

        Parameters
        ----------
        sql : str
            SQL query string compatible with Oracle.

        Returns
        -------
        pyspark.sql.DataFrame
        """
        dbtable = f"({sql}) T"
        return self.spark.read.jdbc(url=self.url, table=dbtable, properties=self.props)

    def _in_list(self, values: Iterable[Union[str, int]]) -> str:
        """
        Safely format a small IN list for Oracle SQL.
    
        Parameters
        ----------
        values : Iterable[str | int]
    
        Returns
        -------
        str
            Comma-separated list where strings are single-quoted and any single
            quotes inside values are doubled to avoid SQL injection/breakage.
            Example: ["O'Leary", 42] -> "'O''Leary', 42"
        """
        parts: list[str] = []
        for v in values:
            if isinstance(v, str):
                parts.append("'" + v.replace("'", "''") + "'")
            else:
                parts.append(str(v))
        return ", ".join(parts)


        return ", ".join(q(v) for v in values)

    # ------------------ Table access (Spark) ------------------ #
    def airspace_profile_tbl(self) -> DataFrame:
        """
        Return `FSD.ALL_FT_ASP_PROFILE` as a Spark DataFrame.

        Returns
        -------
        pyspark.sql.DataFrame
        """
        return self.spark.read.jdbc(
            url=self.url, table="FSD.ALL_FT_ASP_PROFILE", properties=self.props
        )

    def point_profile_tbl(self) -> DataFrame:
        """
        Return `FSD.ALL_FT_POINT_PROFILE` as a Spark DataFrame.

        Returns
        -------
        pyspark.sql.DataFrame
        """
        return self.spark.read.jdbc(
            url=self.url, table="FSD.ALL_FT_POINT_PROFILE", properties=self.props
        )

    def airlines_tbl(self) -> DataFrame:
        """
        Return `PRUDEV.V_COVID_DIM_AO` as a Spark DataFrame.

        Returns
        -------
        pyspark.sql.DataFrame
        """
        return self.spark.read.jdbc(
            url=self.url, table="PRUDEV.V_COVID_DIM_AO", properties=self.props
        )

    def apdf_tbl(self) -> DataFrame:
        """
        Return `SWH_FCT.FAC_APDS_FLIGHT_IR691` as a Spark DataFrame.

        Returns
        -------
        pyspark.sql.DataFrame
        """
        return self.spark.read.jdbc(
            url=self.url, table="SWH_FCT.FAC_APDS_FLIGHT_IR691", properties=self.props
        )

    # ------------------ Data methods (Spark SQL via JDBC) ------------------ #
    def flights_tidy(
        self,
        wef: Union[str, datetime],
        til: Union[str, datetime],
        icao_flt_types: Optional[Iterable[str]] = ['S', 'N'],
        ids: Optional[Iterable[Union[int, str]]] = None,
        include_sensitive: bool = False,
        include_military: bool = False,
        include_head: bool = False,
    ) -> DataFrame:
        """
        Build a Spark DataFrame for a clean flights list in a right-open interval.

        Parameters
        ----------
        wef : str | datetime.datetime
            Inclusive start (UTC).
        til : str | datetime.datetime
            Exclusive end (UTC).
        icao_flt_types : Iterable[str], optional
            Keep only these ICAO flight types.
        ids : Iterable[int | str], optional
            Explicit list of flight IDs to include.
        include_sensitive : bool, default False
            If False, exclude SENSITIVE = 'Y'.
        include_military : bool, default False
            If False, exclude SK_FLT_TYPE_RULE_ID = 1 (military).
        include_head : bool, default False
            If False, exclude EXMP_RSN_LH = 'HEAD'.

        Returns
        -------
        pyspark.sql.DataFrame
        """
        wef_str = self._fmt(self._as_utc(wef))
        til_str = self._fmt(self._as_utc(til))

        where_parts = [
            f"TO_DATE('{wef_str}', 'YYYY-MM-DD HH24:MI:SS') <= flt.LOBT",
            f"flt.LOBT < TO_DATE('{til_str}', 'YYYY-MM-DD HH24:MI:SS')",
        ]

        if icao_flt_types:
            where_parts.append(f"flt.ICAO_FLT_TYPE IN ({self._in_list(icao_flt_types)})")

        if ids:
            where_parts.append(f"flt.ID IN ({self._in_list(ids)})")

        if not include_sensitive: 
            where_parts.append("flt.SENSITIVE <> 'Y'")

        if not include_military:
            where_parts.append("flt.SK_FLT_TYPE_RULE_ID <> 1")

        if not include_head:
            where_parts.append("flt.EXMP_RSN_LH <> 'HEAD'")

        where_sql = " AND ".join(where_parts)

        sql = f"""
            SELECT
                flt.ID,
                flt.LOBT,
                flt.AIRCRAFT_ID,
                flt.ADEP,
                apt_adep.PRU_DASHBOARD_AP_NAME AS NAME_ADEP,
                apt_adep.COUNTRY_CODE         AS COUNTRY_CODE_ADEP,
                apt_adep.COUNTRY_NAME         AS COUNTRY_NAME_ADEP,
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
                flt.FLT_UID,
                flt.IOBT,
                flt.FLT_RULES,
                flt.ICAO_FLT_TYPE,
                flt.REGISTRATION,
                flt.AIRCRAFT_ADDRESS,
                flt.AIRCRAFT_TYPE_ICAO_ID,
                flt.WK_TBL_CAT,
                flt.AIRCRAFT_OPERATOR,
                aog.AO_ISO_CTRY_CODE,
                aog.AO_GRP_CODE,
                aog.AO_GRP_NAME,
                flt.EOBT_1,
                flt.ARVT_1,
                flt.TAXI_TIME_1,
                flt.AOBT_3,
                flt.ARVT_3,
                flt.TAXI_TIME_3,
                flt.FLT_DUR_1,
                flt.FLT_DUR_3,
                flt.RTE_LEN_1,
                flt.RTE_LEN_3,
                flt.FLT_TOW
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
            WHERE {where_sql}
        """
        return self._jdbc_read_sql(sql)

    def airlines_tidy(
        self,
        member_state_iso2c: Optional[Iterable[str]] = None,
    ) -> DataFrame:
        """
        Airline info including group affiliation and EU membership flag.

        Parameters
        ----------
        member_state_iso2c : Iterable[str], optional
            ISO 2-letter country codes considered EUROCONTROL Member States.

        Returns
        -------
        pyspark.sql.DataFrame
        """
        base = self.airlines_tbl().select(
            "AO_CODE", "AO_NAME", "AO_GRP_CODE", "AO_GRP_NAME", "AO_ISO_CTRY_CODE"
        )
        if member_state_iso2c:
            ms = {c.upper() for c in member_state_iso2c}
            return base.withColumn(
                "EU", lit("TRUE")
            ).where(col("AO_ISO_CTRY_CODE").isin(list(ms))).unionByName(
                base.withColumn("EU", lit("FALSE")).where(~col("AO_ISO_CTRY_CODE").isin(list(ms)))
            )
        return base.withColumn("EU", lit("FALSE"))

    def apdf_tidy(self, wef: str, til: str) -> DataFrame:
        """
        Extract a clean airport operator data flow list in an interval.

        Parameters
        ----------
        wef : str
            With effect (included) in UTC ('YYYY-MM-DD HH:MM:SS').
        til : str
            Until (excluded) in UTC ('YYYY-MM-DD HH:MM:SS').

        Returns
        -------
        pyspark.sql.DataFrame
        """
        sql = f"""
            SELECT *
            FROM SWH_FCT.FAC_APDS_FLIGHT_IR691 apdf
            WHERE TO_DATE('{wef}','YYYY-MM-DD HH24:MI:SS') <= apdf.MVT_TIME_UTC
              AND apdf.MVT_TIME_UTC < TO_DATE('{til}','YYYY-MM-DD HH24:MI:SS')
              AND TO_DATE('{wef}','YYYY-MM-DD HH24:MI:SS') <= apdf.SRC_DATE_FROM
              AND apdf.SRC_DATE_FROM < TO_DATE('{til}','YYYY-MM-DD HH24:MI:SS')
        """
        df = self._jdbc_read_sql(sql)

        exclude_patterns = ("_MIN", "_IN_FRONT", "_CTFM", "_CPF", "TRANSIT")
        cols_keep = [
            c
            for c in df.columns
            if not any(c.endswith(p) for p in exclude_patterns)
            and not any(p in c for p in exclude_patterns)
        ]
        return df.select(*cols_keep)

    def airspace_profiles_tidy(
        self,
        wef: Union[str, datetime],
        til: Union[str, datetime],
        airspace: str = "FIR",
        profile: str = "CTFM",
    ) -> DataFrame:
        """
        Provide all airspace profile segments intersecting [wef, til).

        Parameters
        ----------
        wef : str | datetime.datetime
            Start of the time window (inclusive, UTC).
        til : str | datetime.datetime
            End of the time window (exclusive, UTC).
        airspace : str, default "FIR"
            Airspace type.
        profile : str, default "CTFM"
            Trajectory model type.

        Returns
        -------
        pyspark.sql.DataFrame
        """
        before_hours, after_hours = 28, 24
        w_dt = self._as_utc(wef)
        t_dt = self._as_utc(til)
        wef_before = self._fmt(w_dt - timedelta(hours=before_hours))
        til_after = self._fmt(t_dt + timedelta(hours=after_hours))
        wef_str = self._fmt(w_dt)
        til_str = self._fmt(t_dt)

        ids_sql = f"""
            SELECT DISTINCT ID
            FROM SWH_FCT.V_FAC_FLIGHT_MS
            WHERE TO_DATE('{wef_before}', 'YYYY-MM-DD HH24:MI:SS') <= LOBT
              AND LOBT < TO_DATE('{til_after}', 'YYYY-MM-DD HH24:MI:SS')
        """
        ids_df = self._jdbc_read_sql(ids_sql).select(col("ID").alias("ID_JOIN"))

        prf_sql = f"""
            SELECT
                SAM_ID,
                SEQ_ID,
                ENTRY_TIME,
                ENTRY_LON,
                ENTRY_LAT,
                ENTRY_FL,
                EXIT_TIME,
                EXIT_LON,
                EXIT_LAT,
                EXIT_FL,
                AIRSPACE_ID,
                AIRSPACE_TYPE,
                MODEL_TYPE
            FROM FSD.ALL_FT_ASP_PROFILE
            WHERE TO_DATE('{wef_before}', 'YYYY-MM-DD HH24:MI:SS') <= LOBT
              AND LOBT < TO_DATE('{til_after}', 'YYYY-MM-DD HH24:MI:SS')
              AND MODEL_TYPE = '{profile}'
              AND AIRSPACE_TYPE = '{airspace}'
              AND ENTRY_LON IS NOT NULL
              AND ENTRY_LAT IS NOT NULL
              AND ENTRY_TIME IS NOT NULL
              AND ENTRY_FL IS NOT NULL
              AND EXIT_LON IS NOT NULL
              AND EXIT_LAT IS NOT NULL
              AND EXIT_TIME IS NOT NULL
              AND EXIT_FL IS NOT NULL
              AND ENTRY_TIME <= TO_DATE('{til_str}', 'YYYY-MM-DD HH24:MI:SS')
              AND TO_DATE('{wef_str}', 'YYYY-MM-DD HH24:MI:SS') < EXIT_TIME
        """
        prf_df = self._jdbc_read_sql(prf_sql).withColumnRenamed("SAM_ID", "ID_JOIN")
        out = prf_df.join(ids_df.dropDuplicates(["ID_JOIN"]), on="ID_JOIN", how="inner")
        return out.withColumnRenamed("ID_JOIN", "ID")

    def flights_airspace_profiles_tidy(
        self,
        wef: Union[str, datetime],
        til: Union[str, datetime],
        airspace: str = "FIR",
        profile: str = "CTFM",
    ) -> DataFrame:
        """
        Extract flights whose airspace profile segments intersect [wef, til).

        Parameters
        ----------
        wef : str | datetime.datetime
            Start of the time window (inclusive, UTC).
        til : str | datetime.datetime
            End of the time window (exclusive, UTC).
        airspace : str, default "FIR"
            Airspace type.
        profile : str, default "CTFM"
            Trajectory model type.

        Returns
        -------
        pyspark.sql.DataFrame
        """
        before_hours, after_hours = 28, 24
        w_dt = self._as_utc(wef)
        t_dt = self._as_utc(til)
        wef_before = self._fmt(w_dt - timedelta(hours=before_hours))
        til_after = self._fmt(t_dt + timedelta(hours=after_hours))

        prf = self.airspace_profiles_tidy(wef, til, airspace, profile).select("ID").dropDuplicates()

        flt_sql = f"""
            SELECT *
            FROM SWH_FCT.V_FAC_FLIGHT_MS
            WHERE TO_DATE('{wef_before}', 'YYYY-MM-DD HH24:MI:SS') <= LOBT
              AND LOBT < TO_DATE('{til_after}', 'YYYY-MM-DD HH24:MI:SS')
        """
        flt_df = self._jdbc_read_sql(flt_sql)

        return flt_df.join(prf, flt_df["ID"] == prf["ID"], how="inner").drop(prf["ID"]).dropDuplicates()

    def export_model_trajectory(
        self,
        wef: Union[str, datetime],
        til: Union[str, datetime],
        profile: str = "CTFM",
        bbox: Optional[Dict[str, float]] = None,
        lobt_buffer: Optional[Dict[str, float]] = None,
        timeover_buffer: Optional[Dict[str, float]] = None,
    ) -> DataFrame:
        """
        Export point profiles for flights in a time window and model type.

        Parameters
        ----------
        wef : str | datetime.datetime
            Start of the time window (inclusive, UTC).
        til : str | datetime.datetime
            End of the time window (exclusive, UTC).
        profile : str, default "CTFM"
            Trajectory model type. One of {"CPF","CTFM","DCT","FTFM","SCR","SRR","SUR"}.
        bbox : dict[str, float], optional
            Axis-aligned bounding box with keys {"xmin","xmax","ymin","ymax"} in degrees.
        lobt_buffer : dict[str, float], optional
            Hours to extend the LOBT window around wef/til, keys {"before","after"}.
        timeover_buffer : dict[str, float], optional
            Hours to extend filtering on TIME_OVER around the window,
            keys {"before","after"}.

        Returns
        -------
        pyspark.sql.DataFrame
            Point profile trajectory rows.
        """
        valid = {"CPF", "CTFM", "DCT", "FTFM", "SCR", "SRR", "SUR"}
        if profile not in valid:
            raise ValueError(f"Invalid profile '{profile}'.")

        w_dt = self._as_utc(wef)
        t_dt = self._as_utc(til)
        wef_iso = w_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        til_iso = t_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        lobt_before = float(lobt_buffer["before"]) if lobt_buffer else 28.0
        lobt_after = float(lobt_buffer["after"]) if lobt_buffer else 24.0

        where_bbox = ""
        if bbox:
            where_bbox = (
                "AND ((:lon_min <= p.LON AND p.LON <= :lon_max) "
                "AND (:lat_min <= p.LAT AND p.LAT <= :lat_max))"
            )

        where_timeover = ""
        if timeover_buffer:
            where_timeover = (
                "AND (((SELECT LOBT_WEF FROM ARGS) - (:to_before / 24) <= p.TIME_OVER) "
                "AND (p.TIME_OVER < (SELECT LOBT_TIL FROM ARGS) + (:to_after / 24)))"
            )

        # Note: Oracle's JDBC source does not allow bind variables via dbtable string;
        # therefore we inline constants safely where needed.
        bbox_clause = ""
        if bbox:
            bbox_clause = (
                f"AND (({float(bbox['xmin'])} <= p.LON AND p.LON <= {float(bbox['xmax'])}) "
                f"AND ({float(bbox['ymin'])} <= p.LAT AND p.LAT <= {float(bbox['ymax'])}))"
            )

        timeover_clause = ""
        if timeover_buffer:
            to_b = float(timeover_buffer["before"])
            to_a = float(timeover_buffer["after"])
            timeover_clause = (
                f"AND ((TO_DATE('{wef_iso}','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') - ({to_b} / 24) "
                f"<= p.TIME_OVER) AND (p.TIME_OVER < "
                f"TO_DATE('{til_iso}','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') + ({to_a} / 24)))"
            )

        sql = f"""
            SELECT P.SAM_ID AS FLIGHT_ID, P.TIME_OVER, P.LON AS LONGITUDE, P.LAT AS LATITUDE,
                   P.FLIGHT_LEVEL, P.POINT_ID, P.AIR_ROUTE, P.LOBT, P.SEQ_ID,
                   F.AIRCRAFT_ID AS CALLSIGN, F.REGISTRATION, P.MODEL_TYPE,
                   F.AIRCRAFT_TYPE_ICAO_ID AS AIRCRAFT_TYPE, F.AIRCRAFT_OPERATOR,
                   F.AIRCRAFT_ADDRESS AS ICAO24, F.ADEP, F.ADES
            FROM FSD.ALL_FT_POINT_PROFILE P
            JOIN FLX.FLIGHT F ON (F.ID = P.SAM_ID AND F.LOBT = P.LOBT)
            WHERE F.LOBT >= TO_DATE('{wef_iso}','YYYY-MM-DD"T"HH24:MI:SS"Z"') - ({lobt_before} / 24)
              AND F.LOBT <  TO_DATE('{til_iso}','YYYY-MM-DD"T"HH24:MI:SS"Z"') + ({lobt_after} / 24)
              AND P.LOBT >= TO_DATE('{wef_iso}','YYYY-MM-DD"T"HH24:MI:SS"Z"') - ({lobt_before} / 24)
              AND P.LOBT <  TO_DATE('{til_iso}','YYYY-MM-DD"T"HH24:MI:SS"Z"') + ({lobt_after} / 24)
              AND P.MODEL_TYPE = '{profile}'
              AND P.LON IS NOT NULL
              AND P.LAT IS NOT NULL
              AND P.TIME_OVER IS NOT NULL
              {bbox_clause}
              {timeover_clause}
        """
        return self._jdbc_read_sql(sql).fillna({"POINT_ID": "NO_POINT", "AIR_ROUTE": "NO_ROUTE"})

    def point_profiles_tidy(
        self,
        wef: Union[str, datetime],
        til: Optional[Union[str, datetime]] = None,
        profile: str = "CTFM",
        bbox: Optional[Dict[str, float]] = None,
    ) -> DataFrame:
        """
        Convenience wrapper to export point profiles for a window/model.

        Parameters
        ----------
        wef : str | datetime.datetime
            Start of the time window (inclusive, UTC).
        til : str | datetime.datetime, optional
            End of the time window (exclusive, UTC). If None, defaults to
            current UTC midnight.
        profile : str, default "CTFM"
            Trajectory model type.
        bbox : dict[str, float], optional
            Axis-aligned bounding box.

        Returns
        -------
        pyspark.sql.DataFrame
        """
        if til is None:
            til = datetime.utcnow().replace(
                hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc
            )
        return self.export_model_trajectory(
            wef,
            til,
            profile,
            bbox,
            lobt_buffer={"before": 28, "after": 24},
            timeover_buffer=None,
        )

    # ------------------ SO6 generation (Pandas) ------------------ #
    def generate_so6(self, trajectory: pd.DataFrame) -> pd.DataFrame:
        """
        Convert point profile trajectories into SO6 segment format.

        Parameters
        ----------
        trajectory : pandas.DataFrame
            Point profile dataframe with columns:
            FLIGHT_ID, TIME_OVER, LONGITUDE, LATITUDE, FLIGHT_LEVEL,
            POINT_ID, AIR_ROUTE, LOBT, SEQ_ID, CALLSIGN, REGISTRATION,
            MODEL_TYPE, AIRCRAFT_TYPE, AIRCRAFT_OPERATOR, ADEP, ADES.

        Returns
        -------
        pandas.DataFrame
            SO6-formatted segments with distance, status, and metadata.

        Raises
        ------
        ValueError
            If required columns are missing.
        """
        required_cols = [
            "FLIGHT_ID",
            "TIME_OVER",
            "LONGITUDE",
            "LATITUDE",
            "FLIGHT_LEVEL",
            "POINT_ID",
            "AIR_ROUTE",
            "LOBT",
            "SEQ_ID",
            "CALLSIGN",
            "REGISTRATION",
            "MODEL_TYPE",
            "AIRCRAFT_TYPE",
            "AIRCRAFT_OPERATOR",
            "ADEP",
            "ADES",
        ]
        missing = [c for c in required_cols if c not in trajectory.columns]
        if missing:
            raise ValueError(f"Missing columns: {missing}")

        if not pd.api.types.is_datetime64_any_dtype(trajectory["TIME_OVER"]):
            trajectory = trajectory.copy()
            trajectory["TIME_OVER"] = pd.to_datetime(
                trajectory["TIME_OVER"], utc=True, errors="coerce"
            )

        rows = []
        for flight_id, grp in trajectory.groupby("FLIGHT_ID", sort=True):
            grp = grp.sort_values("TIME_OVER").reset_index(drop=True)
            n_points = len(grp)
            for i in range(n_points if n_points == 1 else n_points - 1):
                start = grp.iloc[i]
                end = grp.iloc[i if n_points == 1 else i + 1]
                fl_begin = int(start["FLIGHT_LEVEL"])
                fl_end = int(end["FLIGHT_LEVEL"])
                status = 0 if fl_begin < fl_end else (2 if fl_begin == fl_end else 1)
                seg_len_nm = self._distance_nm(
                    float(start["LATITUDE"]),
                    float(start["LONGITUDE"]),
                    float(end["LATITUDE"]),
                    float(end["LONGITUDE"]),
                )
                rows.append(
                    {
                        "SEGMENT_ID": f"{start['POINT_ID']}_{end['POINT_ID']}",
                        "ADEP": start["ADEP"],
                        "ADES": start["ADES"],
                        "AIRCRAFT_TYPE": start["AIRCRAFT_TYPE"],
                        "SEGMENT_HHMM_BEGIN": start["TIME_OVER"].strftime("%H%M%S"),
                        "SEGMENT_HHMM_END": end["TIME_OVER"].strftime("%H%M%S"),
                        "SEGMENT_FL_BEGIN": fl_begin,
                        "SEGMENT_FL_END": fl_end,
                        "STATUS": status,
                        "CALLSIGN": start["CALLSIGN"],
                        "SEGMENT_DATE_BEGIN": start["TIME_OVER"].strftime("%y%m%d"),
                        "SEGMENT_DATE_END": end["TIME_OVER"].strftime("%y%m%d"),
                        "SEGMENT_LATITUDE_BEGIN": float(start["LATITUDE"]) * 60,
                        "SEGMENT_LONGITUDE_BEGIN": float(start["LONGITUDE"]) * 60,
                        "SEGMENT_LATITUDE_END": float(end["LATITUDE"]) * 60,
                        "SEGMENT_LONGITUDE_END": float(end["LONGITUDE"]) * 60,
                        "FLIGHT_ID": flight_id,
                        "SEQUENCE": i + 1,
                        "SEGMENT_LENGTH": float(seg_len_nm),
                        "SEGMENT_PARITY": 0,
                    }
                )
        return (
            pd.DataFrame(rows)
            .sort_values(["FLIGHT_ID", "SEQUENCE"])
            .reset_index(drop=True)
        )
