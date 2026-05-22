"""
Abstract base class shared by EUROCONTROLSpark and EUROCONTROLpy.

All SQL-building logic lives here so both backends stay in sync.
Concrete subclasses only need to implement _execute_query, _execute_table,
and _select_columns; every data-access method is inherited for free.
"""
from __future__ import annotations

import io
from abc import ABC, abstractmethod
from datetime import date, datetime, timedelta, timezone
from math import asin, cos, radians, sin, sqrt
from typing import Any, Dict, Iterable, Optional, Union

import pandas as pd

try:
    from geopy.distance import geodesic
except ImportError:
    geodesic = None


class _EUROCONTROLBase(ABC):
    """
    Shared logic for Oracle PRISME/NM data access.

    Subclasses must implement:
      - _execute_query(sql)   -> backend DataFrame
      - _execute_table(table) -> backend DataFrame
      - _select_columns(df, cols) -> backend DataFrame
    """

    # ──────────────────────────── static helpers ─────────────────────────────

    @staticmethod
    def _as_utc(dt: Union[str, datetime]) -> datetime:
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
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _distance_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        if geodesic is not None:
            return geodesic((lat1, lon1), (lat2, lon2)).nautical
        r_km = 6371.0088
        d_lat = radians(lat2 - lat1)
        d_lon = radians(lon2 - lon1)
        a = (
            sin(d_lat / 2) ** 2
            + cos(radians(lat1)) * cos(radians(lat2)) * sin(d_lon / 2) ** 2
        )
        return r_km * 2 * asin(sqrt(a)) * 0.539956803

    @staticmethod
    def _in_list(values: Iterable[Union[str, int]]) -> str:
        parts: list[str] = []
        for v in values:
            if isinstance(v, str):
                parts.append("'" + v.replace("'", "''") + "'")
            else:
                parts.append(str(v))
        return ", ".join(parts)

    # ──────────────────────────── abstract layer ─────────────────────────────

    @abstractmethod
    def _execute_query(self, sql: str) -> Any:
        """Execute a SQL string and return a backend-native DataFrame."""
        ...

    @abstractmethod
    def _execute_table(self, table: str) -> Any:
        """Return an entire Oracle table as a backend-native DataFrame."""
        ...

    def _select_columns(self, df: Any, cols: list[str]) -> Any:
        """Select columns from a backend DataFrame. Override for non-pandas backends."""
        return df[cols]

    # ──────────────────────────── SQL builders ───────────────────────────────

    def _build_flights_sql(
        self,
        wef_str: str,
        til_str: str,
        icao_flt_types: Optional[Iterable[str]] = None,
        ids: Optional[Iterable[Union[int, str]]] = None,
        include_sensitive: bool = False,
        include_military: bool = False,
        include_head: bool = False,
        extra_cols: Optional[list[str]] = None,
    ) -> str:
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

        extra_select = ""
        if extra_cols:
            extra_select = ",\n                " + ",\n                ".join(
                f"flt.{c}" for c in extra_cols
            )

        return f"""
            SELECT
                flt.ID,
                flt.LOBT,
                flt.AIRCRAFT_ID,
                flt.ADEP,
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
                flt.FLT_TOW{extra_select}
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
            WHERE {' AND '.join(where_parts)}
        """

    def _build_adrr_sql(self, wef_date: str, til_date: str) -> str:
        return f"""
            SELECT
                f.ID                    AS "ECTRL ID",
                f.ADEP                  AS "ADEP",
                ad.LATITUDE             AS "ADEP Latitude",
                ad.LONGITUDE            AS "ADEP Longitude",
                f.ADES                  AS "ADES",
                aa.LATITUDE             AS "ADES Latitude",
                aa.LONGITUDE            AS "ADES Longitude",
                f.LOBT                  AS "FILED OFF BLOCK TIME",
                f.ARVT_1                AS "FILED ARRIVAL TIME",
                f.AOBT_3                AS "ACTUAL OFF BLOCK TIME",
                f.ARVT_3                AS "ACTUAL ARRIVAL TIME",
                f.AIRCRAFT_TYPE_ICAO_ID AS "AC Type",
                CASE WHEN ao.LAST_ICAO_VERSION_NUMBER IS NOT NULL
                     THEN ao.ICAO_OP_CODE ELSE 'ZZZ' END AS "AC Operator",
                f.CORR_REGISTRATION     AS "AC Registration",
                f.ICAO_FLT_TYPE         AS "ICAO Flight Type",
                r.RULE_DESCRIPTION      AS "STATFOR Market Segment",
                f.FL_REQ                AS "Requested FL",
                f.RTE_LEN_3             AS "Actual Distance Flown (nm)"
            FROM SWH_FCT.V_FAC_FLIGHT_MS f
            INNER JOIN SWH_FCT.DIM_FLIGHT_TYPE_RULE r
                ON f.SK_FLT_TYPE_RULE_ID = r.SK_FLT_TYPE_RULE_ID
            LEFT JOIN SWH_FCT.DIM_OPERATOR ao
                ON f.SK_OP_ID = ao.SK_OP_ID
            LEFT JOIN SWH_FCT.DIM_AIRPORT ad
                ON f.ADEP = ad.EC_AP_CODE
               AND f.LOBT >= ad.VALID_FROM
               AND f.LOBT < ad.VALID_TO
            LEFT JOIN SWH_FCT.DIM_AIRPORT aa
                ON f.ADES = aa.EC_AP_CODE
               AND f.LOBT >= aa.VALID_FROM
               AND f.LOBT < aa.VALID_TO
            WHERE TO_DATE('{wef_date}', 'YYYY-MM-DD') <= f.LOBT
              AND f.LOBT < TO_DATE('{til_date}', 'YYYY-MM-DD')
              AND f.ICAO_FLT_TYPE IN ('S', 'N')
              AND r.RULE_DESCRIPTION <> 'Military'
            ORDER BY f.LOBT, f.ID
        """

    def _build_apdf_sql(self, wef_str: str, til_str: str) -> str:
        return f"""
            SELECT *
            FROM SWH_FCT.FAC_APDS_FLIGHT_IR691 apdf
            WHERE TO_DATE('{wef_str}', 'YYYY-MM-DD HH24:MI:SS') <= apdf.MVT_TIME_UTC
              AND apdf.MVT_TIME_UTC < TO_DATE('{til_str}', 'YYYY-MM-DD HH24:MI:SS')
              AND TO_DATE('{wef_str}', 'YYYY-MM-DD HH24:MI:SS') <= apdf.SRC_DATE_FROM
              AND apdf.SRC_DATE_FROM < TO_DATE('{til_str}', 'YYYY-MM-DD HH24:MI:SS')
        """

    def _build_airlines_sql(self, member_state_iso2c: Optional[list[str]] = None) -> str:
        if member_state_iso2c:
            ms = [c.upper() for c in member_state_iso2c]
            eu_expr = f"CASE WHEN AO_ISO_CTRY_CODE IN ({self._in_list(ms)}) THEN 'TRUE' ELSE 'FALSE' END"
        else:
            eu_expr = "'FALSE'"
        return f"""
            SELECT
                AO_CODE,
                AO_NAME,
                AO_GRP_CODE,
                AO_GRP_NAME,
                AO_ISO_CTRY_CODE,
                {eu_expr} AS EU
            FROM PRUDEV.V_COVID_DIM_AO
        """

    def _build_export_model_trajectory_sql(
        self,
        wef_iso: str,
        til_iso: str,
        profile: str,
        bbox: Optional[Dict[str, float]] = None,
        lobt_before: float = 28.0,
        lobt_after: float = 24.0,
        timeover_buffer: Optional[Dict[str, float]] = None,
    ) -> str:
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
                f"AND (TO_DATE('{wef_iso}','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') - ({to_b} / 24) <= p.TIME_OVER "
                f"AND p.TIME_OVER < TO_DATE('{til_iso}','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') + ({to_a} / 24))"
            )

        return f"""
            SELECT
                p.SAM_ID                 AS FLIGHT_ID,
                p.TIME_OVER,
                p.LON                    AS LONGITUDE,
                p.LAT                    AS LATITUDE,
                p.FLIGHT_LEVEL,
                p.POINT_ID,
                p.AIR_ROUTE,
                p.LOBT,
                p.SEQ_ID,
                f.AIRCRAFT_ID            AS CALLSIGN,
                f.REGISTRATION,
                p.MODEL_TYPE,
                f.AIRCRAFT_TYPE_ICAO_ID  AS AIRCRAFT_TYPE,
                f.AIRCRAFT_OPERATOR,
                f.AIRCRAFT_ADDRESS       AS ICAO24,
                f.ADEP,
                f.ADES
            FROM FSD.ALL_FT_POINT_PROFILE p
            JOIN FLX.FLIGHT f ON (f.ID = p.SAM_ID AND f.LOBT = p.LOBT)
            WHERE f.LOBT >= TO_DATE('{wef_iso}','YYYY-MM-DD"T"HH24:MI:SS"Z"') - ({lobt_before} / 24)
              AND f.LOBT <  TO_DATE('{til_iso}','YYYY-MM-DD"T"HH24:MI:SS"Z"') + ({lobt_after}  / 24)
              AND p.LOBT >= TO_DATE('{wef_iso}','YYYY-MM-DD"T"HH24:MI:SS"Z"') - ({lobt_before} / 24)
              AND p.LOBT <  TO_DATE('{til_iso}','YYYY-MM-DD"T"HH24:MI:SS"Z"') + ({lobt_after}  / 24)
              AND p.MODEL_TYPE = '{profile}'
              AND p.LON IS NOT NULL
              AND p.LAT IS NOT NULL
              AND p.TIME_OVER IS NOT NULL
              {bbox_clause}
              {timeover_clause}
        """

    def _build_airspace_profiles_sql(
        self,
        wef_before: str,
        til_after: str,
        wef_str: str,
        til_str: str,
        airspace: str,
        profile: str,
    ) -> str:
        """
        Single-query version using an IN subquery. Used by default in both backends.
        EUROCONTROLSpark overrides airspace_profiles_tidy to use the two-query approach
        which can be more efficient on very large Spark jobs.
        """
        return f"""
            SELECT
                asp.SAM_ID     AS ID,
                asp.SEQ_ID,
                asp.ENTRY_TIME,
                asp.ENTRY_LON,
                asp.ENTRY_LAT,
                asp.ENTRY_FL,
                asp.EXIT_TIME,
                asp.EXIT_LON,
                asp.EXIT_LAT,
                asp.EXIT_FL,
                asp.AIRSPACE_ID,
                asp.AIRSPACE_TYPE,
                asp.MODEL_TYPE
            FROM FSD.ALL_FT_ASP_PROFILE asp
            WHERE asp.LOBT >= TO_DATE('{wef_before}', 'YYYY-MM-DD HH24:MI:SS')
              AND asp.LOBT <  TO_DATE('{til_after}',  'YYYY-MM-DD HH24:MI:SS')
              AND asp.MODEL_TYPE    = '{profile}'
              AND asp.AIRSPACE_TYPE = '{airspace}'
              AND asp.ENTRY_LON  IS NOT NULL
              AND asp.ENTRY_LAT  IS NOT NULL
              AND asp.ENTRY_TIME IS NOT NULL
              AND asp.ENTRY_FL   IS NOT NULL
              AND asp.EXIT_LON   IS NOT NULL
              AND asp.EXIT_LAT   IS NOT NULL
              AND asp.EXIT_TIME  IS NOT NULL
              AND asp.EXIT_FL    IS NOT NULL
              AND asp.ENTRY_TIME <= TO_DATE('{til_str}', 'YYYY-MM-DD HH24:MI:SS')
              AND TO_DATE('{wef_str}', 'YYYY-MM-DD HH24:MI:SS') < asp.EXIT_TIME
              AND asp.SAM_ID IN (
                  SELECT DISTINCT ID
                  FROM SWH_FCT.V_FAC_FLIGHT_MS
                  WHERE LOBT >= TO_DATE('{wef_before}', 'YYYY-MM-DD HH24:MI:SS')
                    AND LOBT <  TO_DATE('{til_after}',  'YYYY-MM-DD HH24:MI:SS')
              )
        """

    def _build_flights_airspace_profiles_sql(
        self,
        wef_before: str,
        til_after: str,
        wef_str: str,
        til_str: str,
        airspace: str,
        profile: str,
    ) -> str:
        return f"""
            SELECT DISTINCT flt.*
            FROM (
                SELECT
                    flt2.ID, flt2.LOBT, flt2.AIRCRAFT_ID, flt2.ADEP,
                    apt_adep.PRU_DASHBOARD_AP_NAME AS NAME_ADEP,
                    apt_adep.COUNTRY_CODE          AS COUNTRY_CODE_ADEP,
                    apt_adep.COUNTRY_NAME          AS COUNTRY_NAME_ADEP,
                    flt2.ADES,
                    apt_ades.PRU_DASHBOARD_AP_NAME AS NAME_ADES,
                    apt_ades.COUNTRY_CODE          AS COUNTRY_CODE_ADES,
                    apt_ades.COUNTRY_NAME          AS COUNTRY_NAME_ADES,
                    flt2.ADES_FILED,
                    apt_ades_f.PRU_DASHBOARD_AP_NAME AS NAME_ADES_FILED,
                    apt_ades_f.COUNTRY_CODE          AS COUNTRY_CODE_ADES_FILED,
                    apt_ades_f.COUNTRY_NAME          AS COUNTRY_NAME_ADES_FILED,
                    flt2.SENSITIVE,
                    flt2.EXMP_RSN_LH AS SPECIAL_EXEMPT,
                    frl.RULE_NAME,
                    flt2.FLT_UID, flt2.IOBT, flt2.FLT_RULES, flt2.ICAO_FLT_TYPE,
                    flt2.REGISTRATION, flt2.AIRCRAFT_ADDRESS, flt2.AIRCRAFT_TYPE_ICAO_ID,
                    flt2.WK_TBL_CAT, flt2.AIRCRAFT_OPERATOR,
                    aog.AO_ISO_CTRY_CODE, aog.AO_GRP_CODE, aog.AO_GRP_NAME,
                    flt2.EOBT_1, flt2.ARVT_1, flt2.TAXI_TIME_1,
                    flt2.AOBT_3, flt2.ARVT_3, flt2.TAXI_TIME_3,
                    flt2.FLT_DUR_1, flt2.FLT_DUR_3,
                    flt2.RTE_LEN_1, flt2.RTE_LEN_3, flt2.FLT_TOW
                FROM SWH_FCT.V_FAC_FLIGHT_MS flt2
                LEFT JOIN SWH_FCT.DIM_FLIGHT_TYPE_RULE frl
                    ON flt2.SK_FLT_TYPE_RULE_ID = frl.SK_FLT_TYPE_RULE_ID
                LEFT JOIN PRUDEV.V_COVID_DIM_AO aog
                    ON flt2.AIRCRAFT_OPERATOR = aog.AO_CODE
                LEFT JOIN PRUDEV.V_COVID_REL_AIRPORT_AREA apt_adep
                    ON flt2.ADEP = apt_adep.CFMU_AP_CODE
                LEFT JOIN PRUDEV.V_COVID_REL_AIRPORT_AREA apt_ades
                    ON flt2.ADES = apt_ades.CFMU_AP_CODE
                LEFT JOIN PRUDEV.V_COVID_REL_AIRPORT_AREA apt_ades_f
                    ON flt2.ADES_FILED = apt_ades_f.CFMU_AP_CODE
                WHERE flt2.LOBT >= TO_DATE('{wef_before}', 'YYYY-MM-DD HH24:MI:SS')
                  AND flt2.LOBT <  TO_DATE('{til_after}',  'YYYY-MM-DD HH24:MI:SS')
            ) flt
            WHERE flt.ID IN (
                SELECT DISTINCT asp.SAM_ID
                FROM FSD.ALL_FT_ASP_PROFILE asp
                WHERE asp.LOBT >= TO_DATE('{wef_before}', 'YYYY-MM-DD HH24:MI:SS')
                  AND asp.LOBT <  TO_DATE('{til_after}',  'YYYY-MM-DD HH24:MI:SS')
                  AND asp.MODEL_TYPE    = '{profile}'
                  AND asp.AIRSPACE_TYPE = '{airspace}'
                  AND asp.ENTRY_LON  IS NOT NULL
                  AND asp.ENTRY_LAT  IS NOT NULL
                  AND asp.ENTRY_TIME IS NOT NULL
                  AND asp.ENTRY_FL   IS NOT NULL
                  AND asp.EXIT_LON   IS NOT NULL
                  AND asp.EXIT_LAT   IS NOT NULL
                  AND asp.EXIT_TIME  IS NOT NULL
                  AND asp.EXIT_FL    IS NOT NULL
                  AND asp.ENTRY_TIME <= TO_DATE('{til_str}', 'YYYY-MM-DD HH24:MI:SS')
                  AND TO_DATE('{wef_str}', 'YYYY-MM-DD HH24:MI:SS') < asp.EXIT_TIME
                  AND asp.SAM_ID IN (
                      SELECT DISTINCT ID
                      FROM SWH_FCT.V_FAC_FLIGHT_MS
                      WHERE LOBT >= TO_DATE('{wef_before}', 'YYYY-MM-DD HH24:MI:SS')
                        AND LOBT <  TO_DATE('{til_after}',  'YYYY-MM-DD HH24:MI:SS')
                  )
            )
        """

    def _build_export_airports_sql(self, wef_date: str, til_date: str) -> str:
        return f"""
            SELECT *
            FROM SWH_FCT.DIM_AIRPORT
            WHERE VALID_FROM <= TO_DATE('{wef_date}', 'YYYY-MM-DD')
              AND TO_DATE('{til_date}', 'YYYY-MM-DD') <= VALID_TO
        """

    # ──────────────────────────── shared data methods ────────────────────────

    def flights_tbl(self) -> Any:
        """Return `SWH_FCT.V_FAC_FLIGHT_MS` as a backend-native DataFrame."""
        return self._execute_table("SWH_FCT.V_FAC_FLIGHT_MS")

    def airlines_tbl(self) -> Any:
        """Return `PRUDEV.V_COVID_DIM_AO` as a backend-native DataFrame."""
        return self._execute_table("PRUDEV.V_COVID_DIM_AO")

    def apdf_tbl(self) -> Any:
        """Return `SWH_FCT.FAC_APDS_FLIGHT_IR691` as a backend-native DataFrame."""
        return self._execute_table("SWH_FCT.FAC_APDS_FLIGHT_IR691")

    def airspace_profile_tbl(self) -> Any:
        """Return `FSD.ALL_FT_ASP_PROFILE` as a backend-native DataFrame."""
        return self._execute_table("FSD.ALL_FT_ASP_PROFILE")

    def point_profile_tbl(self) -> Any:
        """Return `FSD.ALL_FT_POINT_PROFILE` as a backend-native DataFrame."""
        return self._execute_table("FSD.ALL_FT_POINT_PROFILE")

    def flights_tidy(
        self,
        wef: Union[str, datetime],
        til: Union[str, datetime],
        icao_flt_types: Optional[Iterable[str]] = ("S", "N"),
        ids: Optional[Iterable[Union[int, str]]] = None,
        include_sensitive: bool = False,
        include_military: bool = False,
        include_head: bool = False,
        extra_cols: Optional[list[str]] = None,
    ) -> Any:
        """
        Clean flight list for the interval [wef, til).

        Parameters
        ----------
        wef : str | datetime
            Inclusive start (UTC).
        til : str | datetime
            Exclusive end (UTC).
        icao_flt_types : iterable of str, default ('S', 'N')
            ICAO flight type filter. Pass None to include all.
        ids : iterable of int | str, optional
            Restrict to specific SAM IDs.
        include_sensitive : bool, default False
        include_military : bool, default False
        include_head : bool, default False
        extra_cols : list of str, optional
            Additional columns to pull from V_FAC_FLIGHT_MS.
        """
        wef_str = self._fmt(self._as_utc(wef))
        til_str = self._fmt(self._as_utc(til))
        sql = self._build_flights_sql(
            wef_str, til_str, icao_flt_types, ids,
            include_sensitive, include_military, include_head, extra_cols,
        )
        return self._execute_query(sql)

    def adrr_flights_tidy(
        self,
        wef: Union[str, datetime],
        til: Union[str, datetime],
    ) -> Any:
        """
        ADRR (Aviation Data Repository for Research) flight list.

        Returns only S/N (scheduled/non-scheduled, non-military) flights with
        column names matching the ADRR manual specification.
        """
        wef_date = self._as_utc(wef).strftime("%Y-%m-%d")
        til_date = self._as_utc(til).strftime("%Y-%m-%d")
        return self._execute_query(self._build_adrr_sql(wef_date, til_date))

    def airlines_tidy(
        self,
        member_state_iso2c: Optional[Iterable[str]] = None,
    ) -> Any:
        """
        Airline info with EU membership flag.

        Parameters
        ----------
        member_state_iso2c : iterable of str, optional
            ISO 2-letter codes of EUROCONTROL Member States used to set the EU
            column. If None, the bundled member_state dataset is used.
        """
        if member_state_iso2c is None:
            try:
                from .data import member_state
                member_state_iso2c = list(member_state["iso2c"])
            except Exception:
                member_state_iso2c = []
        return self._execute_query(
            self._build_airlines_sql(list(member_state_iso2c))
        )

    def apdf_tidy(
        self,
        wef: Union[str, datetime],
        til: Union[str, datetime],
    ) -> Any:
        """
        Clean airport operator data flow (APDF) list for [wef, til).

        Note: can only reliably cover one month at a time.
        """
        wef_str = self._fmt(self._as_utc(wef))
        til_str = self._fmt(self._as_utc(til))
        df = self._execute_query(self._build_apdf_sql(wef_str, til_str))
        exclude = ("_MIN", "_IN_FRONT", "_CTFM", "_CPF", "TRANSIT")
        cols_keep = [
            c for c in df.columns
            if not any(c.endswith(p) or p in c for p in exclude)
        ]
        return self._select_columns(df, cols_keep)

    def airspace_profiles_tidy(
        self,
        wef: Union[str, datetime],
        til: Union[str, datetime],
        airspace: str = "FIR",
        profile: str = "CTFM",
    ) -> Any:
        """
        All airspace profile segments intersecting [wef, til).

        Parameters
        ----------
        wef, til : str | datetime
            UTC time window (right-open).
        airspace : str, default 'FIR'
            One of 'FIR', 'NAS', 'AUA', 'ES'.
        profile : str, default 'CTFM'
            One of 'FTFM', 'RTFM', 'CTFM', 'CPF', 'DCT', 'SCR', 'SRR', 'SUR'.
        """
        w_dt = self._as_utc(wef)
        t_dt = self._as_utc(til)
        wef_before = self._fmt(w_dt - timedelta(hours=28))
        til_after = self._fmt(t_dt + timedelta(hours=24))
        wef_str = self._fmt(w_dt)
        til_str = self._fmt(t_dt)
        sql = self._build_airspace_profiles_sql(
            wef_before, til_after, wef_str, til_str, airspace, profile
        )
        return self._execute_query(sql)

    def flights_airspace_profiles_tidy(
        self,
        wef: Union[str, datetime],
        til: Union[str, datetime],
        airspace: str = "FIR",
        profile: str = "CTFM",
    ) -> Any:
        """
        Flights whose airspace profile segments intersect [wef, til).

        Returns the same columns as flights_tidy.
        """
        w_dt = self._as_utc(wef)
        t_dt = self._as_utc(til)
        wef_before = self._fmt(w_dt - timedelta(hours=28))
        til_after = self._fmt(t_dt + timedelta(hours=24))
        wef_str = self._fmt(w_dt)
        til_str = self._fmt(t_dt)
        sql = self._build_flights_airspace_profiles_sql(
            wef_before, til_after, wef_str, til_str, airspace, profile
        )
        return self._execute_query(sql)

    def export_model_trajectory(
        self,
        wef: Union[str, datetime],
        til: Union[str, datetime],
        profile: str = "CTFM",
        bbox: Optional[Dict[str, float]] = None,
        lobt_buffer: Optional[Dict[str, float]] = None,
        timeover_buffer: Optional[Dict[str, float]] = None,
    ) -> Any:
        """
        Point profiles for flights in [wef, til) for the given trajectory model.

        Parameters
        ----------
        profile : str, default 'CTFM'
            One of 'CPF', 'CTFM', 'DCT', 'FTFM', 'SCR', 'SRR', 'SUR'.
        bbox : dict, optional
            Keys 'xmin', 'xmax', 'ymin', 'ymax' in decimal degrees.
        lobt_buffer : dict, optional
            Keys 'before' and 'after' (hours). Defaults to 28/24.
        timeover_buffer : dict, optional
            Keys 'before' and 'after' (hours) to filter TIME_OVER.
        """
        valid = {"CPF", "CTFM", "DCT", "FTFM", "SCR", "SRR", "SUR"}
        if profile not in valid:
            raise ValueError(f"Invalid profile '{profile}'. Choose from {valid}.")

        w_dt = self._as_utc(wef)
        t_dt = self._as_utc(til)
        wef_iso = w_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        til_iso = t_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        lobt_before = float(lobt_buffer["before"]) if lobt_buffer else 28.0
        lobt_after = float(lobt_buffer["after"]) if lobt_buffer else 24.0

        sql = self._build_export_model_trajectory_sql(
            wef_iso, til_iso, profile, bbox, lobt_before, lobt_after, timeover_buffer
        )
        df = self._execute_query(sql)
        return self._fillna_trajectory(df)

    def _fillna_trajectory(self, df: Any) -> Any:
        """Fill NULLs in trajectory result. Override in Spark subclass."""
        if hasattr(df, "fillna"):
            # pandas path
            return df.fillna({"POINT_ID": "NO_POINT", "AIR_ROUTE": "NO_ROUTE"})
        return df

    def point_profiles_tidy(
        self,
        wef: Union[str, datetime],
        til: Optional[Union[str, datetime]] = None,
        profile: str = "CTFM",
        bbox: Optional[Dict[str, float]] = None,
    ) -> Any:
        """
        Convenience wrapper for export_model_trajectory with default LOBT buffers.

        til defaults to current UTC midnight when omitted.
        """
        if til is None:
            til = datetime.utcnow().replace(
                hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc
            )
        return self.export_model_trajectory(
            wef, til, profile, bbox,
            lobt_buffer={"before": 28, "after": 24},
        )

    def export_airports(
        self,
        wef: Union[str, datetime],
        til: Union[str, datetime],
    ) -> Any:
        """
        Airports from SWH_FCT.DIM_AIRPORT that are valid in [wef, til).
        """
        wef_date = self._as_utc(wef).strftime("%Y-%m-%d")
        til_date = self._as_utc(til).strftime("%Y-%m-%d")
        return self._execute_query(
            self._build_export_airports_sql(wef_date, til_date)
        )

    # ──────────────────────────── SO6 (pandas, backend-independent) ──────────

    def generate_so6(self, trajectory: pd.DataFrame) -> pd.DataFrame:
        """
        Convert point profile trajectories to SO6 segment format.

        Parameters
        ----------
        trajectory : pandas.DataFrame
            Must contain: FLIGHT_ID, TIME_OVER, LONGITUDE, LATITUDE,
            FLIGHT_LEVEL, POINT_ID, AIR_ROUTE, LOBT, SEQ_ID, CALLSIGN,
            REGISTRATION, MODEL_TYPE, AIRCRAFT_TYPE, AIRCRAFT_OPERATOR,
            ADEP, ADES.
        """
        required = [
            "FLIGHT_ID", "TIME_OVER", "LONGITUDE", "LATITUDE", "FLIGHT_LEVEL",
            "POINT_ID", "AIR_ROUTE", "LOBT", "SEQ_ID", "CALLSIGN", "REGISTRATION",
            "MODEL_TYPE", "AIRCRAFT_TYPE", "AIRCRAFT_OPERATOR", "ADEP", "ADES",
        ]
        missing = [c for c in required if c not in trajectory.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        if not pd.api.types.is_datetime64_any_dtype(trajectory["TIME_OVER"]):
            trajectory = trajectory.copy()
            trajectory["TIME_OVER"] = pd.to_datetime(
                trajectory["TIME_OVER"], utc=True, errors="coerce"
            )

        rows = []
        for flight_id, grp in trajectory.groupby("FLIGHT_ID", sort=True):
            grp = grp.sort_values("TIME_OVER").reset_index(drop=True)
            n = len(grp)
            for i in range(n if n == 1 else n - 1):
                s = grp.iloc[i]
                e = grp.iloc[i if n == 1 else i + 1]
                fl_begin = int(s["FLIGHT_LEVEL"])
                fl_end = int(e["FLIGHT_LEVEL"])
                status = 0 if fl_begin < fl_end else (2 if fl_begin == fl_end else 1)
                rows.append({
                    "SEGMENT_ID":              f"{s['POINT_ID']}_{e['POINT_ID']}",
                    "ADEP":                    s["ADEP"],
                    "ADES":                    s["ADES"],
                    "AIRCRAFT_TYPE":           s["AIRCRAFT_TYPE"],
                    "SEGMENT_HHMM_BEGIN":      s["TIME_OVER"].strftime("%H%M%S"),
                    "SEGMENT_HHMM_END":        e["TIME_OVER"].strftime("%H%M%S"),
                    "SEGMENT_FL_BEGIN":        fl_begin,
                    "SEGMENT_FL_END":          fl_end,
                    "STATUS":                  status,
                    "CALLSIGN":                s["CALLSIGN"],
                    "SEGMENT_DATE_BEGIN":      s["TIME_OVER"].strftime("%y%m%d"),
                    "SEGMENT_DATE_END":        e["TIME_OVER"].strftime("%y%m%d"),
                    "SEGMENT_LATITUDE_BEGIN":  float(s["LATITUDE"])  * 60,
                    "SEGMENT_LONGITUDE_BEGIN": float(s["LONGITUDE"]) * 60,
                    "SEGMENT_LATITUDE_END":    float(e["LATITUDE"])  * 60,
                    "SEGMENT_LONGITUDE_END":   float(e["LONGITUDE"]) * 60,
                    "FLIGHT_ID":               flight_id,
                    "SEQUENCE":                i + 1,
                    "SEGMENT_LENGTH":          self._distance_nm(
                        float(s["LATITUDE"]),  float(s["LONGITUDE"]),
                        float(e["LATITUDE"]),  float(e["LONGITUDE"]),
                    ),
                    "SEGMENT_PARITY":          0,
                })
        return (
            pd.DataFrame(rows)
            .sort_values(["FLIGHT_ID", "SEQUENCE"])
            .reset_index(drop=True)
        )

    # ──────────────────────────── IATA season (no DB) ────────────────────────

    @staticmethod
    def season_iata(year: int, season: str = "summer") -> tuple[datetime, datetime]:
        """
        Start and end dates of an IATA season (inclusive on both ends).

        Parameters
        ----------
        year : int
        season : str, default 'summer'
            'summer' or 'winter'.

        Returns
        -------
        tuple[datetime, datetime]
            (start, end) as UTC-aware datetimes.
        """
        from .iata_season import season_iata as _season_iata
        return _season_iata(year, season)

    @staticmethod
    def iata_season_for_date(date: Union[str, datetime]) -> str:
        """
        Return the IATA season name for a given date.

        Returns
        -------
        str
            Format: 'summer-yyyy' or 'winter-yyyy'.
        """
        from .iata_season import iata_season_for_date as _iata_season_for_date
        return _iata_season_for_date(date)

    # ──────────────────────────── OurAirports (no DB) ────────────────────────

    @staticmethod
    def airports_oa() -> pd.DataFrame:
        """
        Latest airport list from OurAirports (requires internet access).

        Includes a set of manually added offshore heliports not present in the
        OurAirports dataset.
        """
        try:
            import requests
        except ImportError:
            raise ImportError(
                "requests is required for airports_oa(). Install it with: pip install requests"
            )

        url = (
            "https://raw.githubusercontent.com/davidmegginson/"
            "ourairports-data/main/airports.csv"
        )
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(resp.text), low_memory=False)

        df["gps_code"] = df["gps_code"].astype(str).str.strip()
        df["ident"] = df["ident"].astype(str).str.strip()
        df["icao"] = df.apply(
            lambda r: r["gps_code"] if r["ident"] != r["gps_code"] else r["ident"],
            axis=1,
        )
        df = df[
            (~df["type"].isin(["closed"]))
            & df["gps_code"].str.match(r"^[A-Z]{4}$", na=False)
            & (df["icao"] != "EDDT")
        ]
        df = df.rename(columns={
            "iata_code":     "iata",
            "latitude_deg":  "latitude",
            "longitude_deg": "longitude",
            "elevation_ft":  "elevation",
        })[["icao", "iata", "latitude", "longitude", "elevation",
            "type", "name", "iso_country", "iso_region", "continent"]]

        extra = pd.DataFrame([
            # NL offshore heliports
            {"icao": "EHFO", "iata": "", "longitude":  4.82722, "latitude": 54.21583, "elevation": 400.0, "type": "heliport", "name": "F15-A",          "iso_country": "NL", "iso_region": "", "continent": "EU"},
            {"icao": "EHGO", "iata": "", "longitude":  5.43472, "latitude": 54.16944, "elevation": 400.0, "type": "heliport", "name": "G14-B",          "iso_country": "NL", "iso_region": "", "continent": "EU"},
            {"icao": "EHGQ", "iata": "", "longitude":  5.43194, "latitude": 54.04917, "elevation": 400.0, "type": "heliport", "name": "G17D-A",         "iso_country": "NL", "iso_region": "", "continent": "EU"},
            {"icao": "EHGN", "iata": "", "longitude":  5.49861, "latitude": 54.22389, "elevation": 400.0, "type": "heliport", "name": "G14-A",          "iso_country": "NL", "iso_region": "", "continent": "EU"},
            {"icao": "EHFT", "iata": "", "longitude":  4.51278, "latitude": 53.81806, "elevation": 400.0, "type": "heliport", "name": "L5-D",           "iso_country": "NL", "iso_region": "", "continent": "EU"},
            {"icao": "EHFR", "iata": "", "longitude":  4.35111, "latitude": 53.81083, "elevation": 400.0, "type": "heliport", "name": "L5-FA-1",        "iso_country": "NL", "iso_region": "", "continent": "EU"},
            {"icao": "EHFD", "iata": "", "longitude":  4.69472, "latitude": 54.85306, "elevation": 400.0, "type": "heliport", "name": "F3-FB-1",        "iso_country": "NL", "iso_region": "", "continent": "EU"},
            {"icao": "EHFB", "iata": "", "longitude":  4.57250, "latitude": 54.94444, "elevation": 400.0, "type": "heliport", "name": "F2-A",           "iso_country": "NL", "iso_region": "", "continent": "EU"},
            {"icao": "EHAX", "iata": "", "longitude":  3.83167, "latitude": 55.10500, "elevation": 400.0, "type": "heliport", "name": "A18-A",          "iso_country": "NL", "iso_region": "", "continent": "EU"},
            {"icao": "EHFQ", "iata": "", "longitude":  4.49611, "latitude": 53.96056, "elevation": 400.0, "type": "heliport", "name": "L2-FA-1",        "iso_country": "NL", "iso_region": "", "continent": "EU"},
            {"icao": "EKAF", "iata": "", "longitude":  3.99000, "latitude": 55.80000, "elevation": 400.0, "type": "heliport", "name": "A6A",            "iso_country": "NL", "iso_region": "", "continent": "EU"},
            # NO offshore heliports
            {"icao": "ENUG", "iata": "", "longitude": 22.25000, "latitude": 71.30000, "elevation": 400.0, "type": "heliport", "name": "Goliat",         "iso_country": "NO", "iso_region": "", "continent": "EU"},
            {"icao": "ENDR", "iata": "", "longitude":  7.79167, "latitude": 64.35556, "elevation": 400.0, "type": "heliport", "name": "Draugen",        "iso_country": "NO", "iso_region": "", "continent": "EU"},
            {"icao": "ENFB", "iata": "", "longitude":  1.82889, "latitude": 61.20639, "elevation": 400.0, "type": "heliport", "name": "Statfjord B",    "iso_country": "NO", "iso_region": "", "continent": "EU"},
            {"icao": "ENGA", "iata": "", "longitude":  2.18750, "latitude": 61.17556, "elevation": 400.0, "type": "heliport", "name": "Gullfaks A",     "iso_country": "NO", "iso_region": "", "continent": "EU"},
            {"icao": "ENGC", "iata": "", "longitude":  2.27098, "latitude": 61.21406, "elevation": 400.0, "type": "heliport", "name": "Gullfaks C",     "iso_country": "NO", "iso_region": "", "continent": "EU"},
            {"icao": "ENHE", "iata": "", "longitude":  7.31583, "latitude": 65.32556, "elevation": 400.0, "type": "heliport", "name": "Heidrun A",      "iso_country": "NO", "iso_region": "", "continent": "EU"},
            {"icao": "ENHM", "iata": "", "longitude":  2.23333, "latitude": 59.56667, "elevation": 400.0, "type": "heliport", "name": "Heimdal",        "iso_country": "NO", "iso_region": "", "continent": "EU"},
            {"icao": "ENJS", "iata": "", "longitude":  2.54556, "latitude": 58.83528, "elevation": 400.0, "type": "heliport", "name": "Johan Sverdrup", "iso_country": "NO", "iso_region": "", "continent": "EU"},
            {"icao": "ENLE", "iata": "", "longitude":  3.21667, "latitude": 56.53330, "elevation": 400.0, "type": "heliport", "name": "Ekofisk Oil Pltf","iso_country": "NO", "iso_region": "", "continent": "EU"},
            {"icao": "ENNE", "iata": "", "longitude":  8.08333, "latitude": 66.03333, "elevation": 400.0, "type": "heliport", "name": "Norne A",        "iso_country": "NO", "iso_region": "", "continent": "EU"},
            {"icao": "ENQG", "iata": "", "longitude":  2.19883, "latitude": 61.20211, "elevation": 400.0, "type": "heliport", "name": "Gullfaks B",     "iso_country": "NO", "iso_region": "", "continent": "EU"},
            {"icao": "ENQJ", "iata": "", "longitude":  3.89500, "latitude": 61.33167, "elevation": 400.0, "type": "heliport", "name": "Gjoa",           "iso_country": "NO", "iso_region": "", "continent": "EU"},
            {"icao": "ENQK", "iata": "", "longitude":  2.50000, "latitude": 61.08333, "elevation": 400.0, "type": "heliport", "name": "Kvitebjorn",     "iso_country": "NO", "iso_region": "", "continent": "EU"},
            {"icao": "ENQL", "iata": "", "longitude":  2.07333, "latitude": 60.49833, "elevation": 400.0, "type": "heliport", "name": "Martin Linge B", "iso_country": "NO", "iso_region": "", "continent": "EU"},
            {"icao": "ENQM", "iata": "", "longitude":  2.01472, "latitude": 60.50750, "elevation": 400.0, "type": "heliport", "name": "Martin Linge A", "iso_country": "NO", "iso_region": "", "continent": "EU"},
            {"icao": "ENQR", "iata": "", "longitude":  2.21667, "latitude": 61.51667, "elevation": 400.0, "type": "heliport", "name": "Snorre B",       "iso_country": "NO", "iso_region": "", "continent": "EU"},
            {"icao": "ENQS", "iata": "", "longitude":  1.90083, "latitude": 61.29610, "elevation": 400.0, "type": "heliport", "name": "Statfjord C",    "iso_country": "NO", "iso_region": "", "continent": "EU"},
            {"icao": "ENQV", "iata": "", "longitude":  2.45722, "latitude": 61.36972, "elevation": 400.0, "type": "heliport", "name": "Visund A",       "iso_country": "NO", "iso_region": "", "continent": "EU"},
            {"icao": "ENQW", "iata": "", "longitude":  2.36694, "latitude": 61.04000, "elevation": 400.0, "type": "heliport", "name": "Valemon",        "iso_country": "NO", "iso_region": "", "continent": "EU"},
            {"icao": "ENSE", "iata": "", "longitude":  2.15000, "latitude": 61.45000, "elevation": 400.0, "type": "heliport", "name": "Snorre A",       "iso_country": "NO", "iso_region": "", "continent": "EU"},
            {"icao": "ENSF", "iata": "", "longitude":  1.85211, "latitude": 61.25518, "elevation": 400.0, "type": "heliport", "name": "Statfjord A",    "iso_country": "NO", "iso_region": "", "continent": "EU"},
            {"icao": "ENUA", "iata": "", "longitude":  6.72556, "latitude": 65.06417, "elevation": 400.0, "type": "heliport", "name": "Kristin",        "iso_country": "NO", "iso_region": "", "continent": "EU"},
            {"icao": "ENUB", "iata": "", "longitude":  6.78944, "latitude": 65.11000, "elevation": 400.0, "type": "heliport", "name": "Aasgard B",      "iso_country": "NO", "iso_region": "", "continent": "EU"},
            {"icao": "ENUC", "iata": "", "longitude":  6.92590, "latitude": 65.08630, "elevation": 400.0, "type": "heliport", "name": "Asgard C",       "iso_country": "NO", "iso_region": "", "continent": "EU"},
            {"icao": "ENUK", "iata": "", "longitude":  6.55056, "latitude": 64.99417, "elevation": 400.0, "type": "heliport", "name": "Aasgard A",      "iso_country": "NO", "iso_region": "", "continent": "EU"},
            {"icao": "ENUR", "iata": "", "longitude":  7.36722, "latitude": 65.34333, "elevation": 400.0, "type": "heliport", "name": "Heidrun B",      "iso_country": "NO", "iso_region": "", "continent": "EU"},
            {"icao": "ENUS", "iata": "", "longitude":  7.65083, "latitude": 65.69972, "elevation": 400.0, "type": "heliport", "name": "Skarv",          "iso_country": "NO", "iso_region": "", "continent": "EU"},
            {"icao": "ENWE", "iata": "", "longitude":  2.24722, "latitude": 58.84250, "elevation": 400.0, "type": "heliport", "name": "Edvard Grieg",   "iso_country": "NO", "iso_region": "", "continent": "EU"},
            {"icao": "ENWG", "iata": "", "longitude":  1.74333, "latitude": 58.84500, "elevation": 400.0, "type": "heliport", "name": "Gudrun",         "iso_country": "NO", "iso_region": "", "continent": "EU"},
            {"icao": "ENWI", "iata": "", "longitude":  2.19778, "latitude": 58.92222, "elevation": 400.0, "type": "heliport", "name": "Ivar Aasen",     "iso_country": "NO", "iso_region": "", "continent": "EU"},
            {"icao": "ENWK", "iata": "", "longitude":  1.69528, "latitude": 58.57139, "elevation": 400.0, "type": "heliport", "name": "Gina Krog",      "iso_country": "NO", "iso_region": "", "continent": "EU"},
            {"icao": "ENWR", "iata": "", "longitude":  1.73278, "latitude": 58.58333, "elevation": 400.0, "type": "heliport", "name": "Randgrid",       "iso_country": "NO", "iso_region": "", "continent": "EU"},
            {"icao": "ENWS", "iata": "", "longitude":  3.26278, "latitude": 56.37361, "elevation": 400.0, "type": "heliport", "name": "Eldfisk S",      "iso_country": "NO", "iso_region": "", "continent": "EU"},
            {"icao": "ENWV", "iata": "", "longitude":  3.40000, "latitude": 56.28333, "elevation": 400.0, "type": "heliport", "name": "Valhall PH",     "iso_country": "NO", "iso_region": "", "continent": "EU"},
            {"icao": "ENXW", "iata": "", "longitude":  2.48667, "latitude": 59.16500, "elevation": 400.0, "type": "heliport", "name": "Grane",          "iso_country": "NO", "iso_region": "", "continent": "EU"},
        ])

        df = pd.concat([df, extra], ignore_index=True)
        df["last_updated"] = date.today()
        return df

    # ──────────────────────────── H3 polyfill ────────────────────────────────

    @staticmethod
    def _h3_cells_for_geometry(geom, resolution: int) -> list[str]:
        """
        Return a list of H3 cell indices that cover a Shapely geometry.

        Supports both h3-py v3 (polyfill_geojson) and v4 (geo_to_cells).
        Handles Polygon and MultiPolygon; returns [] for other types.
        """
        try:
            import h3
        except ImportError:
            raise ImportError(
                "h3-py is required for polyfill_h3(). "
                "Install it with: pip install h3"
            )
        from shapely.geometry import mapping

        def _polyfill_polygon(poly_geojson: dict) -> list[str]:
            try:
                return list(h3.geo_to_cells(poly_geojson, res=resolution))   # h3 >= 4
            except AttributeError:
                return list(h3.polyfill_geojson(poly_geojson, resolution))    # h3 3.x

        geojson = mapping(geom)
        geo_type = geojson.get("type", "")

        if geo_type == "Polygon":
            return _polyfill_polygon(geojson)
        if geo_type == "MultiPolygon":
            cells: set[str] = set()
            for poly_coords in geojson["coordinates"]:
                cells.update(
                    _polyfill_polygon({"type": "Polygon", "coordinates": poly_coords})
                )
            return sorted(cells)
        return []

    def polyfill_h3(
        self,
        gdf: Any,
        resolution: int,
        explode: bool = False,
    ) -> pd.DataFrame:
        """
        Fill airspace polygons with Uber H3 cells at the given resolution.

        Parameters
        ----------
        gdf : geopandas.GeoDataFrame
            Output from any ``*_sf`` method (acc_sf, ansp_sf, es_sf, fir_sf).
        resolution : int
            H3 resolution level (0 = coarsest, 15 = finest). Typical airspace
            use cases: 3–6 for FIRs, 5–7 for smaller sectors.
        explode : bool, default False
            If False (default), return one row per airspace with ``h3_index``
            as a Python list of cell strings.
            If True, return one row per H3 cell — all other columns duplicated.

        Returns
        -------
        pandas.DataFrame
            The geometry column is dropped; all other properties are retained
            alongside the new ``h3_index`` column.

        Notes
        -----
        Requires ``h3-py`` (``pip install h3``) and ``geopandas``
        (``pip install geopandas``).

        EUROCONTROLSpark overrides this method to return a Spark DataFrame
        using a PySpark UDF, so the polyfill runs on Spark workers.
        """
        gdf = gdf.copy()
        gdf["h3_index"] = gdf["geometry"].apply(
            lambda geom: self._h3_cells_for_geometry(geom, resolution)
        )
        result = pd.DataFrame(gdf.drop(columns=["geometry"]))
        if explode:
            return result.explode("h3_index").reset_index(drop=True)
        return result
