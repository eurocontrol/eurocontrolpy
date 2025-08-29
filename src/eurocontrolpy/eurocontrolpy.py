import os
import stat
import shutil
import zipfile
import subprocess
import urllib.request
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Union

import cx_Oracle
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import MetaData, Table, event, text
from sqlalchemy.engine import Engine

try:
    from geopy.distance import geodesic
except ImportError:
    geodesic = None


# ------------------ Instant Client helpers (fix for 1 KB libclntsh.so) ------------------ #
class InstantClientError(RuntimeError):
    """Raised when Oracle Instant Client installation or validation fails."""


def _is_elf_64(path: Path) -> bool:
    """
    Return True if the file at `path` is a 64-bit ELF shared object.

    This uses the `file` utility when available; otherwise it inspects the ELF
    magic and class bytes directly.

    Parameters
    ----------
    path : pathlib.Path
        Path to the file.

    Returns
    -------
    bool
        True if the file looks like an ELF 64-bit shared object, else False.
    """
    try:
        out = subprocess.check_output(["file", "-b", str(path)], text=True).lower()
        return "elf" in out and "64-bit" in out
    except Exception:
        pass

    try:
        with path.open("rb") as f:
            hdr = f.read(5)
        # ELF magic 0x7f,'E','L','F' and EI_CLASS == 2 (64-bit)
        return len(hdr) >= 5 and hdr[:4] == b"\x7fELF" and hdr[4] == 2
    except Exception:
        return False


def _extract_zip_preserving_symlinks(zip_path: Path, dest_dir: Path) -> None:
    """
    Extract `zip_path` into `dest_dir`, restoring Unix symlinks correctly.

    Why this exists
    ---------------
    `zipfile.ZipFile.extractall()` often flattens symlinks into tiny regular files
    (≈1 KB) that contain the link target text. Oracle Instant Client relies on
    `libclntsh.so` being a symlink to a versioned `.so`, so flattening breaks
    Thick mode with DPI-1047.

    This function reconstructs symlinks by interpreting the Unix mode/type bits
    in the ZIP's external attributes and reading the entry payload as the link
    target.

    Parameters
    ----------
    zip_path : pathlib.Path
        Path to the ZIP archive.
    dest_dir : pathlib.Path
        Destination directory to extract into.

    Returns
    -------
    None
    """
    with zipfile.ZipFile(zip_path) as zf:
        for info in zf.infolist():
            out_path = dest_dir / info.filename
            # Upper 16 bits: Unix file mode (type + perms)
            mode_bits = (info.external_attr >> 16) & 0o777777
            file_type = mode_bits & 0o170000
            perms = mode_bits & 0o777
            is_dir = (file_type == stat.S_IFDIR) or info.is_dir()
            is_link = file_type == stat.S_IFLNK

            if is_dir:
                out_path.mkdir(parents=True, exist_ok=True)
                if perms:
                    try:
                        os.chmod(out_path, perms)
                    except Exception:
                        pass
                continue

            out_path.parent.mkdir(parents=True, exist_ok=True)

            if is_link:
                # The entry content is the link target path (bytes -> UTF-8 text)
                target = zf.read(info).decode("utf-8")
                if out_path.exists() or out_path.is_symlink():
                    out_path.unlink()
                os.symlink(target, out_path)
            else:
                with zf.open(info) as src, open(out_path, "wb") as dst:
                    shutil.copyfileobj(src, dst)
                if perms:
                    try:
                        os.chmod(out_path, perms)
                    except Exception:
                        pass


# ---------------------------------------- Main class ---------------------------------------- #
class EUROCONTROL:
    """
    Python client for interacting with EUROCONTROL PRISME / NM trajectory
    and airspace profile data stored in Oracle.

    Parameters
    ----------
    schema : str, default "PRU_PROD"
        The schema/environment prefix used to read Oracle credentials from
        environment variables. If ``schema="PRU_DEV"``, the code expects
        the following variables to be defined:
        ``PRU_DEV_USR``, ``PRU_DEV_PWD``, ``PRU_DEV_DBNAME``.

    Attributes
    ----------
    engine : sqlalchemy.engine.Engine
        SQLAlchemy engine connected to the Oracle database.

    Notes
    -----
    - The connection is configured to use UTC for date/time handling.
    - The client relies on SQLAlchemy's ``cx_Oracle`` dialect.
    - To pre-stage Instant Client, set ``ORACLE_IC_LIB_DIR`` to its directory.
    - To download at runtime, set ``ORACLE_IC_ZIP_URL`` to a permanent ZIP URL.
    """

    def __init__(self, schema: str = "PRU_PROD"):
        """
        Create a new EUROCONTROL client.

        Parameters
        ----------
        schema : str, default "PRU_PROD"
            Environment prefix for credentials.
        """
        self.schema = schema
        self.engine = self._db_connection(schema)

    def _init_oracle_client_if_needed(self) -> None:
        """
        Initialize cx_Oracle (Thick mode) from an Instant Client directory.

        Honors the environment variable:
        - ORACLE_IC_LIB_DIR -> Instant Client directory (defaults to ~/instantclient_23_9)

        Returns
        -------
        None

        Raises
        ------
        InstantClientError
            If initialization fails or the directory is not found.
        """
        lib_dir = Path(os.getenv("ORACLE_IC_LIB_DIR", "~/instantclient_23_9")).expanduser()
        if not lib_dir.is_dir():
            raise InstantClientError(
                f"Instant Client directory not found: {lib_dir}. "
                "Set ORACLE_IC_LIB_DIR or install Instant Client."
            )

        try:
            # Safe to call once per-process; subsequent calls raise an error.
            cx_Oracle.init_oracle_client(lib_dir=str(lib_dir))
        except cx_Oracle.ProgrammingError:
            # Already initialized in this process; ignore.
            pass
        except Exception as exc:
            raise InstantClientError(
                f"Failed to initialize Oracle Client from {lib_dir}: {exc}"
            ) from exc

    # ------------------ Private helpers ------------------ #
    def _db_connection(self, schema: str) -> Engine:
        """
        Establish a SQLAlchemy engine for the Oracle PRISME / NM database.

        The ``schema`` parameter is used as a prefix to fetch the Oracle
        credentials from the environment.

        Parameters
        ----------
        schema : str
            Environment prefix for credentials. Expected variables:
            ``<schema>_USR``, ``<schema>_PWD``, ``<schema>_DBNAME``.

        Returns
        -------
        sqlalchemy.engine.Engine
            A SQLAlchemy engine connected to the Oracle database.

        Raises
        ------
        ValueError
            If any of the required environment variables are missing.

        Notes
        -----
        - Sets Oracle session timezone and NLS date/timestamp formats to UTC.
        - Ensures UTF-8 encoding for the connection.
        """
        self._init_oracle_client_if_needed()

        usr = os.getenv(f"{schema}_USR")
        pwd = os.getenv(f"{schema}_PWD")
        dbn = os.getenv(f"{schema}_DBNAME")

        missing = [
            name
            for name, val in (
                (f"{schema}_USR", usr),
                (f"{schema}_PWD", pwd),
                (f"{schema}_DBNAME", dbn),
            )
            if not val
        ]
        if missing:
            fixed_missing = [m.replace("}", "") for m in missing]
            raise ValueError(
                f"Missing required environment variables: {', '.join(fixed_missing)}"
            )

        os.environ.setdefault("TZ", "UTC")
        os.environ.setdefault("ORA_SDTZ", "UTC")
        os.environ.setdefault("NLS_LANG", ".AL32UTF8")

        # With cx_Oracle, use the "cx_oracle" SQLAlchemy dialect.
        url = f"oracle+cx_oracle://{usr}:{pwd}@{dbn}"
        engine = sa.create_engine(url, pool_pre_ping=True)

        @event.listens_for(engine, "connect")
        def _set_session_timezone(dbapi_connection, _):
            """
            Set session timezone/NLS formats to UTC upon each connection.
            """
            cursor = dbapi_connection.cursor()
            try:
                cursor.execute("ALTER SESSION SET TIME_ZONE = 'UTC'")
                cursor.execute(
                    "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
                )
                cursor.execute(
                    "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
                )
                cursor.execute(
                    "ALTER SESSION SET "
                    "NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'"
                )
            finally:
                cursor.close()

        return engine

    @staticmethod
    def _as_utc(dt: Union[str, datetime]) -> datetime:
        """
        Convert a string or datetime into a timezone-aware UTC ``datetime``.

        Parameters
        ----------
        dt : str or datetime.datetime
            Either an ISO-8601 string (e.g., ``'2020-01-01T00:00:00Z'`` or
            ``'2020-01-01 00:00:00'``) or a ``datetime`` object.

        Returns
        -------
        datetime.datetime
            A timezone-aware (UTC) ``datetime``.
        """
        if isinstance(dt, str):
            try:
                out = datetime.fromisoformat(dt.replace("Z", "+00:00"))
            except ValueError:
                out = datetime.strptime(dt, "%Y-%m-%d")
        else:
            out = dt
        if out.tzinfo is None:
            out = out.replace(tzinfo=timezone.utc)
        else:
            out = out.astimezone(timezone.utc)
        return out

    @staticmethod
    def _fmt(dt: datetime) -> str:
        """
        Format a ``datetime`` to ``YYYY-MM-DD HH:MM:SS`` (UTC).

        Parameters
        ----------
        dt : datetime.datetime
            Datetime to be formatted. If it has a timezone, it should
            already be UTC.

        Returns
        -------
        str
            Formatted timestamp string.
        """
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _distance_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        Compute geodesic distance in nautical miles between two coordinates.

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

        Notes
        -----
        Uses ``geopy.distance.geodesic`` when available, otherwise falls back
        to a Haversine approximation with Earth mean radius.
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

    # ------------------ Table access ------------------ #
    def airspace_profile_tbl(self) -> Table:
        """
        Return a SQLAlchemy ``Table`` for ``FSD.ALL_FT_ASP_PROFILE``.

        Returns
        -------
        sqlalchemy.Table
            Reflected table object for the PRISME airspace profiles.
        """
        metadata = MetaData()
        return Table(
            "ALL_FT_ASP_PROFILE", metadata, schema="FSD", autoload_with=self.engine
        )

    def point_profile_tbl(self) -> Table:
        """
        Return a SQLAlchemy ``Table`` for ``FSD.ALL_FT_POINT_PROFILE``.

        Returns
        -------
        sqlalchemy.Table
            Reflected table object for the PRISME point profiles.
        """
        metadata = MetaData()
        return Table(
            "ALL_FT_POINT_PROFILE", metadata, schema="FSD", autoload_with=self.engine
        )

    # ------------------ Data methods ------------------ #
    def flights_tidy(
        self,
        wef: Union[str, datetime],
        til: Union[str, datetime],
        icao_flt_types: Optional[list[str]] = None,
        ids: Optional[list[Union[int, str]]] = None,
        include_sensitive: bool = False,
        include_military: bool = False,
        include_head: bool = False,
    ) -> sa.sql.Select:
        """
        Build a lazy SQL query for a clean flights list in a right-open interval.

        Parameters
        ----------
        wef : str or datetime.datetime
            Inclusive start (UTC).
        til : str or datetime.datetime
            Exclusive end (UTC).
        icao_flt_types : list[str], optional
            Keep only these ICAO flight types.
        ids : list[int | str], optional
            Explicit list of flight IDs to include.
        include_sensitive : bool, default False
            If False, exclude ``SENSITIVE = 'Y'``.
        include_military : bool, default False
            If False, exclude ``SK_FLT_TYPE_RULE_ID = 1`` (military).
        include_head : bool, default False
            If False, exclude ``EXMP_RSN_LH = 'HEAD'``.

        Returns
        -------
        sqlalchemy.sql.Select
            A **lazy** query you can pass to pandas or execute via SQLAlchemy.
        """
        # Normalize timestamps to UTC strings as in the R filter
        wef_str = self._fmt(self._as_utc(wef))
        til_str = self._fmt(self._as_utc(til))

        md = MetaData()
        flt = Table("V_FAC_FLIGHT_MS", md, schema="SWH_FCT", autoload_with=self.engine)
        frl = Table(
            "DIM_FLIGHT_TYPE_RULE", md, schema="SWH_FCT", autoload_with=self.engine
        )
        aog = Table("V_COVID_DIM_AO", md, schema="PRUDEV", autoload_with=self.engine)
        apt = Table(
            "V_COVID_REL_AIRPORT_AREA", md, schema="PRUDEV", autoload_with=self.engine
        )

        # Airport table aliased 3 times (ADEP / ADES / ADES_FILED)
        apt_adep = apt.alias("APT_ADEP")
        apt_ades = apt.alias("APT_ADES")
        apt_ades_filed = apt.alias("APT_ADES_FILED")

        # Base FROM with LEFT joins as per R dplyr::left_join chain
        j = (
            flt.outerjoin(frl, flt.c.SK_FLT_TYPE_RULE_ID == frl.c.SK_FLT_TYPE_RULE_ID)
            .outerjoin(aog, flt.c.AIRCRAFT_OPERATOR == aog.c.AO_CODE)
            .outerjoin(apt_adep, flt.c.ADEP == apt_adep.c.CFMU_AP_CODE)
            .outerjoin(apt_ades, flt.c.ADES == apt_ades.c.CFMU_AP_CODE)
            .outerjoin(apt_ades_filed, flt.c.ADES_FILED == apt_ades_filed.c.CFMU_AP_CODE)
        )

        # Interval predicates using TO_DATE(...) like the R code
        interval_predicates = sa.and_(
            text(f"TO_DATE('{wef_str}', 'YYYY-MM-DD HH24:MI:SS') <= LOBT"),
            text(f"LOBT < TO_DATE('{til_str}', 'YYYY-MM-DD HH24:MI:SS')"),
        )

        # Optional filters mirroring the R implementation
        where_clauses = [interval_predicates]

        if icao_flt_types is not None:
            where_clauses.append(flt.c.ICAO_FLT_TYPE.in_(list(icao_flt_types)))

        if ids is not None:
            where_clauses.append(flt.c.ID.in_(list(ids)))

        if not include_sensitive:
            # exclude sensitive flights
            where_clauses.append(flt.c.SENSITIVE != sa.literal("Y"))

        if not include_military:
            # exclude military: SK_FLT_TYPE_RULE_ID != 1
            where_clauses.append(flt.c.SK_FLT_TYPE_RULE_ID != sa.literal(1))

        if not include_head:
            # exclude Head of State flights (might overlap with SENSITIVE)
            where_clauses.append(flt.c.EXMP_RSN_LH != sa.literal("HEAD"))

        # Column selection matching the R output (with renames)
        cols = [
            # -- flight
            flt.c.ID,
            flt.c.LOBT,
            flt.c.AIRCRAFT_ID,
            flt.c.ADEP,
            apt_adep.c.PRU_DASHBOARD_AP_NAME.label("NAME_ADEP"),
            apt_adep.c.COUNTRY_CODE.label("COUNTRY_CODE_ADEP"),
            apt_adep.c.COUNTRY_NAME.label("COUNTRY_NAME_ADEP"),
            flt.c.ADES,
            apt_ades.c.PRU_DASHBOARD_AP_NAME.label("NAME_ADES"),
            apt_ades.c.COUNTRY_CODE.label("COUNTRY_CODE_ADES"),
            apt_ades.c.COUNTRY_NAME.label("COUNTRY_NAME_ADES"),
            flt.c.ADES_FILED,
            apt_ades_filed.c.PRU_DASHBOARD_AP_NAME.label("NAME_ADES_FILED"),
            apt_ades_filed.c.COUNTRY_CODE.label("COUNTRY_CODE_ADES_FILED"),
            apt_ades_filed.c.COUNTRY_NAME.label("COUNTRY_NAME_ADES_FILED"),
            flt.c.SENSITIVE,
            flt.c.EXMP_RSN_LH.label("SPECIAL_EXEMPT"),
            frl.c.RULE_NAME,  # Market Segment
            flt.c.FLT_UID,
            flt.c.IOBT,
            flt.c.FLT_RULES,
            flt.c.ICAO_FLT_TYPE,
            # -- aircraft
            flt.c.REGISTRATION,
            flt.c.AIRCRAFT_ADDRESS,
            flt.c.AIRCRAFT_TYPE_ICAO_ID,
            flt.c.WK_TBL_CAT,
            # -- aircraft operator
            flt.c.AIRCRAFT_OPERATOR,
            aog.c.AO_ISO_CTRY_CODE,
            aog.c.AO_GRP_CODE,
            aog.c.AO_GRP_NAME,
            # -- operational details
            flt.c.EOBT_1,
            flt.c.ARVT_1,
            flt.c.TAXI_TIME_1,
            flt.c.AOBT_3,
            flt.c.ARVT_3,
            flt.c.TAXI_TIME_3,
            flt.c.FLT_DUR_1,
            flt.c.FLT_DUR_3,
            flt.c.RTE_LEN_1,
            flt.c.RTE_LEN_3,
            flt.c.FLT_TOW,
        ]

        query = sa.select(*cols).select_from(j).where(sa.and_(*where_clauses))
        return query

    def airlines_tbl(self) -> Table:
        """
        Return a reference to the Airlines table (PRUDEV.V_COVID_DIM_AO).

        Returns
        -------
        sqlalchemy.Table
            Reflected table object for ``PRUDEV.V_COVID_DIM_AO``.
        """
        metadata = MetaData()
        return Table("V_COVID_DIM_AO", metadata, schema="PRUDEV", autoload_with=self.engine)

    def airlines_tidy(
        self,
        member_state_iso2c: Optional[Union[list[str], tuple[str, ...]]] = None,
    ) -> sa.sql.Select:
        """
        Airline info including group affiliation and EU membership flag.

        Parameters
        ----------
        member_state_iso2c : list[str] | tuple[str, ...], optional
            ISO 2-letter country codes considered EUROCONTROL Member States.

        Returns
        -------
        sqlalchemy.sql.Select
            Lazy select with airline info and derived EU flag.
        """
        arl = self.airlines_tbl()
        ms_list = list(member_state_iso2c or [])

        eu_flag = sa.case(
            (arl.c.AO_ISO_CTRY_CODE.in_(ms_list), sa.literal("TRUE")),
            else_=sa.literal("FALSE"),
        ).label("EU")

        cols = [
            arl.c.AO_CODE,
            arl.c.AO_NAME,
            arl.c.AO_GRP_CODE,
            arl.c.AO_GRP_NAME,
            arl.c.AO_ISO_CTRY_CODE,
            eu_flag,
        ]

        return sa.select(*cols)

    def apdf_tbl(self) -> Table:
        """
        Return a reference to the Airport Operator Data Flow table
        (SWH_FCT.FAC_APDS_FLIGHT_IR691).

        Returns
        -------
        sqlalchemy.Table
            Reflected table object for ``SWH_FCT.FAC_APDS_FLIGHT_IR691``.
        """
        metadata = MetaData()
        return Table(
            "FAC_APDS_FLIGHT_IR691",
            metadata,
            schema="SWH_FCT",
            autoload_with=self.engine,
        )

    def apdf_tidy(
        self,
        wef: str,
        til: str,
    ) -> sa.sql.Select:
        """
        Extract a clean airport operator data flow list in an interval.

        Parameters
        ----------
        wef : str
            **W**ith **EF**fect (included) in UTC.
        til : str
            Un**TIL** (excluded) in UTC.

        Returns
        -------
        sqlalchemy.sql.Select
            A **lazy** SQLAlchemy ``Select`` with filtered columns.
        """
        apdf = self.apdf_tbl()

        wef_date = sa.func.to_date(sa.literal(wef), "yyyy-mm-dd hh24:mi:ss")
        til_date = sa.func.to_date(sa.literal(til), "yyyy-mm-dd hh24:mi:ss")

        cols = [
            apdf.c.APDS_ID,
            apdf.c.IM_SAMAD_ID.label("ID"),
            apdf.c.AP_C_FLTID,
            apdf.c.AP_C_FLTRUL,
            apdf.c.AP_C_REG,
            apdf.c.ADEP_ICAO,
            apdf.c.ADES_ICAO,
            apdf.c.SRC_PHASE,
            apdf.c.MVT_TIME_UTC,
            apdf.c.BLOCK_TIME_UTC,
            apdf.c.SCHED_TIME_UTC,
            apdf.c.ARCTYP,
            apdf.c.AP_C_RWY,
            apdf.c.AP_C_STND,
            *[c for c in apdf.c if str(c.name).startswith("C40_")],
            *[c for c in apdf.c if str(c.name).startswith("C100_")],
        ]

        query = sa.select(*cols).where(
            wef_date <= apdf.c.MVT_TIME_UTC,
            apdf.c.MVT_TIME_UTC < til_date,
            wef_date <= apdf.c.SRC_DATE_FROM,
            apdf.c.SRC_DATE_FROM < til_date,
        )

        # Exclude unwanted suffix patterns (mirroring R's select(-ends_with(...)))
        exclude_patterns = (
            "_MIN",
            "_IN_FRONT",
            "_CTFM",
            "_CPF",
            "TRANSIT",
        )
        final_cols = [
            col
            for col in query.selected_columns
            if not any(str(col.name).endswith(p) for p in exclude_patterns)
            and not any(p in str(col.name) for p in exclude_patterns)
        ]

        return sa.select(*final_cols).select_from(apdf)

    def airspace_profiles_tidy(
        self,
        wef: Union[str, datetime],
        til: Union[str, datetime],
        airspace: str = "FIR",
        profile: str = "CTFM",
    ) -> pd.DataFrame:
        """
        Provide all airspace profile segments intersecting ``[wef, til)``.

        Parameters
        ----------
        wef : str or datetime.datetime
            Start of the time window (inclusive).
        til : str or datetime.datetime
            End of the time window (exclusive).
        airspace : str, default "FIR"
            Airspace type, one of ``{"FIR","NAS","AUA","ES"}``.
        profile : str, default "CTFM"
            Trajectory model type, one of
            ``{"FTFM","RTFM","CTFM","CPF","DCT","SCR","SRR","SUR"}``.

        Returns
        -------
        pandas.DataFrame
            Airspace profile segments intersecting the window.
        """
        before_hours, after_hours = 28, 24
        w_dt = self._as_utc(wef)
        t_dt = self._as_utc(til)
        wef_before = self._fmt(w_dt - timedelta(hours=before_hours))
        til_after = self._fmt(t_dt + timedelta(hours=after_hours))
        wef_str = self._fmt(w_dt)
        til_str = self._fmt(t_dt)

        # NOTE: flights_tidy() returns a Select; collect IDs via pandas.
        with self.engine.connect() as cx:
            ids_df = pd.read_sql(
                sa.select(text("ID")).select_from(
                    text("SWH_FCT.V_FAC_FLIGHT_MS")
                ).where(
                    text(
                        f"TO_DATE('{wef_before}', 'YYYY-MM-DD HH24:MI:SS') <= LOBT "
                        f"AND LOBT < TO_DATE('{til_after}', 'YYYY-MM-DD HH24:MI:SS')"
                    )
                ).distinct(),
                cx,
            )

        asp = self.airspace_profile_tbl()
        query = sa.select(
            asp.c.SAM_ID,
            asp.c.SEQ_ID,
            asp.c.ENTRY_TIME,
            asp.c.ENTRY_LON,
            asp.c.ENTRY_LAT,
            asp.c.ENTRY_FL,
            asp.c.EXIT_TIME,
            asp.c.EXIT_LON,
            asp.c.EXIT_LAT,
            asp.c.EXIT_FL,
            asp.c.AIRSPACE_ID,
            asp.c.AIRSPACE_TYPE,
            asp.c.MODEL_TYPE,
        ).where(
            text(f"TO_DATE('{wef_before}', 'YYYY-MM-DD HH24:MI:SS') <= LOBT"),
            text(f"LOBT < TO_DATE('{til_after}', 'YYYY-MM-DD HH24:MI:SS')"),
            asp.c.MODEL_TYPE.in_([profile]),
            asp.c.AIRSPACE_TYPE == airspace,
            asp.c.ENTRY_LON.isnot(None),
            asp.c.ENTRY_LAT.isnot(None),
            asp.c.ENTRY_TIME.isnot(None),
            asp.c.ENTRY_FL.isnot(None),
            asp.c.EXIT_LON.isnot(None),
            asp.c.EXIT_LAT.isnot(None),
            asp.c.EXIT_TIME.isnot(None),
            asp.c.EXIT_FL.isnot(None),
            text(f"ENTRY_TIME <= TO_DATE('{til_str}', 'YYYY-MM-DD HH24:MI:SS')"),
            text(f"TO_DATE('{wef_str}', 'YYYY-MM-DD HH24:MI:SS') < EXIT_TIME"),
        )
        with self.engine.connect() as cx:
            prf_df = pd.read_sql(query, cx)
        prf_df = prf_df.merge(ids_df.drop_duplicates(), left_on="SAM_ID", right_on="ID")
        prf_df = prf_df.rename(columns={"SAM_ID": "ID"})
        return prf_df

    def flights_airspace_profiles_tidy(
        self,
        wef: Union[str, datetime],
        til: Union[str, datetime],
        airspace: str = "FIR",
        profile: str = "CTFM",
    ) -> pd.DataFrame:
        """
        Extract flights whose airspace profile segments intersect ``[wef, til)``.

        Parameters
        ----------
        wef : str or datetime.datetime
            Start of the time window (inclusive).
        til : str or datetime.datetime
            End of the time window (exclusive).
        airspace : str, default "FIR"
            Airspace type.
        profile : str, default "CTFM"
            Trajectory model type.

        Returns
        -------
        pandas.DataFrame
            Flight rows with at least one intersecting airspace segment.
        """
        before_hours, after_hours = 28, 24
        w_dt = self._as_utc(wef)
        t_dt = self._as_utc(til)
        wef_before = self._fmt(w_dt - timedelta(hours=before_hours))
        til_after = self._fmt(t_dt + timedelta(hours=after_hours))

        prf = self.airspace_profiles_tidy(wef, til, airspace, profile)

        # Collect base flights within buffered LOBT window
        with self.engine.connect() as cx:
            flt_df = pd.read_sql(
                self.flights_tidy(wef_before, til_after),
                cx,
            )

        merged = flt_df.merge(prf[["ID"]].drop_duplicates(), on="ID", how="inner")
        return merged.drop_duplicates()

    def export_model_trajectory(
        self,
        wef: Union[str, datetime],
        til: Union[str, datetime],
        profile: str = "CTFM",
        bbox: Optional[Dict[str, float]] = None,
        lobt_buffer: Optional[Dict[str, float]] = None,
        timeover_buffer: Optional[Dict[str, float]] = None,
    ) -> pd.DataFrame:
        """
        Export point profiles for flights in a time window and model type.

        Parameters
        ----------
        wef : str or datetime.datetime
            Start of the time window (inclusive).
        til : str or datetime.datetime
            End of the time window (exclusive).
        profile : str, default "CTFM"
            Trajectory model type. One of
            ``{"CPF", "CTFM", "DCT", "FTFM", "SCR", "SRR", "SUR"}``.
        bbox : dict[str, float], optional
            Axis-aligned bounding box with keys
            ``{"xmin","xmax","ymin","ymax"}`` in decimal degrees.
        lobt_buffer : dict[str, float], optional
            Hours to extend the LOBT window around ``wef``/``til``.
        timeover_buffer : dict[str, float], optional
            Hours to extend filtering on ``TIME_OVER`` around the window.

        Returns
        -------
        pandas.DataFrame
            Point profile trajectory rows.
        """
        valid_profiles = {"CPF", "CTFM", "DCT", "FTFM", "SCR", "SRR", "SUR"}
        if profile not in valid_profiles:
            raise ValueError(f"Invalid profile '{profile}'.")

        w_dt = self._as_utc(wef)
        t_dt = self._as_utc(til)
        wef_iso = w_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        til_iso = t_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        where_bbox, where_timeover_buffer = "", ""
        lobt_before, lobt_after = 28.0, 24.0

        if bbox:
            where_bbox = (
                "AND ((:lon_min <= p.LON AND p.LON <= :lon_max) "
                "AND (:lat_min <= p.LAT AND p.LAT <= :lat_max))"
            )
        if lobt_buffer:
            lobt_before = float(lobt_buffer["before"])
            lobt_after = float(lobt_buffer["after"])
        if timeover_buffer:
            where_timeover_buffer = (
                "AND (((SELECT LOBT_WEF FROM ARGS) - (:to_before / 24) <= p.TIME_OVER) "
                "AND (p.TIME_OVER < (SELECT LOBT_TIL FROM ARGS) + (:to_after / 24)))"
            )

        sql_template = f"""
            WITH ARGS AS (
                SELECT TO_DATE(:WEF, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS LOBT_WEF,
                       TO_DATE(:TIL, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS LOBT_TIL
                FROM DUAL
            )
            SELECT P.SAM_ID AS FLIGHT_ID, P.TIME_OVER, P.LON AS LONGITUDE, P.LAT AS LATITUDE,
                   P.FLIGHT_LEVEL, P.POINT_ID, P.AIR_ROUTE, P.LOBT, P.SEQ_ID,
                   F.AIRCRAFT_ID AS CALLSIGN, F.REGISTRATION, P.MODEL_TYPE,
                   F.AIRCRAFT_TYPE_ICAO_ID AS AIRCRAFT_TYPE, F.AIRCRAFT_OPERATOR,
                   F.AIRCRAFT_ADDRESS AS ICAO24, F.ADEP, F.ADES
            FROM FSD.ALL_FT_POINT_PROFILE P
            JOIN FLX.FLIGHT F ON (F.ID = P.SAM_ID AND F.LOBT = P.LOBT)
            WHERE F.LOBT >= (SELECT LOBT_WEF FROM ARGS) - ({lobt_before} / 24)
              AND F.LOBT <  (SELECT LOBT_TIL FROM ARGS) + ({lobt_after} / 24)
              AND P.LOBT >= (SELECT LOBT_WEF FROM ARGS) - ({lobt_before} / 24)
              AND P.LOBT <  (SELECT LOBT_TIL FROM ARGS) + ({lobt_after} / 24)
              AND P.MODEL_TYPE = :MODEL
              AND P.LON IS NOT NULL
              AND P.LAT IS NOT NULL
              AND P.TIME_OVER IS NOT NULL
              {where_bbox}
              {where_timeover_buffer}
        """
        params = {"WEF": wef_iso, "TIL": til_iso, "MODEL": profile}
        if bbox:
            params.update(
                {
                    "lon_min": float(bbox["xmin"]),
                    "lon_max": float(bbox["xmax"]),
                    "lat_min": float(bbox["ymin"]),
                    "lat_max": float(bbox["ymax"]),
                }
            )
        if timeover_buffer:
            params.update(
                {
                    "to_before": float(timeover_buffer["before"]),
                    "to_after": float(timeover_buffer["after"]),
                }
            )

        with self.engine.connect() as cx:
            df = pd.read_sql(text(sql_template), cx, params=params)
        df["POINT_ID"] = df["POINT_ID"].fillna("NO_POINT")
        df["AIR_ROUTE"] = df["AIR_ROUTE"].fillna("NO_ROUTE")
        return df

    def point_profiles_tidy(
        self,
        wef: Union[str, datetime],
        til: Optional[Union[str, datetime]] = None,
        profile: str = "CTFM",
        bbox: Optional[Dict[str, float]] = None,
    ) -> pd.DataFrame:
        """
        Convenience wrapper to export point profiles for a window/model.

        Parameters
        ----------
        wef : str or datetime.datetime
            Start of the time window (inclusive).
        til : str or datetime.datetime, optional
            End of the time window (exclusive). If None, defaults to current
            UTC midnight (00:00:00) of today.
        profile : str, default "CTFM"
            Trajectory model type.
        bbox : dict[str, float], optional
            Axis-aligned bounding box.

        Returns
        -------
        pandas.DataFrame
            See :meth:`export_model_trajectory`.
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

    def generate_so6(self, trajectory: pd.DataFrame) -> pd.DataFrame:
        """
        Convert point profile trajectories into SO6 segment format.

        Parameters
        ----------
        trajectory : pandas.DataFrame
            Point profile dataframe with columns:
            ``FLIGHT_ID``, ``TIME_OVER``, ``LONGITUDE``, ``LATITUDE``,
            ``FLIGHT_LEVEL``, ``POINT_ID``, ``AIR_ROUTE``, ``LOBT``,
            ``SEQ_ID``, ``CALLSIGN``, ``REGISTRATION``, ``MODEL_TYPE``,
            ``AIRCRAFT_TYPE``, ``AIRCRAFT_OPERATOR``, ``ADEP``, ``ADES``.

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
