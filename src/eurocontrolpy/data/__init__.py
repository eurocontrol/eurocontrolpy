"""
Bundled reference datasets.

member_state : pd.DataFrame
    EUROCONTROL Member States with ISO codes and membership status.
    Columns: name, iso3c, iso2c, icao, iso3n, date, status
    (status: 'M' = Member, 'C' = Comprehensive Agreement, None = Kosovo)

aircraft_type and aircraft_model are sourced from the ICAO Aircraft Type
Designators list and are not bundled here due to size and update frequency.
Fetch them via the ICAO API or provide your own CSV and load with pd.read_csv().
"""
from pathlib import Path

import pandas as pd

_DATA_DIR = Path(__file__).parent


def _load_member_state() -> pd.DataFrame:
    df = pd.read_csv(
        _DATA_DIR / "member_state.csv",
        dtype={"iso3n": str},
    )
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df


member_state: pd.DataFrame = _load_member_state()

__all__ = ["member_state"]
