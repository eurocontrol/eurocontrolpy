"""
IATA season helpers ported from the eurocontrol R package.

IATA summer season: last Sunday of March → last Saturday of October (inclusive).
IATA winter season: day after summer end → day before next year's summer start.
"""
from __future__ import annotations

import calendar
from datetime import date, datetime, timedelta, timezone
from typing import Union


def _last_weekday_of_month(year: int, month: int, weekday: int) -> date:
    """
    Return the last occurrence of `weekday` (0=Monday … 6=Sunday) in the
    given year/month.
    """
    # Find last day of month, then walk back to the desired weekday.
    last_day = calendar.monthrange(year, month)[1]
    d = date(year, month, last_day)
    offset = (d.weekday() - weekday) % 7
    return d - timedelta(days=offset)


def _summer_start(year: int) -> date:
    """Last Sunday of March."""
    return _last_weekday_of_month(year, 3, 6)  # 6 = Sunday


def _summer_end(year: int) -> date:
    """Last Saturday of October."""
    return _last_weekday_of_month(year, 10, 5)  # 5 = Saturday


def season_iata(
    year: int, season: str = "summer"
) -> tuple[datetime, datetime]:
    """
    Start and end datetimes for an IATA season (both ends inclusive, UTC midnight).

    Parameters
    ----------
    year : int
        The year of the summer season definition.
        For 'winter', this is the year the winter season *starts* in.
    season : str, default 'summer'
        'summer' or 'winter'.

    Returns
    -------
    tuple[datetime, datetime]
        (start, end) as UTC-aware datetimes at midnight.

    Examples
    --------
    >>> season_iata(2024, "summer")
    (datetime(2024, 3, 31, 0, 0, tzinfo=timezone.utc),
     datetime(2024, 10, 26, 0, 0, tzinfo=timezone.utc))
    """
    season = season.lower()
    if season not in ("summer", "winter"):
        raise ValueError(f"season must be 'summer' or 'winter', got '{season}'.")

    def _to_utc(d: date) -> datetime:
        return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)

    if season == "summer":
        start = _to_utc(_summer_start(year))
        end = _to_utc(_summer_end(year))
    else:
        # Winter starts the day after summer ends, ends the day before next summer.
        start = _to_utc(_summer_end(year) + timedelta(days=1))
        end = _to_utc(_summer_start(year + 1) - timedelta(days=1))

    return start, end


def iata_season_for_date(date_input: Union[str, datetime, date]) -> str:
    """
    Return the IATA season name for a given date.

    Parameters
    ----------
    date_input : str | datetime | date
        The date to check. Strings are parsed with ``date.fromisoformat``.

    Returns
    -------
    str
        'summer-yyyy' or 'winter-yyyy', where yyyy is the year of the
        summer season that defines the period (matches R package behaviour).

    Examples
    --------
    >>> iata_season_for_date("2024-06-15")
    'summer-2024'
    >>> iata_season_for_date("2024-12-01")
    'winter-2024'
    >>> iata_season_for_date("2024-01-15")
    'winter-2023'
    """
    if isinstance(date_input, str):
        d = date.fromisoformat(date_input[:10])
    elif isinstance(date_input, datetime):
        d = date_input.date()
    else:
        d = date_input

    year = d.year
    summer_s = _summer_start(year)
    winter_s = _summer_end(year) + timedelta(days=1)  # winter start of year `year`

    if d < summer_s:
        # Before this year's summer → in previous year's winter
        return f"winter-{year - 1}"
    elif d < winter_s:
        return f"summer-{year}"
    else:
        return f"winter-{year}"


def iata_season_year(date_input: Union[str, datetime, date]) -> int:
    """
    Return the year component of the IATA season for a given date.

    This matches ``iata_season_year`` from the R package.
    """
    return int(iata_season_for_date(date_input)[-4:])
