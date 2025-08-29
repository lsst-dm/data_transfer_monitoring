"""
Time conversion utilities for TAI (International Atomic Time) and UTC.

As Marcus Aurelius once said: "Time is a sort of river of passing events,
and strong is its current." This module helps navigate those currents between
different temporal reference frames.
"""

from datetime import datetime
from typing import Union, Optional, List
import numpy as np
from astropy.time import Time
from astropy import units as u


def tai_to_utc(
    tai_time: Union[float, datetime, str, List[float], np.ndarray],
    format: Optional[str] = None,
    scale: str = 'tai'
) -> Time:
    """
    Convert TAI (International Atomic Time) to UTC.

    TAI is a continuous time scale that doesn't have leap seconds,
    while UTC occasionally adds leap seconds to stay synchronized
    with Earth's rotation. As of 2017, TAI is ahead of UTC by 37 seconds.

    Parameters
    ----------
    tai_time : float, datetime, str, list of floats, or numpy array
        The TAI time to convert. Can be:
        - MJD (Modified Julian Date) as float
        - ISO format string (e.g., '2024-01-15 12:00:00')
        - datetime object
        - Unix timestamp (seconds since 1970-01-01) as float or string
        - String containing Unix timestamp (e.g., "1756423395.2931683")
        - List or array of any of the above
    format : str, optional
        The format of the input time. Common formats:
        - 'mjd': Modified Julian Date
        - 'iso': ISO 8601 string format
        - 'datetime': Python datetime object
        - 'unix': Unix timestamp
        - 'jd': Julian Date
        If None, astropy will attempt to infer the format.
        For Unix timestamp strings, format will be set to 'unix' automatically.
    scale : str, default='tai'
        The time scale of the input. Should typically be 'tai'.

    Returns
    -------
    astropy.time.Time
        Time object in UTC scale. Access different formats via:
        - .iso for ISO string
        - .datetime for Python datetime
        - .mjd for Modified Julian Date
        - .unix for Unix timestamp

    Examples
    --------
    >>> # Convert TAI MJD to UTC
    >>> tai_mjd = 60000.5
    >>> utc_time = tai_to_utc(tai_mjd, format='mjd')
    >>> print(utc_time.iso)

    >>> # Convert TAI Unix timestamp string to UTC
    >>> tai_unix_str = "1756423395.2931683"
    >>> utc_time = tai_to_utc(tai_unix_str)
    >>> print(utc_time.iso)

    >>> # Convert TAI ISO string to UTC
    >>> tai_iso = '2024-01-15 12:00:00'
    >>> utc_time = tai_to_utc(tai_iso, format='iso')
    >>> print(utc_time.datetime)

    >>> # Batch conversion of multiple TAI times
    >>> tai_times = [60000.0, 60001.0, 60002.0]
    >>> utc_times = tai_to_utc(tai_times, format='mjd')
    >>> for t in utc_times:
    ...     print(t.iso)
    """
    # Handle Unix timestamp strings - detect and convert
    if isinstance(tai_time, str) and format is None:
        # Check if string looks like a Unix timestamp (digits and optional decimal point)
        try:
            # Try to convert to float - if successful, it's likely a Unix timestamp
            tai_time_float = float(tai_time)
            # Unix timestamps are typically > 1e9 (after year 2001)
            # and < 2e9 (before year 2033)
            if 1e8 < tai_time_float < 1e11:  # Reasonable Unix timestamp range
                tai_time = tai_time_float
                format = 'unix'
        except (ValueError, TypeError):
            # Not a numeric string, let astropy handle it
            pass

    # Create Time object in TAI scale
    tai_time_obj = Time(tai_time, format=format, scale=scale)

    # Convert to UTC scale
    utc_time_obj = tai_time_obj.utc

    return utc_time_obj


def utc_to_tai(
    utc_time: Union[float, datetime, str, List[float], np.ndarray],
    format: Optional[str] = None,
    scale: str = 'utc'
) -> Time:
    """
    Convert UTC to TAI (International Atomic Time).

    The inverse operation of tai_to_utc. Useful when you need
    continuous time without leap second discontinuities.

    Parameters
    ----------
    utc_time : float, datetime, str, list of floats, or numpy array
        The UTC time to convert.
    format : str, optional
        The format of the input time (see tai_to_utc for options).
    scale : str, default='utc'
        The time scale of the input. Should typically be 'utc'.

    Returns
    -------
    astropy.time.Time
        Time object in TAI scale.

    Examples
    --------
    >>> utc_iso = '2024-01-15 12:00:00'
    >>> tai_time = utc_to_tai(utc_iso, format='iso')
    >>> print(tai_time.mjd)
    """
    # Create Time object in UTC scale
    utc_time_obj = Time(utc_time, format=format, scale=scale)

    # Convert to TAI scale
    tai_time_obj = utc_time_obj.tai

    return tai_time_obj


def tai_utc_offset(
    time: Union[float, datetime, str],
    format: Optional[str] = None
) -> float:
    """
    Calculate the offset between TAI and UTC at a given time.

    This offset changes whenever a leap second is added to UTC.
    As Caesar would say: "Experience is the teacher of all things,"
    and experience has taught us that Earth's rotation is irregular!

    Parameters
    ----------
    time : float, datetime, or str
        The time at which to calculate the offset.
    format : str, optional
        The format of the input time.

    Returns
    -------
    float
        The TAI-UTC offset in seconds.

    Examples
    --------
    >>> # Get current TAI-UTC offset
    >>> offset = tai_utc_offset('2024-01-15', format='iso')
    >>> print(f"TAI is ahead of UTC by {offset} seconds")
    """
    # Create time object (format will be inferred if not specified)
    t = Time(time, format=format)

    # Calculate offset: TAI - UTC
    offset = (t.tai - t.utc).to(u.second).value

    return offset


def convert_time_array(
    times: Union[List, np.ndarray],
    from_scale: str,
    to_scale: str,
    format: Optional[str] = None
) -> np.ndarray:
    """
    Efficiently convert an array of times between different time scales.

    A more general function following the DRY principle, allowing conversion
    between any supported time scales (TAI, UTC, TT, TCB, TCG, TDB, UT1).

    Parameters
    ----------
    times : list or numpy array
        Array of times to convert.
    from_scale : str
        Source time scale ('tai', 'utc', 'tt', etc.).
    to_scale : str
        Target time scale ('tai', 'utc', 'tt', etc.).
    format : str, optional
        Format of the input times.

    Returns
    -------
    numpy.ndarray
        Converted times in the same format as input.

    Examples
    --------
    >>> mjd_times = np.array([60000.0, 60001.0, 60002.0])
    >>> utc_times = convert_time_array(mjd_times, 'tai', 'utc', format='mjd')
    """
    # Create Time object array in source scale
    time_obj = Time(times, format=format, scale=from_scale)

    # Convert to target scale
    converted = getattr(time_obj, to_scale)

    # Return in the same format as input
    if format == 'mjd':
        return converted.mjd
    elif format == 'jd':
        return converted.jd
    elif format == 'unix':
        return converted.unix
    elif format == 'iso':
        return converted.iso
    else:
        # Return as MJD by default for numeric consistency
        return converted.mjd


def get_leap_seconds_table():
    """
    Get information about all leap seconds that have been applied.

    Returns a formatted string with leap second history, because
    as Seneca said: "Every new thing excites the mind, but a mind
    that seeks truth turns from the new and seeks the old."

    Returns
    -------
    str
        Formatted table of leap second information.
    """
    from astropy.utils.iers import conf

    # This will show when leap seconds were added
    info = []
    info.append("=" * 60)
    info.append("LEAP SECONDS HISTORY")
    info.append("=" * 60)
    info.append("Leap seconds maintain UTC within 0.9s of UT1")
    info.append("TAI-UTC offset increases by 1 second with each leap second")
    info.append("-" * 60)

    # Calculate offset at various important dates
    important_dates = [
        ('1972-01-01', 'Initial offset at start of leap seconds'),
        ('1980-01-01', 'GPS epoch'),
        ('2000-01-01', 'Y2K millennium'),
        ('2017-01-01', 'Last leap second (as of 2024)'),
        ('2024-01-01', 'Recent reference'),
    ]

    for date_str, description in important_dates:
        t = Time(date_str, format='iso')
        offset = (t.tai - t.utc).to(u.second).value
        info.append(f"{date_str}: TAI-UTC = {offset:+.0f}s  ({description})")

    return '\n'.join(info)


# Convenience functions for common use cases
def tai_mjd_to_utc_iso(tai_mjd: float) -> str:
    """Convert TAI MJD directly to UTC ISO string."""
    return tai_to_utc(tai_mjd, format='mjd').iso


def utc_iso_to_tai_mjd(utc_iso: str) -> float:
    """Convert UTC ISO string directly to TAI MJD."""
    return utc_to_tai(utc_iso, format='iso').mjd


def current_tai_time() -> Time:
    """Get the current time in TAI scale."""
    return Time.now().tai


def current_utc_time() -> Time:
    """Get the current time in UTC scale."""
    return Time.now().utc


# As Cicero proclaimed: "Nothing is more noble, nothing more venerable
# than fidelity." May this code serve you faithfully in your temporal endeavors!
