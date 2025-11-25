from datetime import datetime, timezone
from typing import Union, Optional
import astropy.time
import astropy.units

def get_observation_day(time: Optional[Union[datetime, astropy.time.Time]] = None) -> str:
    """
    Get the observation day for a given time using the TAI-based definition.

    The observation day is defined as the TAI date of an instant 12 hours before
    the timestamp. This ensures that all observations during a night are grouped
    under the same observation day.

    Parameters
    ----------
    time : datetime, astropy.time.Time, or None
        The timestamp to convert. If None, uses the current UTC time.
        If a datetime is provided, it should be timezone-aware (preferably UTC).

    Returns
    -------
    day_obs : str
        The observation day corresponding to the time, in YYYY-MM-DD format.

    Examples
    --------
    >>> from datetime import datetime, timezone
    >>> import astropy.time
    >>>
    >>> # Using current time
    >>> day_obs = get_observation_day()
    >>>
    >>> # Using a datetime
    >>> dt = datetime(2024, 1, 15, 3, 0, 0, tzinfo=timezone.utc)
    >>> day_obs = get_observation_day(dt)  # Returns '2024-01-14' (previous day)
    >>>
    >>> # Using an astropy Time
    >>> t = astropy.time.Time('2024-01-15T15:00:00', scale='utc')
    >>> day_obs = get_observation_day(t)  # Returns '2024-01-15' (same day)
    """
    # Handle None case - use current time
    if time is None:
        time = datetime.now(timezone.utc)

    # Convert datetime to astropy.time.Time if necessary
    if isinstance(time, datetime):
        # Ensure datetime is timezone-aware
        if time.tzinfo is None:
            # Assume UTC if no timezone is specified
            time = time.replace(tzinfo=timezone.utc)
        # Convert to astropy Time
        time = astropy.time.Time(time)
    elif not isinstance(time, astropy.time.Time):
        raise TypeError(f"time must be datetime or astropy.time.Time, got {type(time)}")

    # Apply the 12-hour offset in TAI scale
    day_obs_delta = astropy.time.TimeDelta(-12.0 * astropy.units.hour, scale="tai")

    # Calculate the observation day and return as ISO date string
    return (time + day_obs_delta).tai.to_value("iso", "date")


def get_day_obs(time: Optional[Union[datetime, astropy.time.Time]] = None) -> str:
    """
    Alias for get_observation_day for backward compatibility.

    Convert a timestamp into a day-obs string using the TAI-based definition.

    Parameters
    ----------
    time : datetime, astropy.time.Time, or None
        The timestamp to convert. If None, uses the current UTC time.

    Returns
    -------
    day_obs : str
        The day-obs corresponding to the time, in YYYY-MM-DD format.
    """
    return get_observation_day(time)


# Legacy function for compatibility if needed elsewhere in the codebase
def get_observation_day_as_date(time: Optional[Union[datetime, astropy.time.Time]] = None) -> datetime.date:
    """
    Get the observation day as a datetime.date object.

    This is a wrapper around get_observation_day that returns a datetime.date
    instead of a string, for backward compatibility with code expecting that type.

    Parameters
    ----------
    time : datetime, astropy.time.Time, or None
        The timestamp to convert. If None, uses the current UTC time.

    Returns
    -------
    day_obs : datetime.date
        The observation day as a date object.
    """
    day_obs_str = get_observation_day(time)
    # Parse the YYYY-MM-DD string back to a date
    return datetime.fromisoformat(day_obs_str).date()
