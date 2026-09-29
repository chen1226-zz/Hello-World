"""Aggregate a UTC event stream into per-day counts in a user's local timezone.

All day boundaries are derived from the real IANA rules via ``zoneinfo``:
the offset in effect is evaluated at each concrete local moment, never
cached from a reference instant and never hard-coded.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Iterable, Union
from zoneinfo import ZoneInfo

ZoneLike = Union[str, ZoneInfo]


def _tz(zone: ZoneLike) -> ZoneInfo:
    return zone if isinstance(zone, ZoneInfo) else ZoneInfo(str(zone))


def local_day_key(ts: datetime, zone: ZoneLike) -> date:
    """Return the local calendar date of the instant ``ts`` in ``zone``.

    ``ts`` must be timezone-aware. The offset applied is the one in effect
    at that exact instant, so DST transitions, 30-minute and 45-minute
    offsets are all handled by the tz database itself.
    """
    if ts.tzinfo is None or ts.utcoffset() is None:
        raise ValueError("ts must be a timezone-aware datetime")
    return ts.astimezone(_tz(zone)).date()


def _local_midnight(day: date, tz: ZoneInfo) -> datetime:
    """The instant of local midnight at the start of ``day`` in ``tz``."""
    return datetime.combine(day, time.min, tzinfo=tz)


def day_range(day: date, zone: ZoneLike) -> tuple[datetime, datetime]:
    """Return ``[start, end)`` instants covering exactly one local day.

    ``start`` is local midnight of ``day`` and ``end`` is local midnight of
    the next day. Both are returned as UTC-aware datetimes so that
    ``end - start`` is the true elapsed span: 23h on spring-forward days,
    25h on fall-back days (23.5h/24.5h for Lord Howe), 24h otherwise.
    (Subtracting two datetimes that share one ``ZoneInfo`` would silently
    give wall-clock arithmetic instead.)
    """
    tz = _tz(zone)
    start = _local_midnight(day, tz).astimezone(timezone.utc)
    end = _local_midnight(day + timedelta(days=1), tz).astimezone(timezone.utc)
    return start, end


def aggregate_by_day(events: Iterable[datetime], zone: ZoneLike) -> dict[date, int]:
    """Count events per local day in ``zone``.

    Each event is bucketed with :func:`local_day_key`, so the result is
    identical to bucketing every event individually and no instant can be
    counted twice or dropped: local midnights belong to the day they start.
    """
    tz = _tz(zone)
    counts: dict[date, int] = {}
    for ts in events:
        key = local_day_key(ts, tz)
        counts[key] = counts.get(key, 0) + 1
    return counts
