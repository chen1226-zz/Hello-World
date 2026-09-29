"""Reproduction / verification script for the DST day-boundary bug.

For each zone we walk two full years of local days and check, for every
day, that day_range(day, zone) is exactly [local midnight, next local
midnight) and that boundary instants are bucketed into the right day.
Finally we verify aggregate_by_day against per-event local_day_key.

Expected output: OK: 10 zones, 0 mismatches
"""

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from dayreport import aggregate_by_day, day_range, local_day_key

ZONES = [
    "UTC",
    "America/New_York",      # whole-hour DST
    "Europe/London",         # whole-hour DST
    "Europe/Berlin",         # whole-hour DST
    "Australia/Sydney",      # southern-hemisphere DST
    "Australia/Lord_Howe",   # 30-minute DST shift
    "Pacific/Chatham",       # 45-minute offset + DST
    "Pacific/Auckland",      # southern-hemisphere DST
    "Asia/Kathmandu",        # fixed +05:45, no DST
    "America/Santiago",      # southern-hemisphere DST
]

MICROSECOND = timedelta(microseconds=1)


def check_zone(zone_name):
    tz = ZoneInfo(zone_name)
    mismatches = []
    events = []
    day = date(2025, 1, 1)
    last_day = date(2027, 1, 1)

    while day < last_day:
        start, end = day_range(day, tz)

        # 1. Endpoints must bracket exactly this local day. (When a
        #    transition falls at midnight, e.g. America/Santiago, local
        #    00:00 does not exist; the day then starts at the first valid
        #    instant, which the boundary probes below verify exactly.)
        s_local = start.astimezone(tz)
        e_local = end.astimezone(tz)
        if s_local.date() != day:
            mismatches.append(f"{day}: start {s_local} is not in this local day")
        if e_local.date() != day + timedelta(days=1):
            mismatches.append(f"{day}: end {e_local} is not in the next local day")
        if not start < end:
            mismatches.append(f"{day}: empty/inverted range")

        # 2. Boundary instants must land in the right local day, and the
        #    half-open interval must match local_day_key exactly.
        probes = [
            (start - MICROSECOND, day - timedelta(days=1)),
            (start, day),
            (start + timedelta(hours=12), day),
            (end - MICROSECOND, day),
            (end, day + timedelta(days=1)),
        ]
        for instant, expected in probes:
            got = local_day_key(instant, tz)
            if got != expected:
                mismatches.append(f"{day}: {instant.isoformat()} -> {got}, want {expected}")
            in_range = start <= instant < end
            if in_range != (got == day):
                mismatches.append(f"{day}: range/membership mismatch at {instant.isoformat()}")
            events.append(instant)

        day += timedelta(days=1)

    # 3. aggregate_by_day must equal per-event local_day_key bucketing.
    expected_counts = {}
    for ts in events:
        key = local_day_key(ts, tz)
        expected_counts[key] = expected_counts.get(key, 0) + 1
    got_counts = aggregate_by_day(events, tz)
    if got_counts != expected_counts:
        mismatches.append("aggregate_by_day disagrees with local_day_key")

    return mismatches


def main():
    total = 0
    for zone_name in ZONES:
        problems = check_zone(zone_name)
        for p in problems[:5]:
            print(f"MISMATCH [{zone_name}] {p}")
        if len(problems) > 5:
            print(f"MISMATCH [{zone_name}] ... and {len(problems) - 5} more")
        total += len(problems)
    status = "OK" if total == 0 else "FAIL"
    print(f"{status}: {len(ZONES)} zones, {total} mismatches")
    return 0 if total == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
