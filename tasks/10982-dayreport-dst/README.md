# dayreport — DST-safe daily aggregation

`dayreport.py` buckets a stream of timezone-aware UTC instants into per-day
counts in a user's local timezone.

## The bug

The original implementation picked **one fixed UTC offset per timezone** —
effectively evaluating `utcoffset()` once at a reference moment (or using a
hard-coded offset) and reusing it for every event and every day boundary:

- `local_day_key` computed `ts + cached_offset`, so around a DST switch an
  event could be stamped with the wrong local date.
- `day_range` built every day as `midnight_utc ± fixed_offset` for exactly
  24 hours, so on transition days the window was shifted by an hour and the
  day silently gained or lost an hour of events.
- The unit tests only exercised UTC, where a cached offset is always
  correct, so nothing caught it. Zones with 30-minute (`Australia/Lord_Howe`)
  or 45-minute (`Pacific/Chatham`, `Asia/Kathmandu`) offsets made it worse:
  their boundaries never align to whole UTC hours at all.

## The fix

All local-time reasoning goes through `zoneinfo.ZoneInfo`, which applies
the real IANA rules **at each concrete instant** — nothing is cached and no
offset is hard-coded:

- `local_day_key(ts, zone)` converts with `ts.astimezone(zone)` and takes
  the local `.date()`, so the offset in effect at that exact moment is used.
- `day_range(day, zone)` returns `[start, end)` where the endpoints are the
  *local midnights* of `day` and `day + 1`, converted to UTC. The span is
  therefore exactly one local day: 23 h on spring-forward days, 25 h on
  fall-back days (23.5 h / 24.5 h for Lord Howe's 30-minute shift).
  The endpoints are returned in UTC on purpose: subtracting two datetimes
  that share one `ZoneInfo` does wall-clock arithmetic and would hide the
  23/25-hour lengths.
- `aggregate_by_day` buckets every event with `local_day_key`, so its
  result is identical to per-event bucketing and each instant lands in
  exactly one day. Because ranges are half-open (`[start, end)`), an event
  at local midnight belongs to the day that starts there.

Edge case: in zones whose transition happens *at* midnight (e.g.
`America/Santiago`), local 00:00 does not exist on that day; the day then
starts at the first valid instant, which is exactly what the computed
`start` instant represents.

## Verify

```sh
python3 repro.py                      # OK: 10 zones, 0 mismatches
python3 -m unittest discover -s tests # OK
```
