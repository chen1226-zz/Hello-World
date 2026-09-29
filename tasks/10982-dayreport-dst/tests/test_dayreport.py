import unittest
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from dayreport import aggregate_by_day, day_range, local_day_key

UTC = timezone.utc
US = timedelta(microseconds=1)


def utc(y, m, d, hh=0, mm=0, ss=0):
    return datetime(y, m, d, hh, mm, ss, tzinfo=UTC)


class LocalDayKeyTest(unittest.TestCase):
    def test_same_instant_different_zones(self):
        # 2026-01-15T12:00Z is still Jan 15 in New York and Kathmandu,
        # but already Jan 16 in Auckland (UTC+13).
        ts = utc(2026, 1, 15, 12)
        self.assertEqual(local_day_key(ts, "America/New_York"), date(2026, 1, 15))
        self.assertEqual(local_day_key(ts, "Asia/Kathmandu"), date(2026, 1, 15))
        self.assertEqual(local_day_key(ts, "Pacific/Auckland"), date(2026, 1, 16))

    def test_offset_applied_at_the_instant_not_cached(self):
        # Two instants 24h apart straddle the US spring forward; the same
        # UTC clock time maps to different local dates only if the offset
        # is evaluated per instant.
        before = utc(2026, 3, 7, 12)  # EST (UTC-5) -> 07:00 local, Mar 7
        after = utc(2026, 4, 7, 12)   # EDT (UTC-4) -> 08:00 local, Apr 7
        self.assertEqual(local_day_key(before, "America/New_York"), date(2026, 3, 7))
        self.assertEqual(local_day_key(after, "America/New_York"), date(2026, 4, 7))

    def test_exact_local_midnight_belongs_to_that_day(self):
        # Local midnight in Kathmandu (UTC+5:45) is 18:15 UTC the day before.
        ts = utc(2026, 6, 14, 18, 15)
        self.assertEqual(local_day_key(ts, "Asia/Kathmandu"), date(2026, 6, 15))
        self.assertEqual(
            local_day_key(ts - US, "Asia/Kathmandu"), date(2026, 6, 14)
        )

    def test_naive_datetime_rejected(self):
        with self.assertRaises(ValueError):
            local_day_key(datetime(2026, 1, 1), "UTC")


class DayRangeTest(unittest.TestCase):
    def assert_span(self, zone, day, hours):
        start, end = day_range(day, zone)
        self.assertEqual(end - start, timedelta(hours=hours), f"{zone} {day}")
        return start, end

    def test_normal_day_is_24_hours(self):
        self.assert_span("America/New_York", date(2026, 3, 7), 24)
        self.assert_span("UTC", date(2026, 3, 8), 24)
        self.assert_span("Asia/Kathmandu", date(2026, 6, 15), 24)

    def test_whole_hour_dst_transition_days(self):
        # US: spring forward 2026-03-08 (23h), fall back 2026-11-01 (25h).
        self.assert_span("America/New_York", date(2026, 3, 8), 23)
        self.assert_span("America/New_York", date(2026, 11, 1), 25)
        # UK: spring forward 2026-03-29, fall back 2026-10-25.
        self.assert_span("Europe/London", date(2026, 3, 29), 23)
        self.assert_span("Europe/London", date(2026, 10, 25), 25)

    def test_half_hour_dst_shift_lord_howe(self):
        # Lord Howe shifts by 30 minutes: 23.5h / 24.5h transition days.
        self.assert_span("Australia/Lord_Howe", date(2026, 10, 4), 23.5)
        self.assert_span("Australia/Lord_Howe", date(2026, 4, 5), 24.5)

    def test_45_minute_offset_zone(self):
        # Chatham Islands: UTC+12:45 / +13:45 with DST.
        start, _ = day_range(date(2026, 6, 15), "Pacific/Chatham")
        self.assertEqual(start, utc(2026, 6, 14, 11, 15))
        self.assert_span("Pacific/Chatham", date(2026, 9, 27), 23)
        self.assert_span("Pacific/Chatham", date(2026, 4, 5), 25)

    def test_midnight_aligned_to_zone_not_utc(self):
        # Kathmandu local midnight is 18:15 UTC the previous day.
        start, end = day_range(date(2026, 6, 15), "Asia/Kathmandu")
        self.assertEqual(start, utc(2026, 6, 14, 18, 15))
        self.assertEqual(end, utc(2026, 6, 15, 18, 15))

    def test_half_open_interval_boundaries(self):
        zone = "America/New_York"
        day = date(2026, 3, 8)  # spring-forward day
        start, end = day_range(day, zone)
        self.assertEqual(local_day_key(start, zone), day)
        self.assertEqual(local_day_key(start - US, zone), day - timedelta(days=1))
        self.assertEqual(local_day_key(end - US, zone), day)
        self.assertEqual(local_day_key(end, zone), day + timedelta(days=1))

    def test_midnight_transition_zone_santiago(self):
        # Chile springs forward at local midnight: 2026-09-06 has no 00:00,
        # the day is 23h and starts at the first valid instant (04:00 UTC).
        start, end = day_range(date(2026, 9, 6), "America/Santiago")
        self.assertEqual(end - start, timedelta(hours=23))
        self.assertEqual(start, utc(2026, 9, 6, 4))
        self.assertEqual(local_day_key(start, "America/Santiago"), date(2026, 9, 6))
        self.assertEqual(
            local_day_key(start - US, "America/Santiago"), date(2026, 9, 5)
        )


class AggregateByDayTest(unittest.TestCase):
    def test_matches_local_day_key_per_event(self):
        zone = "Australia/Lord_Howe"
        events = [
            utc(2026, 10, 3, 13, 30),      # local midnight, start of Oct 4
            utc(2026, 10, 3, 13, 30) - US,  # 1us before: still Oct 3
            utc(2026, 10, 4, 13, 0),       # local midnight, start of Oct 5
            utc(2026, 10, 4, 12, 59, 59),  # inside Oct 4 (23.5h day)
            utc(2026, 4, 4, 13, 0),        # local midnight, start of Apr 5
            utc(2026, 4, 5, 13, 30),       # local midnight, start of Apr 6
        ]
        expected = {}
        for ts in events:
            key = local_day_key(ts, zone)
            expected[key] = expected.get(key, 0) + 1
        self.assertEqual(aggregate_by_day(events, zone), expected)
        self.assertEqual(
            aggregate_by_day(events, zone),
            {
                date(2026, 10, 3): 1,
                date(2026, 10, 4): 2,
                date(2026, 10, 5): 1,
                date(2026, 4, 5): 1,
                date(2026, 4, 6): 1,
            },
        )

    def test_no_event_double_counted_or_dropped(self):
        zone = "Pacific/Chatham"
        start, _ = day_range(date(2026, 9, 27), zone)  # spring-forward day
        events = [start + timedelta(minutes=30) * i for i in range(48)]
        counts = aggregate_by_day(events, zone)
        self.assertEqual(sum(counts.values()), len(events))
        for ts in events:
            self.assertIn(local_day_key(ts, zone), counts)

    def test_same_instant_bucketed_per_zone(self):
        ts = utc(2026, 1, 15, 12)
        self.assertEqual(
            aggregate_by_day([ts], "Pacific/Auckland"), {date(2026, 1, 16): 1}
        )
        self.assertEqual(
            aggregate_by_day([ts], "America/New_York"), {date(2026, 1, 15): 1}
        )


if __name__ == "__main__":
    unittest.main()
