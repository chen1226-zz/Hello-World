import unittest
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import dayreport


def ts_of(iso):
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()


class TestLocalDayKey(unittest.TestCase):
    def test_utc_day_key_for_shanghai(self):
        """既有断言：东八区无夏令时，换算必须正确。"""
        self.assertEqual(dayreport.local_day_key(ts_of("2024-06-01T16:00:00Z"), "Asia/Shanghai"), "2024-06-02")
        self.assertEqual(dayreport.local_day_key(ts_of("2024-06-01T15:59:59Z"), "Asia/Shanghai"), "2024-06-01")

    def test_utc_offset_zone_without_dst(self):
        """既有断言：45 分钟偏移的时区。"""
        self.assertEqual(dayreport.local_day_key(ts_of("2024-06-01T18:15:00Z"), "Asia/Kathmandu"), "2024-06-02")


class TestDayRange(unittest.TestCase):
    def test_range_covers_twenty_four_hours_in_utc_zone(self):
        """既有断言：UTC 时区的一天是 24 小时。"""
        start, end = dayreport.day_range("2024-06-01", "UTC")
        self.assertAlmostEqual(end - start, 86400, places=6)
        self.assertEqual(start, ts_of("2024-06-01T00:00:00Z"))


if __name__ == "__main__":
    unittest.main()

class TestDstBoundaryKeys(unittest.TestCase):
    """新增：夏令时切换当天，切换前后各时刻的日归属。"""

    def test_new_york_spring_forward(self):
        """2024-03-10 02:00 -> 03:00 跳变，UTC 04:00 已是 03-11 本地零点。"""
        self.assertEqual(dayreport.local_day_key(ts_of("2024-03-10T06:59:59Z"), "America/New_York"), "2024-03-10")
        self.assertEqual(dayreport.local_day_key(ts_of("2024-03-10T07:00:00Z"), "America/New_York"), "2024-03-10")
        self.assertEqual(dayreport.local_day_key(ts_of("2024-03-11T03:59:59Z"), "America/New_York"), "2024-03-10")
        self.assertEqual(dayreport.local_day_key(ts_of("2024-03-11T04:00:00Z"), "America/New_York"), "2024-03-11")
        self.assertEqual(dayreport.local_day_key(ts_of("2024-03-11T04:30:00Z"), "America/New_York"), "2024-03-11")

    def test_new_york_fall_back(self):
        """2024-11-03 02:00 回拨，04:30Z 是本地 00:30，UTC 05:00 是次日零点。"""
        self.assertEqual(dayreport.local_day_key(ts_of("2024-11-03T04:00:00Z"), "America/New_York"), "2024-11-03")
        self.assertEqual(dayreport.local_day_key(ts_of("2024-11-03T04:30:00Z"), "America/New_York"), "2024-11-03")
        self.assertEqual(dayreport.local_day_key(ts_of("2024-11-04T04:59:59Z"), "America/New_York"), "2024-11-03")
        self.assertEqual(dayreport.local_day_key(ts_of("2024-11-04T05:00:00Z"), "America/New_York"), "2024-11-04")


class TestDayRangeDstLengths(unittest.TestCase):
    """新增：切换日区间长度与 [start, end) 边界。"""

    def check_range(self, zone, day, hours, start_iso, end_iso):
        start, end = dayreport.day_range(day, zone)
        self.assertAlmostEqual(end - start, hours * 3600, places=6)
        self.assertEqual(start, ts_of(start_iso))
        self.assertEqual(end, ts_of(end_iso))
        self.assertEqual(dayreport.local_day_key(start, zone), day)
        self.assertEqual(dayreport.local_day_key(start - 1, zone) != day, True)
        self.assertEqual(dayreport.local_day_key(end, zone) != day, True)
        self.assertEqual(dayreport.local_day_key(end - 1, zone), day)

    def test_new_york_transition_days(self):
        self.check_range("America/New_York", "2024-03-10", 23,
                         "2024-03-10T05:00:00Z", "2024-03-11T04:00:00Z")
        self.check_range("America/New_York", "2024-11-03", 25,
                         "2024-11-03T04:00:00Z", "2024-11-04T05:00:00Z")

    def test_lord_howe_half_hour_transitions(self):
        """Lord Howe：+10:30/+11 互切，切换日为 24.5 / 23.5 小时。"""
        self.check_range("Australia/Lord_Howe", "2024-04-07", 24.5,
                         "2024-04-06T13:00:00Z", "2024-04-07T13:30:00Z")
        self.check_range("Australia/Lord_Howe", "2024-10-06", 23.5,
                         "2024-10-05T13:30:00Z", "2024-10-06T13:00:00Z")

    def test_chatham_three_quarter_offset_transitions(self):
        """Chatham：+12:45/+13:45 互切，切换日为 25 / 23 小时。"""
        self.check_range("Pacific/Chatham", "2024-04-07", 25,
                         "2024-04-06T10:15:00Z", "2024-04-07T11:15:00Z")
        self.check_range("Pacific/Chatham", "2024-09-29", 23,
                         "2024-09-28T11:15:00Z", "2024-09-29T10:15:00Z")


class TestFractionalOffsetDayKeys(unittest.TestCase):
    """新增：30/45 分钟偏移时区在其冬季/夏季偏移下的日归属。"""

    def test_lord_howe(self):
        self.assertEqual(dayreport.local_day_key(ts_of("2024-06-01T13:20:00Z"), "Australia/Lord_Howe"), "2024-06-01")
        self.assertEqual(dayreport.local_day_key(ts_of("2024-12-01T13:20:00Z"), "Australia/Lord_Howe"), "2024-12-02")

    def test_chatham_and_kathmandu(self):
        self.assertEqual(dayreport.local_day_key(ts_of("2024-09-28T11:14:59Z"), "Pacific/Chatham"), "2024-09-28")
        self.assertEqual(dayreport.local_day_key(ts_of("2024-09-28T11:15:00Z"), "Pacific/Chatham"), "2024-09-29")
        self.assertEqual(dayreport.local_day_key(ts_of("2024-06-01T18:14:59Z"), "Asia/Kathmandu"), "2024-06-01")
        self.assertEqual(dayreport.local_day_key(ts_of("2024-06-01T18:15:00Z"), "Asia/Kathmandu"), "2024-06-02")


class TestSameInstantAcrossZones(unittest.TestCase):
    """新增：同一 UTC 时刻在不同时区可能落在不同自然日。"""

    def test_fall_back_instant(self):
        ts = ts_of("2024-11-03T04:30:00Z")
        expect = {
            "UTC": "2024-11-03", "Europe/Berlin": "2024-11-03",
            "America/New_York": "2024-11-03", "America/Chicago": "2024-11-02",
            "Asia/Shanghai": "2024-11-03",
        }
        for zone, day in expect.items():
            self.assertEqual(dayreport.local_day_key(ts, zone), day, zone)

    def test_berlin_spring_forward_instant(self):
        ts = ts_of("2024-03-31T22:30:00Z")
        expect = {
            "UTC": "2024-03-31", "Europe/Berlin": "2024-04-01",
            "America/New_York": "2024-03-31", "Asia/Shanghai": "2024-04-01",
        }
        for zone, day in expect.items():
            self.assertEqual(dayreport.local_day_key(ts, zone), day, zone)


class TestLocalMidnightEvents(unittest.TestCase):
    """新增：恰好落在本地零点的事件归入当天，且等于区间起点。"""

    def test_midnight_equals_start(self):
        for zone, day in (("America/New_York", "2024-03-10"),
                          ("America/New_York", "2024-11-03"),
                          ("Australia/Lord_Howe", "2024-04-07"),
                          ("Pacific/Chatham", "2024-09-29"),
                          ("Asia/Kathmandu", "2024-06-01")):
            tz = ZoneInfo(zone)
            midnight = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=tz).timestamp()
            start, end = dayreport.day_range(day, zone)
            self.assertEqual(midnight, start, zone)
            self.assertEqual(dayreport.local_day_key(midnight, zone), day, zone)
            self.assertTrue(start <= midnight < end, zone)
            self.assertEqual(dayreport.local_day_key(midnight - 1, zone) != day, True, zone)


class TestAggregateMatchesPerEventKeys(unittest.TestCase):
    """新增：聚合结果与逐条 local_day_key 完全一致（含切换日密集采样）。"""

    def test_dense_events_across_transitions(self):
        windows = ("2024-03-09T00:00:00Z", "2024-03-12T00:00:00Z",
                   "2024-11-02T00:00:00Z", "2024-11-05T00:00:00Z")
        for zone in ("America/New_York", "Australia/Lord_Howe", "Pacific/Chatham"):
            events, expected = [], {}
            for i in range(0, len(windows), 2):
                begin = ts_of(windows[i])
                finish = ts_of(windows[i + 1])
                t = begin
                while t < finish:
                    value = int(t % 7) + 1
                    events.append((t, value))
                    key = dayreport.local_day_key(t, zone)
                    expected[key] = expected.get(key, 0) + value
                    t += 487
            for day in ("2024-03-10", "2024-11-03", "2024-04-07",
                        "2024-10-06", "2024-09-29"):
                start, end = dayreport.day_range(day, zone)
                events.extend([(start, 3), (end - 1, 5), (end, 7), (start - 1, 11)])
                k = dayreport.local_day_key(start, zone)
                expected[k] = expected.get(k, 0) + 3
                k = dayreport.local_day_key(end - 1, zone)
                expected[k] = expected.get(k, 0) + 5
                k = dayreport.local_day_key(end, zone)
                expected[k] = expected.get(k, 0) + 7
                k = dayreport.local_day_key(start - 1, zone)
                expected[k] = expected.get(k, 0) + 11
            self.assertEqual(dayreport.aggregate_by_day(events, zone), expected, zone)
