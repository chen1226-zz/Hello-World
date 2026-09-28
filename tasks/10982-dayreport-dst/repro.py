"""复现脚本：用独立参照实现比对 dayreport 的本地日归属。

参照实现完全不使用 dayreport 的内部函数，只用 zoneinfo 的 astimezone 做换算。
"""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import dayreport

ZONES = [
    ("America/New_York", "2024-03-11T04:30:00Z"),
    ("America/New_York", "2024-11-03T04:30:00Z"),
    ("America/New_York", "2024-11-04T05:30:00Z"),
    ("Europe/Berlin", "2024-03-31T22:30:00Z"),
    ("Europe/Berlin", "2024-10-28T23:30:00Z"),
    ("Australia/Lord_Howe", "2024-06-01T13:20:00Z"),
    ("Australia/Lord_Howe", "2024-12-01T13:20:00Z"),
    ("Asia/Kathmandu", "2024-06-01T18:15:00Z"),
    ("Pacific/Chatham", "2024-09-28T11:45:00Z"),
    ("Asia/Shanghai", "2024-06-01T16:00:00Z"),
]


def reference_day_key(ts, zone):
    return datetime.fromtimestamp(ts, timezone.utc).astimezone(ZoneInfo(zone)).strftime("%Y-%m-%d")


def main():
    mismatches = []
    for zone, iso in ZONES:
        ts = datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()
        want = reference_day_key(ts, zone)
        got = dayreport.local_day_key(ts, zone)
        if want != got:
            mismatches.append((zone, iso, want, got))

    # 边界：本地 00:00:00 的事件必须归到当天
    ny = ZoneInfo("America/New_York")
    for day in ("2024-03-10", "2024-11-03"):
        start, _ = dayreport.day_range(day, ny.key)
        ts = datetime.fromisoformat(f"{day}T00:00:00").replace(tzinfo=ny).timestamp()
        if not (start <= ts):
            mismatches.append((ny.key, f"{day} 00:00 本地零点", day, "被排除在区间外"))
        if dayreport.local_day_key(ts, ny.key) != day:
            mismatches.append((ny.key, f"{day} 00:00 本地零点", day, dayreport.local_day_key(ts, ny.key)))

    # 切换日的实际长度必须是 23 / 25 小时
    for day, hours in (("2024-03-10", 23), ("2024-11-03", 25)):
        start, end = dayreport.day_range(day, "America/New_York")
        real = (end - start) / 3600.0
        if abs(real - hours) > 1e-9:
            mismatches.append(("America/New_York", f"{day} 全天长度", f"{hours}h", f"{real}h"))

    if mismatches:
        print(f"FAIL: {len(mismatches)} mismatches")
        for zone, when, want, got in mismatches:
            print(f"  {zone:<22} {when:<26} 期望 {want}  实际 {got}")
        raise SystemExit(1)

    print(f"OK: {len(ZONES)} zones, 0 mismatches")


if __name__ == "__main__":
    main()
