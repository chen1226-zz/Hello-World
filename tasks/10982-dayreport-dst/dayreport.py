"""按用户本地时区把事件流聚合成日报。

对外接口（不得更改签名）：
    local_day_key(ts, zone)          -> "YYYY-MM-DD"
    aggregate_by_day(events, zone)   -> {day: sum}

本地日归属一律按「事件时刻在该时区的真实规则」换算：偏移量随时刻变化，
绝不缓存某个基准时刻的偏移，因此夏令时（含 30/45 分钟偏移的时区）切换
当天也能正确处理。
"""

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo


def local_day_key(ts, zone):
    """把 UNIX 时间戳换算成该时区的自然日。"""
    local = datetime.fromtimestamp(ts, timezone.utc).astimezone(ZoneInfo(zone))
    return local.strftime("%Y-%m-%d")


def day_range(day, zone):
    """返回某个自然日在该时区下的 [start, end) 时间戳区间。

    start/end 分别是该日本地零点与次日本地零点对应的绝对时刻。夏令时
    开始当天区间为 23 小时、结束当天为 25 小时；30/45 分钟偏移的时区
    同理。区间为左闭右开，本地零点事件落在 start 上，归入当天。
    """
    tz = ZoneInfo(zone)
    start_local = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=tz)
    end_local = start_local + timedelta(days=1)
    return start_local.timestamp(), end_local.timestamp()


def aggregate_by_day(events, zone):
    """events 为 (ts, value) 序列，按本地自然日求和。

    每个事件只经 local_day_key 归属一次，因此结果与逐条调用
    local_day_key 完全一致，同一事件不会被计入两天。
    """
    totals = {}
    for ts, value in events:
        key = local_day_key(ts, zone)
        totals[key] = totals.get(key, 0) + value
    return totals
