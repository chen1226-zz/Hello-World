"""带 TTL + LRU 的进程内缓存。

对外接口（不得更改签名）：
    TTLCache(capacity, ttl, clock=None, on_evict=None)
        .get(key) -> value | None
        .put(key, value)
        .stats() -> dict

约定：TTL 到期即不可读；容量超限按 LRU 淘汰；on_evict 必须在锁外调用。
"""

import threading
import time
from collections import OrderedDict


class TTLCache:
    def __init__(self, capacity=128, ttl=60.0, clock=None, on_evict=None):
        self.capacity = capacity
        self.ttl = ttl
        self._clock = clock or time.monotonic
        self._on_evict = on_evict or (lambda key, value: None)
        self._data = OrderedDict()
        self._lock = threading.RLock()
        self.hits = 0
        self.misses = 0
        self.evictions = 0

    def _now(self):
        """读取当前时间：每次都直接询问时钟，绝不缓存。

        旧实现在这里做了"每 64 次调用才刷新一次"的缓存，TTL 判断会用到
        陈旧时间戳，是并发/高频场景下读到过期条目的根因。
        所有调用方都已持有 self._lock。
        """
        return self._clock()

    def _purge_expired_locked(self, now):
        """清掉所有已过期条目（调用方必须持锁）。

        过期失效不属于容量淘汰，与旧实现保持一致：不触发 on_evict 回调。
        """
        expired = [key for key, (_, expires_at) in self._data.items()
                   if now >= expires_at]
        for key in expired:
            del self._data[key]

    def put(self, key, value):
        evicted = []
        with self._lock:
            now = self._now()
            self._purge_expired_locked(now)
            self._data[key] = (value, now + self.ttl)
            while len(self._data) > self.capacity:
                old_key, (old_value, _) = self._data.popitem(last=False)
                self.evictions += 1
                evicted.append((old_key, old_value))
        for item in evicted:
            self._on_evict(*item)

    def get(self, key):
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                self.misses += 1
                return None
            value, expires_at = entry
            now = self._now()
            if now >= expires_at:
                del self._data[key]
                self.misses += 1
                return None
            self._data.move_to_end(key)
            self.hits += 1
            return value

    def stats(self):
        with self._lock:
            self._purge_expired_locked(self._now())
            return {
                "size": len(self._data),
                "capacity": self.capacity,
                "hits": self.hits,
                "misses": self.misses,
                "evictions": self.evictions,
            }
