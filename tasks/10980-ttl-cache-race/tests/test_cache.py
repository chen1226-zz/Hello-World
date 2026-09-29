import unittest

from cache import TTLCache


class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t

    def advance(self, s):
        self.t += s


class TestTTLCache(unittest.TestCase):
    def test_hit_before_expiry(self):
        """既有断言：TTL 内可读。"""
        clock = Clock()
        c = TTLCache(capacity=4, ttl=10, clock=clock)
        c.put("a", 1)
        clock.advance(1)
        self.assertEqual(c.get("a"), 1)

    def test_expired_after_ttl(self):
        """既有断言：超过 TTL 后读不到（单线程）。"""
        clock = Clock()
        c = TTLCache(capacity=4, ttl=1, clock=clock)
        c.put("a", 1)
        clock.advance(2)
        self.assertIsNone(c.get("a"))

    def test_lru_eviction_and_callback_outside_lock(self):
        """既有断言：超容量按 LRU 淘汰，回调在锁外调用。"""
        evicted = []
        clock = Clock()
        c = TTLCache(capacity=2, ttl=100, clock=clock, on_evict=lambda k, v: evicted.append(k))
        c.put("a", 1)
        c.put("b", 2)
        c.put("c", 3)
        self.assertEqual(evicted, ["a"])
        self.assertEqual(c.stats()["size"], 2)

    def test_stats_counts_hits_and_misses(self):
        """既有断言：命中/未命中统计。"""
        clock = Clock()
        c = TTLCache(capacity=4, ttl=10, clock=clock)
        c.put("a", 1)
        c.get("a")
        c.get("missing")
        s = c.stats()
        self.assertEqual((s["hits"], s["misses"]), (1, 1))

    def test_stale_read_within_clock_refresh_window(self):
        """新增：不用 sleep，用注入时钟稳定复现"读到过期值"。

        如何卡住旧实现：旧 cache 的 _now() 每 64 次调用才真正读一次时钟，
        其余调用返回缓存的旧时间。两次 put 共产生 2 次 _now 调用（计数 2，
        缓存值 0.0）；接着在时钟仍为 0 时对存在的键 "z" 连续 get 63 次：
        前 62 次计数落在 2..63 不刷新，第 63 次计数恰为 64 -> 重新读时钟，
        但此刻时钟还是 0，缓存依旧为 0，计数变为 65。随后把注入时钟推进到
        t=100（TTL=10，"a" 早已过期），此时 get("a") 计数为 65、不触发刷新，
        旧实现拿陈旧的 0.0 与 expires_at=10 比较，误判未过期，返回旧值 1。

        修复后 _now() 每次都实时读时钟，t=100 >= 10，get("a") 必返回 None。
        """
        clock = Clock()
        c = TTLCache(capacity=256, ttl=10, clock=clock)
        c.put("a", 1)
        c.put("z", 9)
        for _ in range(63):
            c.get("z")
        clock.advance(100)
        self.assertIsNone(c.get("a"))

    def test_get_updates_lru_order(self):
        """新增：读命中要把键移到 LRU 末尾，否则会误淘汰热键。"""
        evicted = []
        clock = Clock()
        c = TTLCache(capacity=2, ttl=100, clock=clock,
                     on_evict=lambda k, v: evicted.append(k))
        c.put("a", 1)
        c.put("b", 2)
        c.get("a")
        c.put("c", 3)
        self.assertEqual(evicted, ["b"])
        self.assertIsNone(c.get("b"))
        self.assertEqual(c.get("a"), 1)

    def test_stats_size_excludes_expired_entries(self):
        """新增：TTL 过期的条目不应再计入 size（旧实现会虚高容量）。"""
        clock = Clock()
        c = TTLCache(capacity=4, ttl=1, clock=clock)
        c.put("a", 1)
        c.put("b", 2)
        c.put("c", 3)
        clock.advance(5)
        self.assertEqual(c.stats()["size"], 0)

    def test_ttl_expiry_does_not_invoke_on_evict(self):
        """新增：TTL 失效不是容量淘汰，不触发 on_evict 回调。"""
        evicted = []
        clock = Clock()
        c = TTLCache(capacity=4, ttl=1, clock=clock,
                     on_evict=lambda k, v: evicted.append(k))
        c.put("a", 1)
        clock.advance(5)
        self.assertIsNone(c.get("a"))
        self.assertEqual(evicted, [])


if __name__ == "__main__":
    unittest.main()
