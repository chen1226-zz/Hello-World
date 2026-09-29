"""复现脚本：注入推移的时钟，统计"读到已过期条目"的次数。"""

from cache import TTLCache

OPS = 200_000
TTL = 0.5
STEP = 0.6          # 每次循环推进的时间：一定大于 TTL，条目在 get 时已过期
CAPACITY = 64
KEYS = 64


class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t

    def advance(self, seconds):
        self.t += seconds


def main():
    clock = Clock()
    written_at = {}
    stale = 0
    c = TTLCache(capacity=CAPACITY, ttl=TTL, clock=clock)
    c.put(0, 0)
    written_at[0] = clock()

    for i in range(OPS):
        key = i % KEYS
        if key not in written_at or clock() >= written_at[key] + TTL:
            c.put(key, i)
            written_at[key] = clock()
        clock.advance(STEP)
        value = c.get(key)
        if value is not None and clock() >= written_at[key] + TTL:
            stale += 1

    if stale:
        print(f"FAIL: {OPS} ops, {stale} stale reads")
        raise SystemExit(1)
    print(f"OK: {OPS} ops, 0 stale reads")


if __name__ == "__main__":
    main()
