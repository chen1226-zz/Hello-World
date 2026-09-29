# ratebucket — 多键令牌桶限流器

## 根因分析

### Bug ①：并发压测下超发 5%–10%

原实现的「取桶 → 计算补充 → 扣减 → 写回」不是原子操作：补充与扣减
分散在锁外（或用了 per-key 细锁但读改写之间仍有窗口）。多个线程
同时读到同一份 `tokens`，各自扣减后写回，发生 lost update，同一批
令牌被重复消费，于是单位时间放行数超过 `burst + rate × elapsed`。

**修复**：用一把互斥锁包住整个临界区——`_get`（惰性建桶/TTL 重置）、
`_refill`（按经过时间补充）、扣减，全部在 `with self._lock` 内对
共享状态就地更新。单把全局锁足以保证正确性；令牌桶操作是纳秒级的
内存计算，锁竞争开销可忽略（见 `race_test.py`，8 线程 100 轮无超发）。

### Bug ②：系统时钟回拨后部分 key 被长期误判「额度耗尽」

原实现基于 `time.time()` 墙钟，且把「下次可用时间」或「上次补充
时间」直接按墙钟记录。时钟回拨后：

- `elapsed = now - updated` 变负：若直接累加会扣出「令牌债务」，若
  把 `updated` 更新成回拨后的读数，时钟恢复后 `elapsed` 被放大 →
  超发；
- 按墙钟记录的 `next_available` 远大于回拨后的 `now` → 该 key 在
  真实时间的数秒内一直被拒绝 → 误封。

**修复**：一律基于 `time.monotonic()` 单调时钟（不受 NTP 校时和
手动改时影响），并对时钟异常做纵深防御：

- `_elapsed` 把负的经过时间钳为 0；
- `updated` / `last_seen` 只前进不后退（回拨时保留旧时间戳），
  这样时钟恢复后补充窗口不会被放大，也不会产生债务；
- 前跳时补充量被 `burst` 上限截断，天然不会超发。

**取舍**：`time.monotonic()` 是进程内时钟，重启后归零、跨进程不可比。
但令牌桶状态本来就是进程内的瞬时状态，不需要跨进程/跨重启比较，
因此单调时钟是正确选择；其分辨率（纳秒级）也足以保证 `retry_after`
的精度。若未来需要跨进程限流，应改用外部存储 + 服务端时间，而不是
回到墙钟。

## API

```python
rb = RateBucket(rate=10.0, burst=20.0, idle_ttl=300.0)
rb.allow("user-42")            # -> bool，原子扣 1 个令牌
rb.allow("user-42", cost=5)    # 按成本扣减
rb.retry_after("user-42")      # -> 还需等待的秒数（可用时为 0.0）
rb.reclaim_idle()              # 回收闲置 >= idle_ttl 的桶，返回数量
```

- `rate`：每秒每 key 补充的令牌数；`burst`：桶容量上限。
- `idle_ttl`：key 连续未访问达到该时长后，桶被重置为满并可被回收
  （边界：`>= idle_ttl` 判定为过期）。
- 时钟可通过 `clock=` 注入，便于测试。

## 验证

```bash
python3 race_test.py                  # 100 轮并发压测，期望 OK
python3 -m unittest discover -s tests # 单元测试，期望 OK
```

测试覆盖：并发不超发（`race_test.py` + `test_concurrent_*`）、
时钟回拨（不债务、不误封、恢复后不超发）、时钟前跳（补充被 burst
截断）、idle TTL 重置与 `reclaim_idle` 的边界（恰好等于 TTL）、
`retry_after` 精度、per-key 隔离。
