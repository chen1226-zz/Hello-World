# hotcache 缓存击穿修复

## 根因：为什么单实例锁不够

部署形态是 N 个服务实例（独立进程）共用一份 sqlite 缓存。整点批量过期时：

1. **每个实例各刷各的**：进程内的 `threading.Lock` 只能挡住本进程的并发，挡不住其他进程。
   同一个 key 过期后，N 个实例同时回源，下游 QPS 被放大 N 倍（实测 20 倍）。
2. **没有降级**：缓存已过期、源站又抖动时，异常直接抛给调用方，整批请求失败。
3. **盲写覆盖**：回源结果无条件 `INSERT OR REPLACE`，慢实例的旧结果会把别的实例刚写入的新值覆盖掉。

## 做法

### 跨进程 single-flight（租约）

- 发现 key 过期/缺失时，用 `BEGIN IMMEDIATE` 开启写事务（sqlite 级联互斥，跨进程生效），
  在事务内复查状态并写入 `refreshing_until = now + lease`，抢到租约的实例才回源。
- 没抢到的实例**不自己回源**，而是轮询等待持有者写回：读到新值直接返回；
  持有者失败释放租约后，有旧值则降级返回旧值，完全没有已知值才把异常抛给调用方。
- 租约有时限（10s），持有者崩溃后租约自然过期，其他实例可以接管，不会死锁。

### 版本号 CAS 写回

- 抢锁时记下当前 `version`，回源完成后用
  `UPDATE ... SET value=?, version=version+1, ... WHERE key=? AND version=?` 写回。
- 如果期间有别的实例写入了更新的值（`version` 已变），`WHERE` 不匹配、影响行数为 0，
  本次结果被丢弃，**绝不覆盖**新值。调用方仍拿到自己回源的结果，但库里保留更新的值。

### SWR（stale-while-revalidate）

- TTL 到期但仍在 `stale_ttl` 窗口内：**立刻返回旧值**，不阻塞在回源上，
  同时后台线程补一次刷新。后台刷新同样走租约，所以全库也只触发一次回源。
- 取舍：窗口内的请求会读到稍旧的数据（用一致性换延迟和可用性）；
  超过 `stale_ttl` 后不再返回旧值，转为同步刷新，避免无限期提供过期数据。

### 失败降级与 TTL 抖动

- 回源失败且该 key 有已知值：返回旧值并计入 `stats()["stale_served"]`；
  没有任何已知值的 key 才把异常抛给调用方。
- 写回时 `expires_at = now + ttl + uniform(0, jitter)`，打散过期时间，避免整点同时失效。

### 其他

- `db_path=None` 时退化为进程内缓存（dict + 线程锁），接口与返回形状不变。
- `stats()` 至少含 `refreshes` / `hits` / `stale_served` / `keys`；
  共享模式下计数存在 `hotcache_stats` 表里，是跨进程全库累计值。

## 验收

```bash
python3 stampede_test.py              # 4 进程并发击穿演练
python3 -m unittest discover -s tests # 单元测试
```
