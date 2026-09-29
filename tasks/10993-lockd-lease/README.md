# lockd：带租约的分布式锁

## 根因

旧实现只在 `acquire` 时检查租约，拿到锁后就「一直以为自己持有」。
当业务慢请求、进程被挂起（SIGSTOP/GC 停顿）或网络抖动超过租约时长后：

1. 服务端判定租约到期，把锁交给另一个 worker（这本身是对的）；
2. 被挂起的 worker 恢复后**无从得知**锁已易主，继续执行临界区并提交写入；
3. 两个 worker 同时处于临界区 → 重复扣减 / 重复写入。

本质：租约只能保证「任意时刻最多一个**有效**持有者」，无法保证
「持有者随时知道自己还有效」——客户端的时钟和停顿都不可信。
单机单测不重现，是因为需要真实的停顿/分区才能触发。

## 修复：续期 + fencing token（两者都要，缺一不可）

- **后台续期**（`LeaseLock._renew_loop`）：每 `ttl/3` 续一次，覆盖
  「业务耗时 > 租约」的正常慢请求（如注入的 500ms 业务 vs 200ms 租约）。
  只有 fencing 没有续期，健康但慢的请求会频繁丢锁。
- **fencing token**（`LockServer.acquire` 返回单调递增令牌）：进程被暂停时
  续期线程同样被冻结，续期救不了它；此时只有下游资源在写入前用
  `LockServer.valid` 校验令牌才能挡住过期持有者的脏写。
  只有续期没有 fencing，被暂停的进程恢复后必然造成并发进入。
- **服务端是唯一事实源**，只用单调时钟（`time.monotonic`）：客户端时钟
  漂移无法伪造或延长租约；网络分区期间续期失败，客户端立即置 `lost`，
  分区恢复后旧令牌无法「复活」，必须重新 `acquire` 拿到新令牌。

## 正确用法

```python
lock = LeaseLock(server, "resource-1", owner="worker-7", ttl=0.2)
if lock.acquire(timeout=5):
    try:
        ...                     # 业务处理
        lock.check()            # 副作用前必须确认租约仍在
        store.write(..., fencing_token=lock.token)  # 下游写入必须带令牌
    finally:
        lock.release()
```

规则：

1. 任何对外可见的副作用之前调用 `check()`（或轮询 `lock.lost` 提前放弃）；
2. 下游存储必须校验 fencing token，拒绝过期令牌的写（兜底，即使客户端
   不守规矩也安全）；
3. 租约时长按「网络 RTT + 可容忍停顿」预算设置，**不要**靠调大租约掩盖
   停顿问题——租约再长也救不了被无限期挂起的进程，只有 fencing 能兜底。

## 运行

```bash
python3 chaos.py                    # 默认 60 组：500ms 业务延迟 + 200ms 租约
python3 chaos.py --runs 200         # 200 组压力（或 CHAOS_RUNS=200）
python3 -m unittest discover -s tests
```

`chaos.py` 每组让两个 worker 竞争同一任务，交替注入「进程冻结 >
租约」故障；统计「持有有效租约的并发进入次数」（期望 0）与
「每任务恰好提交一次」。测试套件内含 200 组零并发压力用例
（`ChaosStressTest`），以及租约到期边界、续期失败、持锁进程被
kill 后恢复、网络分区恢复等用例。
