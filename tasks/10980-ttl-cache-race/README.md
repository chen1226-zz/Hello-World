# 10980 · TTL + LRU 缓存（cache）

接口不变：`TTLCache(capacity, ttl, clock=None, on_evict=None)`、`get/put/stats`。

## 根因、修复与偶发原因

1. `_now()` 每 64 次调用才真正读一次时钟，其余返回缓存的旧时间戳，TTL
   比较失真，`get()` 会把早已过期的条目当未过期返回；修复为每次直接读注入时钟。
2. `get()` 命中未 `move_to_end`，LRU 只反映写入顺序，热键被误淘汰；修复为命中即移到末尾。
3. 过期条目只在被 `get()` 时删除，`put/stats` 不清退，`stats()["size"]`
   虚高；修复为写入与统计前在锁内清退全部过期项。
4. 过期失效不触发 `on_evict`；容量淘汰收集待删项后仍在锁外逐个回调，语义不变。

旧实现只有当 TTL 判断恰好落在两次时钟刷新之间、且真实时间已越过过期点时才出错；
是否命中该窗口取决于调用计数与线程交错，故低频短测试难发现，高并发下频繁出现。

## 运行

```
python3 run_race.py                         # OK: 200000 ops, 0 stale reads
python3 -m unittest discover -s tests       # OK
```
