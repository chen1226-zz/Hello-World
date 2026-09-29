# 11000 · 分片路由（shardrouter）

`shardrouter.py` 把 key 路由到固定分片。

对外接口（签名与返回类型保持不变）：

- `normalize_key(key)` → `bytes`
- `hash_key(key)` → `int`
- `shard_of(key, shards, weights=None)` → `str`
- `route_with_legacy(key, shards, legacy, weights=None)` → `str`（`legacy` 是 `{key: shard}` 的既有归属表）

## 目录内容

| 文件 | 说明 |
| --- | --- |
| `shardrouter.py` | 已修复的模块 |
| `crossproc.py` | 复现脚本：跨进程一致性 + 扩容迁移量 + 负载分布 |
| `tests/test_shardrouter.py` | unittest 用例 |

## 原实现为什么跨进程不稳定

旧版 `hash_key` 直接用内置 `hash(key)`。Python 对 `str`/`bytes` 的哈希做了
随机化（PEP 456）：每个进程启动时按 `PYTHONHASHSEED` 生成不同的随机种子，
同一个字符串在不同进程里哈希值不同。`shard_of` 又按 `hash % len(shards)`
取模选分片，于是服务一重启，同一批 key 就有 30%–40% 被路由到别的分片；
而同一进程内种子不变，所以反复计算永远一致——这正对应观察到的现象。
取模还有一个副作用：分片数从 n 变到 n+1 时几乎所有 key 的余数都变，
扩容等于全量搬家。

## 新算法

**确定性哈希**：`hash_key` 改用 `hashlib.md5(normalize_key(key))`，只取决于
key 的字节内容，与进程、机器、重启次数无关，两个 `PYTHONHASHSEED` 不同的
进程对 10 万个 key 的路由结果逐 key 一致。

**一致性哈希环（虚拟节点）**：每个分片在环上放若干虚拟节点（落点 =
`md5("分片名#序号")`），key 顺时针落到最近的虚拟节点所属的分片。扩容新增
一个分片时，只有新节点「切入」的那几段弧上的 key 会迁移，理论迁移比例约
1/(n+1)，实测 16→17 约 6.4%，远低于 1.6/16 的上限；其余 key 原地不动。
每分片 200 个虚拟节点让各分片在环上占的弧长足够均匀，最大/平均负载约 1.16。

**权重 → 虚拟节点数**：`weights` 里权重 w 的分片获得的虚拟节点数与 w 成正比
（以最小权重为基准，每单位权重 200 个节点）。环上点位多，分到的弧长就多，
key 是均匀落点，所以流量自然按权重比例分配，实测偏离约 1.19 倍（上限 1.5）。
`weights` 缺省或为空时所有分片等权，退化为普通一致性哈希。

**历史归属保持**：`route_with_legacy` 先查 `legacy` 表，命中就直接返回表里
记录的原分片，完全绕开哈希环——已落库的 key 一个都不搬家；未命中的新 key
才走一致性哈希，整体负载仍然均匀。

## 运行

```
python3 crossproc.py
python3 -m unittest discover -s tests
```
