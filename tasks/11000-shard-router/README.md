# 11000 · 分片路由（shardrouter）

`shardrouter.py` 把 key 路由到固定分片。

对外接口（签名与返回类型保持不变）：

- `normalize_key(key)` → `bytes`
- `hash_key(key)` → `int`
- `shard_of(key, shards, weights=None)` → `str`
- `route_with_legacy(key, shards, legacy, weights=None)` → `str`
  （`legacy` 是 `{key: shard}` 的既有归属表，`weights` 是 `{shard: 权重}`）

## 目录内容

| 文件 | 说明 |
| --- | --- |
| `shardrouter.py` | 路由模块（已修复） |
| `crossproc.py` | 验收脚本：跨进程一致性 + 扩容迁移量 + 负载分布 + 历史归属 + 权重 |
| `tests/test_shardrouter.py` | unittest 用例 |

## 原实现为什么跨进程不稳定

旧代码的路由是 `shards[hash(key) % len(shards)]`，有两个问题：

1. **内置 `hash()` 会被随机化。** Python 对 `str` / `bytes` 的哈希
   （SipHash）默认使用进程启动时随机生成的种子（`PYTHONHASHSEED`），
   两个进程的种子不同，同一个字符串的哈希值就不同。于是重启后同一批
   key 的 `hash(key) % n` 大面积改变（30%–40% 漂移），缓存命中率骤降；
   而同一进程内种子固定，所以「进程内反复计算永远一致」。
2. **取模路由怕扩容。** `hash % n` 里 n 从 16 变 17 时，几乎所有 key 的
   余数都会变，扩容等于全量搬家。

## 新算法：一致性哈希环（ketama 风格）

- **稳定哈希**：`hash_key` 改用 `hashlib.md5`（标准库），取摘要前 8 字节
  作为 64 位整数。md5 只依赖输入字节，跨进程、跨重启结果完全一致。
- **虚拟节点环**：每个分片在环上放 `256 × 权重` 个虚拟节点，节点位置由
  `md5("分片名#序号")` 决定；key 哈希后顺时针落到最近的虚拟节点，归属
  该节点所属的分片（`bisect` 查找，环按 `(shards, weights)` 缓存）。
- **扩容迁移量小**：新增分片只是往环上添加它自己的虚拟节点，既有分片
  的节点位置不变，只有落在新节点顺时针区间内的 key 会迁移，期望迁移
  比例 = 新增权重 / 总权重（16 → 17 等权时约 1/17 ≈ 5.9%，实测 6.4%，
  低于 1.6/16 的上限）。删分片同理，只有原本属于它的 key 需要挪窝。
- **权重如何体现**：分片的虚拟节点数 = `256 × 权重`，权重 4 的机器在环
  上占的弧长约为其他机器的 4 倍，承接的流量也约为 4 倍。`weights` 缺省
  或为空时每个分片权重按 1 处理，退化为等权。
- **负载均匀**：每单位权重 256 个虚拟节点，单分片负载的相对标准差约
  `1/√256 ≈ 6%`，最大/平均负载比实测约 1.09，远低于 1.5 的上限。

## 历史归属怎么保住

`route_with_legacy` 先查 `legacy` 表：key 命中就直接返回表里记录的原
分片（哪怕该分片已不在当前 `shards` 列表里，数据还在那里，必须能找回）；
未命中的新 key 才走一致性哈希环。因此换算法后已落库的数据一个都不会
漂移，只有新 key 按新算法分配。

## key 归一化

`normalize_key` 给 `bytes` / `str` / `int` 分别加一个字节的类型标签
（`\x00` / `\x01` / `\x02`）后再编码：`str` 用 UTF-8（非 ASCII 安全）、
`int` 用十进制 ASCII。结果只依赖输入值，不依赖进程内随机状态，且
`b"1"`、`"1"`、`1` 不会互相撞车。

## 运行

```
python3 crossproc.py
python3 -m unittest discover -s tests
```
