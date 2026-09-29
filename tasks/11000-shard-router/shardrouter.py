"""把 key 路由到固定分片。

对外接口（不得更改签名）：
    normalize_key(key) -> bytes
    hash_key(key) -> int
    shard_of(key, shards, weights=None) -> str
    route_with_legacy(key, shards, legacy, weights=None) -> str
        legacy 是 {key: shard} 的既有归属表（已经落库、不能改动）
        weights 是 {shard: 权重}，机器配置不同时用它分配更多流量

`shards` 是分片名字列表，例如 ["shard-00", ..., "shard-15"]。

实现说明：
- 哈希用 hashlib.md5，跨进程、跨重启稳定（内置 hash() 会被
  PYTHONHASHSEED 随机化，不能用于路由）。
- 路由用 ketama 风格的一致性哈希环：每个分片按权重放置若干虚拟节点，
  key 顺时针落到最近的虚拟节点上。增删分片只影响环上相邻区间，
  迁移量约为 变化权重 / 总权重。
"""

import bisect
import hashlib

# 每单位权重放置的虚拟节点数。节点越多负载越均匀，环构建越慢；
# 256 时单分片负载的标准差约为 1/sqrt(256) ≈ 6%，足够满足均匀性要求。
POINTS_PER_WEIGHT = 256

# {(tuple(shards), tuple(sorted(weights.items())) | None): (points, owners)}
_ring_cache = {}


def normalize_key(key):
    """把 key 统一成字节串。

    带一个字节的类型标签，避免 b"1" / "1" / 1 互相撞车；
    对 str / bytes / int 都给出确定结果，不依赖进程内随机状态。
    """
    if isinstance(key, bytes):
        return b"\x00" + key
    if isinstance(key, str):
        return b"\x01" + key.encode("utf-8")
    if isinstance(key, int):
        return b"\x02" + str(key).encode("ascii")
    return b"\x01" + str(key).encode("utf-8")


def _hash_bytes(data):
    """64 位稳定哈希（md5 前 8 字节，大端）。"""
    return int.from_bytes(hashlib.md5(data).digest()[:8], "big")


def hash_key(key):
    """返回 key 的哈希值（跨进程稳定）。"""
    return _hash_bytes(normalize_key(key))


def _point(shard, index):
    """分片第 index 个虚拟节点在环上的位置。"""
    return _hash_bytes(f"{shard}#{index}".encode("utf-8"))


def _build_ring(shards, weights):
    entries = []
    for name in shards:
        weight = weights.get(name, 1) if weights else 1
        for i in range(max(int(round(POINTS_PER_WEIGHT * weight)), 0)):
            entries.append((_point(name, i), name))
    if not entries:  # 所有权重都 <= 0 时退化为等权
        for name in shards:
            for i in range(POINTS_PER_WEIGHT):
                entries.append((_point(name, i), name))
    entries.sort(key=lambda item: item[0])
    return [p for p, _ in entries], [name for _, name in entries]


def _ring(shards, weights):
    cache_key = (
        tuple(shards),
        tuple(sorted(weights.items())) if weights else None,
    )
    ring = _ring_cache.get(cache_key)
    if ring is None:
        ring = _build_ring(shards, weights)
        _ring_cache[cache_key] = ring
    return ring


def shard_of(key, shards, weights=None):
    """返回 key 所属的分片名。"""
    if not shards:
        raise ValueError("shards 不能为空")
    points, owners = _ring(shards, weights)
    index = bisect.bisect_left(points, hash_key(key)) % len(points)
    return owners[index]


def route_with_legacy(key, shards, legacy, weights=None):
    """带历史归属的路由：legacy 里的 key 必须保持原有分片。"""
    if not shards:
        raise ValueError("shards 不能为空")
    if key in legacy:
        return legacy[key]
    return shard_of(key, shards, weights)
