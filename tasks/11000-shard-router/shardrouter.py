"""把 key 路由到固定分片。

对外接口（不得更改签名）：
    normalize_key(key) -> bytes
    hash_key(key) -> int
    shard_of(key, shards, weights=None) -> str
    route_with_legacy(key, shards, legacy, weights=None) -> str
        legacy 是 {key: shard} 的既有归属表（已经落库、不能改动）
        weights 是 {shard: 权重}，机器配置不同时用它分配更多流量

`shards` 是分片名字列表，例如 ["shard-00", ..., "shard-15"]。

实现要点：
  * 哈希改用 hashlib.md5，跨进程、跨重启稳定（不再受 PYTHONHASHSEED 影响）；
  * 一致性哈希环 + 虚拟节点：扩容只迁移约 1/(n+1) 的 key，且负载均匀；
  * 权重折算成虚拟节点数，权重高的分片在环上按比例占更多点位；
  * route_with_legacy 先查历史归属表，命中即返回原分片，绝不搬家。
"""

import bisect
import hashlib
from functools import lru_cache

_VNODES_PER_WEIGHT = 200  # 每单位权重对应的虚拟节点数


def normalize_key(key):
    """把 key 统一成字节串（str/bytes/int 均为确定结果）。"""
    if isinstance(key, bytes):
        return key
    if isinstance(key, str):
        return key.encode("utf-8")
    return str(key).encode("utf-8")


def hash_key(key):
    """返回 key 的哈希值，跨进程、跨重启稳定。"""
    return int.from_bytes(hashlib.md5(normalize_key(key)).digest(), "big")


def _vnode_point(shard, index):
    """虚拟节点在哈希环上的落点，只取决于分片名和序号。"""
    seed = f"{shard}#{index}".encode("utf-8")
    return int.from_bytes(hashlib.md5(seed).digest(), "big")


@lru_cache(maxsize=None)
def _ring(shards, weight_items):
    """构建一致性哈希环：[(落点, 分片名), ...]，按落点升序。"""
    weights = dict(weight_items)
    min_weight = min(weights.get(name, 1) for name in shards)
    points = []
    for name in shards:
        weight = weights.get(name, 1)
        if weight <= 0:
            raise ValueError(f"分片 {name} 的权重必须为正数")
        vnodes = max(1, round(_VNODES_PER_WEIGHT * weight / min_weight))
        for index in range(vnodes):
            points.append((_vnode_point(name, index), name))
    points.sort()
    return points


def shard_of(key, shards, weights=None):
    """返回 key 所属的分片名。"""
    if not shards:
        raise ValueError("shards 不能为空")
    weight_items = tuple(sorted((weights or {}).items()))
    ring = _ring(tuple(shards), weight_items)
    point = hash_key(key)
    index = bisect.bisect_left(ring, (point, ""))
    return ring[index % len(ring)][1]


def route_with_legacy(key, shards, legacy, weights=None):
    """带历史归属的路由：legacy 里的 key 必须保持原有分片。"""
    if not shards:
        raise ValueError("shards 不能为空")
    if legacy and key in legacy:
        return legacy[key]
    return shard_of(key, shards, weights)
