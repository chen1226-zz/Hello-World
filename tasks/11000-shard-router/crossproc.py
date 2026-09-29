"""复现脚本：
  1. 两个独立进程（PYTHONHASHSEED 不同）分别计算 10 万个 key 的路由，逐 key 比对；
  2. 增加一个分片后，统计有多少 key 换了分片；
  3. 统计 16 个分片的负载是否均匀。
"""

import json
import os
import random
import subprocess
import sys
import tempfile

import shardrouter

KEYS = 100_000
SHARDS = [f"shard-{i:02d}" for i in range(16)]
OUT = os.path.join(tempfile.mkdtemp(), "routes.json")
PREFIX = "user:"
LEGACY_STEP = 5          # 每 5 个 key 里有 1 个已有历史归属
WEIGHTS = {name: 1 for name in SHARDS}
WEIGHTS["shard-00"] = 4  # 这台机器配置更好，承担 4 倍流量


def check_weights():
    """加权分片：实际负载必须与权重成比例。"""
    loads = {name: 0 for name in SHARDS}
    for i in range(KEYS):
        loads[shardrouter.shard_of(f"{PREFIX}{i}", SHARDS, WEIGHTS)] += 1
    total_weight = sum(WEIGHTS.values())
    worst = 1.0
    for name in SHARDS:
        expected = KEYS * WEIGHTS[name] / total_weight
        ratio = loads[name] / expected
        worst = max(worst, ratio, 1.0 / ratio)
    return worst


def build_legacy():
    rnd = random.Random(20260928)
    legacy = {}
    for i in range(0, KEYS, LEGACY_STEP):
        legacy[f"{PREFIX}{i}"] = SHARDS[rnd.randrange(len(SHARDS))]
    return legacy


def check_legacy():
    """历史归属必须保持；整体负载仍要均匀。"""
    legacy = build_legacy()
    moved = 0
    loads = {name: 0 for name in SHARDS}
    for key, shard in legacy.items():
        loads[shard] += 1
        if shardrouter.route_with_legacy(key, SHARDS, legacy) != shard:
            moved += 1
    for i in range(KEYS):
        key = f"{PREFIX}{i}"
        if key in legacy:
            continue
        loads[shardrouter.route_with_legacy(key, SHARDS, legacy)] += 1
    ideal = KEYS / len(SHARDS)
    max_ratio = max(loads.values()) / ideal
    return moved, max_ratio


def child_source():
    return (
        "import json, sys\n"
        "import shardrouter\n"
        "keys = [f'user:{i}' for i in range(int(sys.argv[1]))]\n"
        "shards = json.loads(sys.argv[2])\n"
        "routes = [shardrouter.shard_of(k, shards) for k in keys]\n"
        "json.dump(routes, open(sys.argv[3], 'w'))\n"
    )


def run_child(seed, path):
    env = dict(os.environ)
    env["PYTHONHASHSEED"] = str(seed)
    subprocess.run(
        [sys.executable, "-c", child_source(), str(KEYS), json.dumps(SHARDS), path],
        check=True,
        env=env,
        cwd=os.path.dirname(os.path.abspath(__file__)),
    )
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def main():
    a_path = os.path.join(os.path.dirname(OUT), "routes-a.json")
    b_path = os.path.join(os.path.dirname(OUT), "routes-b.json")
    routes_a = run_child(0, a_path)
    routes_b = run_child(1, b_path)

    mismatch = sum(1 for x, y in zip(routes_a, routes_b) if x != y)
    if mismatch:
        print(f"FAIL: {KEYS} keys, {mismatch} mismatch")
        print(f"  例：key=user:7 -> 进程A={routes_a[7]} 进程B={routes_b[7]}")
        raise SystemExit(1)

    # 增加一个分片，统计迁移比例
    wider = SHARDS + ["shard-16"]
    moved = 0
    for i in range(KEYS):
        if shardrouter.shard_of(f"{PREFIX}{i}", SHARDS) != shardrouter.shard_of(f"{PREFIX}{i}", wider):
            moved += 1
    moved_ratio = moved / KEYS

    loads = {name: 0 for name in SHARDS}
    for i in range(KEYS):
        loads[shardrouter.shard_of(f"{PREFIX}{i}", SHARDS)] += 1
    ideal = KEYS / len(SHARDS)
    max_ratio = max(loads.values()) / ideal

    problems = []
    if moved_ratio > 1.6 / len(SHARDS):
        problems.append(f"加一个分片迁移了 {moved_ratio:.1%}（上限 {1.6 / len(SHARDS):.1%}）")
    if max_ratio > 1.5:
        problems.append(f"分片负载不均，最大/平均 = {max_ratio:.2f}")

    legacy_moved, legacy_ratio = check_legacy()
    if legacy_moved:
        problems.append(f"{legacy_moved} 个已有归属的 key 被改了分片")
    if legacy_ratio > 1.5:
        problems.append(f"带历史归属时负载不均，最大/平均 = {legacy_ratio:.2f}")

    weight_ratio = check_weights()
    if weight_ratio > 1.5:
        problems.append(f"加权分片没按权重分配，偏离 {weight_ratio:.2f} 倍")

    if problems:
        print(f"FAIL: {KEYS} keys, 0 mismatch, 但迁移/分布不达标")
        for item in problems:
            print("  - " + item)
        raise SystemExit(1)

    print(
        f"OK: {KEYS} keys, 0 mismatch, moved {moved_ratio:.1%} "
        f"(<= {1.6 / len(SHARDS):.1%}), max/avg {max_ratio:.2f}, legacy kept, legacy max/avg {legacy_ratio:.2f}"
        f", weighted deviation {weight_ratio:.2f}"
    )


if __name__ == "__main__":
    main()
