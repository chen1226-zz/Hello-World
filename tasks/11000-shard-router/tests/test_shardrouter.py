import json
import os
import subprocess
import sys
import unittest

import shardrouter

SHARDS = [f"shard-{i:02d}" for i in range(8)]


class TestShardRouter(unittest.TestCase):
    def test_returns_member_of_shards(self):
        """既有断言：返回值必须是给定分片之一。"""
        for i in range(200):
            self.assertIn(shardrouter.shard_of(f"k{i}", SHARDS), SHARDS)

    def test_stable_within_process(self):
        """既有断言：同一进程内同一 key 结果稳定。"""
        first = shardrouter.shard_of("user:42", SHARDS)
        for _ in range(50):
            self.assertEqual(shardrouter.shard_of("user:42", SHARDS), first)

    def test_all_shards_are_used(self):
        """既有断言：分片都被用到。"""
        used = {shardrouter.shard_of(f"user:{i}", SHARDS) for i in range(5000)}
        self.assertEqual(len(used), len(SHARDS))

    def test_empty_shards_rejected(self):
        """既有断言：空分片列表要报错。"""
        with self.assertRaises(ValueError):
            shardrouter.shard_of("k", [])

    def test_normalize_key_returns_bytes(self):
        """既有断言：normalize_key 返回字节串。"""
        self.assertIsInstance(shardrouter.normalize_key("abc"), bytes)
        self.assertIsInstance(shardrouter.normalize_key(b"abc"), bytes)


if __name__ == "__main__":
    unittest.main()


TASK_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def routes_in_subprocess(seed, keys, shards):
    """在指定 PYTHONHASHSEED 的子进程里计算路由，模拟服务重启。"""
    src = (
        "import json, sys\n"
        "import shardrouter\n"
        "keys, shards = json.loads(sys.argv[1]), json.loads(sys.argv[2])\n"
        "json.dump([shardrouter.shard_of(k, shards) for k in keys], sys.stdout)\n"
    )
    env = dict(os.environ, PYTHONHASHSEED=str(seed))
    out = subprocess.run(
        [sys.executable, "-c", src, json.dumps(keys), json.dumps(shards)],
        check=True, capture_output=True, text=True, cwd=TASK_DIR, env=env,
    )
    return json.loads(out.stdout)


class TestCrossProcess(unittest.TestCase):
    def test_routes_identical_across_hash_seeds(self):
        """不同 PYTHONHASHSEED 的进程（模拟重启）路由结果必须逐 key 一致。"""
        keys = [f"user:{i}" for i in range(3000)] + ["用户:中文", "café", "🔑emoji"]
        expected = [shardrouter.shard_of(k, SHARDS) for k in keys]
        for seed in (0, 1, 42):
            self.assertEqual(routes_in_subprocess(seed, keys, SHARDS), expected)

    def test_hash_key_stable_across_processes(self):
        """hash_key 不依赖进程内随机状态。"""
        src = (
            "import shardrouter, json, sys\n"
            "keys = [bytes.fromhex(v) if t == 'b' else v for t, v in json.loads(sys.argv[1])]\n"
            "json.dump([shardrouter.hash_key(k) for k in keys], sys.stdout)\n"
        )
        keys = [["s", "user:1"], ["s", "用户"], ["b", b"\x00\xff".hex()], ["i", 42]]
        results = []
        for seed in (0, 7):
            env = dict(os.environ, PYTHONHASHSEED=str(seed))
            out = subprocess.run(
                [sys.executable, "-c", src, json.dumps(keys)],
                check=True, capture_output=True, text=True, cwd=TASK_DIR, env=env,
            )
            results.append(json.loads(out.stdout))
        self.assertEqual(results[0], results[1])
        self.assertTrue(all(isinstance(h, int) for h in results[0]))


class TestBalanceAndWeights(unittest.TestCase):
    def test_load_is_balanced(self):
        """负载均匀：最大分片负载 / 平均负载 <= 1.5。"""
        loads = {name: 0 for name in SHARDS}
        for i in range(20000):
            loads[shardrouter.shard_of(f"user:{i}", SHARDS)] += 1
        ideal = 20000 / len(SHARDS)
        self.assertLessEqual(max(loads.values()) / ideal, 1.5)

    def test_weights_proportional(self):
        """权重高的分片按比例承担更多流量，偏差不超过 1.5 倍。"""
        weights = {name: 1 for name in SHARDS}
        weights[SHARDS[0]] = 4
        loads = {name: 0 for name in SHARDS}
        for i in range(20000):
            loads[shardrouter.shard_of(f"user:{i}", SHARDS, weights)] += 1
        total_weight = sum(weights.values())
        for name in SHARDS:
            expected = 20000 * weights[name] / total_weight
            ratio = loads[name] / expected
            self.assertLessEqual(max(ratio, 1 / ratio), 1.5)
        self.assertGreater(loads[SHARDS[0]], 2 * loads[SHARDS[1]])

    def test_empty_and_none_weights_are_equal(self):
        """weights 为空或缺省时退化为等权，结果一致。"""
        equal = {name: 1 for name in SHARDS}
        for i in range(500):
            key = f"k{i}"
            self.assertEqual(shardrouter.shard_of(key, SHARDS),
                             shardrouter.shard_of(key, SHARDS, {}))
            self.assertEqual(shardrouter.shard_of(key, SHARDS),
                             shardrouter.shard_of(key, SHARDS, equal))


class TestMigration(unittest.TestCase):
    def moved_ratio(self, before, after, count=20000):
        moved = sum(
            1 for i in range(count)
            if shardrouter.shard_of(f"user:{i}", before)
            != shardrouter.shard_of(f"user:{i}", after)
        )
        return moved / count

    def test_add_shard_minimal_movement(self):
        """扩容加一个分片，迁移量 <= 1.6/n。"""
        ratio = self.moved_ratio(SHARDS, SHARDS + ["shard-08"])
        self.assertLessEqual(ratio, 1.6 / len(SHARDS))

    def test_remove_shard_minimal_movement(self):
        """缩容减一个分片，迁移量 <= 1.6/n。"""
        ratio = self.moved_ratio(SHARDS + ["shard-08"], SHARDS)
        self.assertLessEqual(ratio, 1.6 / len(SHARDS))


class TestLegacy(unittest.TestCase):
    def test_legacy_keys_keep_original_shard(self):
        """历史归属表里的 key 必须全部保持原分片。"""
        legacy = {f"user:{i}": SHARDS[i % len(SHARDS)] for i in range(0, 5000, 3)}
        for key, shard in legacy.items():
            self.assertEqual(
                shardrouter.route_with_legacy(key, SHARDS, legacy), shard)

    def test_legacy_load_stays_balanced(self):
        """带历史归属时整体负载仍然均匀。"""
        legacy = {f"user:{i}": SHARDS[i % len(SHARDS)] for i in range(0, 20000, 5)}
        loads = {name: 0 for name in SHARDS}
        for i in range(20000):
            key = f"user:{i}"
            loads[shardrouter.route_with_legacy(key, SHARDS, legacy)] += 1
        ideal = 20000 / len(SHARDS)
        self.assertLessEqual(max(loads.values()) / ideal, 1.5)


class TestNormalizeKey(unittest.TestCase):
    def test_deterministic_for_supported_types(self):
        """str/bytes/int 归一化结果确定。"""
        self.assertEqual(shardrouter.normalize_key("abc"), b"abc")
        self.assertEqual(shardrouter.normalize_key(b"abc"), b"abc")
        self.assertEqual(shardrouter.normalize_key(42), b"42")
        self.assertEqual(shardrouter.normalize_key("用户:①"), "用户:①".encode("utf-8"))
        self.assertEqual(shardrouter.normalize_key("café"), "café".encode("utf-8"))

    def test_non_ascii_keys_route_consistently(self):
        """非 ASCII key 路由稳定且落在给定分片内。"""
        for key in ["用户:42", "café", "🔑", "ключ", "キー"]:
            first = shardrouter.shard_of(key, SHARDS)
            self.assertIn(first, SHARDS)
            for _ in range(10):
                self.assertEqual(shardrouter.shard_of(key, SHARDS), first)

    def test_int_keys_route(self):
        """int 类型 key 可以直接路由。"""
        self.assertIn(shardrouter.shard_of(123, SHARDS), SHARDS)
        self.assertEqual(shardrouter.hash_key(123), shardrouter.hash_key(123))
