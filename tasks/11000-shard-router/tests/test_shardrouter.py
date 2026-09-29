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


# ---------------------------------------------------------------------------
# 以下为修复后新增的用例（只新增，不改动上面的既有断言）
# ---------------------------------------------------------------------------

import os
import subprocess
import sys

SHARDS16 = [f"shard-{i:02d}" for i in range(16)]
MODULE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class TestCrossProcessConsistency(unittest.TestCase):
    def test_same_routes_across_hash_seeds(self):
        """不同 PYTHONHASHSEED 的独立进程，路由结果必须逐 key 一致。"""
        code = (
            "import shardrouter\n"
            "shards = [f'shard-{i:02d}' for i in range(16)]\n"
            "keys = [f'user:{i}' for i in range(2000)] + ['用户:①', 'café']\n"
            "print(','.join(shardrouter.shard_of(k, shards) for k in keys))\n"
        )
        results = []
        for seed in ("0", "1", "20260929"):
            env = dict(os.environ)
            env["PYTHONHASHSEED"] = seed
            out = subprocess.run(
                [sys.executable, "-c", code],
                check=True, capture_output=True, text=True,
                cwd=MODULE_DIR, env=env,
            )
            results.append(out.stdout)
        self.assertEqual(results[0], results[1])
        self.assertEqual(results[0], results[2])


class TestRestartRegression(unittest.TestCase):
    """钉死一批 key 的哈希值与归属，防止算法被改动后历史数据漂移。"""

    def test_pinned_hash_values(self):
        self.assertEqual(shardrouter.hash_key("user:42"), 4550442195970673963)
        self.assertEqual(shardrouter.hash_key("用户:①"), 13132167698905349177)
        self.assertIsInstance(shardrouter.hash_key("user:42"), int)

    def test_pinned_routes(self):
        expected = {
            "user:42": "shard-02",
            "user:7": "shard-14",
            "hello": "shard-11",
            "用户:①": "shard-03",
        }
        for key, shard in expected.items():
            self.assertEqual(shardrouter.shard_of(key, SHARDS16), shard)


class TestLoadBalance(unittest.TestCase):
    def test_uniform_distribution(self):
        """等权时最大分片负载 / 平均负载 <= 1.5。"""
        total = 20000
        loads = {name: 0 for name in SHARDS16}
        for i in range(total):
            loads[shardrouter.shard_of(f"user:{i}", SHARDS16)] += 1
        ideal = total / len(SHARDS16)
        self.assertLessEqual(max(loads.values()) / ideal, 1.5)
        self.assertGreater(min(loads.values()), 0)

    def test_weighted_distribution(self):
        """加权时每个分片的实际负载与权重占比的偏差 <= 1.5 倍。"""
        weights = {name: 1 for name in SHARDS16}
        weights["shard-00"] = 4
        total = 20000
        loads = {name: 0 for name in SHARDS16}
        for i in range(total):
            loads[shardrouter.shard_of(f"user:{i}", SHARDS16, weights)] += 1
        total_weight = sum(weights.values())
        for name in SHARDS16:
            expected = total * weights[name] / total_weight
            ratio = loads[name] / expected
            self.assertLessEqual(max(ratio, 1.0 / ratio), 1.5, name)

    def test_empty_weights_falls_back_to_equal(self):
        """weights 为空字典时与缺省（等权）结果一致。"""
        for i in range(200):
            key = f"k{i}"
            self.assertEqual(
                shardrouter.shard_of(key, SHARDS16, {}),
                shardrouter.shard_of(key, SHARDS16),
            )


class TestMinimalMigration(unittest.TestCase):
    def test_add_shard_moves_few_keys(self):
        """16 -> 17 个分片，迁移量 <= 1.6/16。"""
        wider = SHARDS16 + ["shard-16"]
        total = 20000
        moved = sum(
            1 for i in range(total)
            if shardrouter.shard_of(f"user:{i}", SHARDS16)
            != shardrouter.shard_of(f"user:{i}", wider)
        )
        self.assertLessEqual(moved / total, 1.6 / 16)

    def test_remove_shard_moves_few_keys(self):
        """16 -> 15 个分片，迁移量 <= 1.6/16。"""
        narrower = SHARDS16[:-1]
        total = 20000
        moved = sum(
            1 for i in range(total)
            if shardrouter.shard_of(f"user:{i}", SHARDS16)
            != shardrouter.shard_of(f"user:{i}", narrower)
        )
        self.assertLessEqual(moved / total, 1.6 / 16)


class TestLegacyRouting(unittest.TestCase):
    def test_legacy_keys_keep_original_shard(self):
        """legacy 表里的 key 一个都不能改。"""
        legacy = {f"user:{i}": SHARDS16[i % 16] for i in range(0, 5000, 2)}
        for key, shard in legacy.items():
            self.assertEqual(
                shardrouter.route_with_legacy(key, SHARDS16, legacy), shard
            )

    def test_legacy_shard_not_in_list_is_still_returned(self):
        """历史分片即使已下线，也要原样返回（数据还在那里）。"""
        legacy = {"user:1": "shard-retired"}
        self.assertEqual(
            shardrouter.route_with_legacy("user:1", SHARDS16, legacy),
            "shard-retired",
        )

    def test_non_legacy_keys_still_balanced(self):
        """带历史归属时整体负载仍然均匀（最大/平均 <= 1.5）。"""
        legacy = {f"user:{i}": SHARDS16[i % 16] for i in range(0, 4000, 5)}
        total = 20000
        loads = {name: 0 for name in SHARDS16}
        for i in range(total):
            key = f"user:{i}"
            loads[shardrouter.route_with_legacy(key, SHARDS16, legacy)] += 1
        ideal = total / len(SHARDS16)
        self.assertLessEqual(max(loads.values()) / ideal, 1.5)


class TestNormalizeKeyTypes(unittest.TestCase):
    def test_str_bytes_int_are_deterministic(self):
        """同类型同值的 key 归一化结果确定，且都是 bytes。"""
        for key in ("abc", b"abc", 12345, "用户:①", "café", 0, -7, 10 ** 30):
            self.assertIsInstance(shardrouter.normalize_key(key), bytes)
            self.assertEqual(
                shardrouter.normalize_key(key), shardrouter.normalize_key(key)
            )

    def test_different_types_do_not_collide(self):
        """str / bytes / int 即使字面相同也不能归一化成同一段字节。"""
        self.assertNotEqual(
            shardrouter.normalize_key(1), shardrouter.normalize_key("1")
        )
        self.assertNotEqual(
            shardrouter.normalize_key(b"1"), shardrouter.normalize_key("1")
        )

    def test_non_ascii_keys_route_deterministically(self):
        """非 ASCII key 的路由结果稳定。"""
        first = shardrouter.shard_of("用户:①", SHARDS16)
        for _ in range(20):
            self.assertEqual(shardrouter.shard_of("用户:①", SHARDS16), first)
