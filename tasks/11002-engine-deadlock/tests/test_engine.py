import faulthandler
import sys
import threading
import unittest

from engine import Engine


def _cross_threads(engine, rounds=50, timeout=5.0):
    """并发交叉触发 transfer 与 reindex，返回仍阻塞的线程列表。"""
    stuck = []
    for _ in range(rounds):
        threads = [
            threading.Thread(target=engine.transfer, args=("x", "y", 1), daemon=True),
            threading.Thread(target=engine.reindex, args=("k",), daemon=True),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=timeout)
        stuck = [thread for thread in threads if thread.is_alive()]
        if stuck:
            break
    return stuck


class TestEngine(unittest.TestCase):
    def test_transfer_moves_money(self):
        """既有断言：转账后余额正确。"""
        engine = Engine()
        engine.transfer("x", "y", 30)
        self.assertEqual(engine.snapshot()["accounts"], {"x": 70, "y": 130})

    def test_reindex_updates_index(self):
        """既有断言：重建索引会写入 key。"""
        engine = Engine()
        engine.reindex("k")
        self.assertEqual(engine.snapshot()["index"]["k"], 1)

    def test_snapshot_shape(self):
        """既有断言：snapshot 返回账户与索引两部分。"""
        snap = Engine().snapshot()
        self.assertEqual(sorted(snap), ["accounts", "index"])

    def test_no_circular_wait_under_contention(self):
        """回归：两条路径并发交叉时不允许出现环形等待。

        原实现中 transfer 按 account->index、reindex 按 index->account
        加锁，本用例在 60 秒内必然复现死锁；修复后两条路径统一为
        account->index，不存在环形等待。
        """
        sys.setswitchinterval(1e-6)
        stuck = _cross_threads(Engine())
        if stuck:
            faulthandler.dump_traceback()
            self.fail(
                f"检测到环形等待：{len(stuck)} 个线程阻塞在锁上，"
                "两条代码路径的加锁顺序相反（死锁）"
            )

    def test_concurrent_results_consistent(self):
        """并发执行后账户总额守恒、索引计数正确。"""
        engine = Engine()
        rounds = 200
        threads = [
            threading.Thread(
                target=lambda: [engine.transfer("x", "y", 1) for _ in range(rounds)],
                daemon=True,
            ),
            threading.Thread(
                target=lambda: [engine.reindex("k") for _ in range(rounds)],
                daemon=True,
            ),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        self.assertFalse(any(thread.is_alive() for thread in threads))
        snap = engine.snapshot()
        self.assertEqual(sum(snap["accounts"].values()), 200)
        self.assertEqual(snap["index"]["x->y"], rounds)
        self.assertEqual(snap["index"]["k"], rounds)


if __name__ == "__main__":
    unittest.main()
