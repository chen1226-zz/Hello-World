import faulthandler
import sys
import threading
import unittest

from engine import Engine


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
        """回归测试：交叉触发 transfer/reindex，断言不存在环形等待。

        旧的实现里两条路径加锁顺序相反（account->index 与 index->account），
        在本测试的调度压力下几轮内就会互相持有对方需要的锁而卡死；
        卡住时通过 faulthandler 转储全部线程栈，可看到双方都停在锁等待上。
        """
        old_interval = sys.getswitchinterval()
        sys.setswitchinterval(1e-6)
        try:
            for _ in range(300):
                engine = Engine()
                threads = [
                    threading.Thread(target=engine.transfer, args=("x", "y", 1), daemon=True),
                    threading.Thread(target=engine.reindex, args=("k",), daemon=True),
                ]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join(timeout=2)
                stuck = [thread.name for thread in threads if thread.is_alive()]
                if stuck:
                    faulthandler.dump_traceback()
                    self.fail(f"检测到环形等待，线程阻塞在锁上: {stuck}")
        finally:
            sys.setswitchinterval(old_interval)

    def test_concurrent_mixed_ops_finish(self):
        """回归测试：多线程混合调用在限时内全部完成且结果一致。"""
        engine = Engine()
        threads = [
            threading.Thread(target=engine.transfer, args=("x", "y", 1), daemon=True)
            for _ in range(8)
        ]
        threads += [
            threading.Thread(target=engine.reindex, args=("k",), daemon=True)
            for _ in range(8)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)
        stuck = [thread.name for thread in threads if thread.is_alive()]
        if stuck:
            faulthandler.dump_traceback()
            self.fail(f"检测到环形等待，线程阻塞在锁上: {stuck}")
        snap = engine.snapshot()
        self.assertEqual(snap["accounts"], {"x": 92, "y": 108})
        self.assertEqual(snap["index"]["k"], 8)


if __name__ == "__main__":
    unittest.main()
