"""复现脚本：反复交叉触发两条代码路径，检查是否出现环形等待。"""

import sys
import threading

from engine import Engine

RUNS = 5000
STEP_TIMEOUT = 0.5


def one_run():
    engine = Engine()
    threads = [
        threading.Thread(target=engine.transfer, args=("x", "y", 1), daemon=True),
        threading.Thread(target=engine.reindex, args=("k",), daemon=True),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=STEP_TIMEOUT)
    return any(thread.is_alive() for thread in threads)


def main():
    sys.setswitchinterval(1e-6)
    for index in range(RUNS):
        if one_run():
            alive = [t.name for t in threading.enumerate() if t.name.startswith("Thread-")]
            print(f"FAIL: 第 {index + 1} 次出现死锁（阻塞线程数 {len(alive)}）")
            print("  提示：两条路径的加锁顺序相反，形成环形等待")
            raise SystemExit(1)
    print(f"OK: {RUNS} runs, no deadlock")


if __name__ == "__main__":
    main()
