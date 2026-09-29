"""用 sqlite3 实现的任务队列，多进程 worker 并发领取。

对外接口（不得更改签名）：
    JobStore(path)
        .add(titles)
        .claim(worker) -> job_id | None
        .stats()  -> {"pending": int, "claimed": int, "total": int}

并发方案（详见 README.md）：
  * 每个进程/线程各自创建自己的 JobStore（即自己的连接），不跨线程共享连接；
  * 连接开启 WAL 模式 + busy_timeout=30s，读写不互斥、写者排队等待而非立刻报错；
  * claim 用单条 UPDATE ... RETURNING 原子完成"选任务 + 置状态"，
    不存在 SELECT 与 UPDATE 之间的窗口，两个 worker 不可能领到同一任务；
  * 仅在 sqlite3.OperationalError(locked) 时按指数退避重试整个语句/事务，
    其他异常直接抛出，绝不静默吞掉。
"""

import sqlite3
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
  id     INTEGER PRIMARY KEY AUTOINCREMENT,
  title  TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  worker TEXT
);
"""

_BUSY_TIMEOUT_MS = 30_000
_MAX_RETRIES = 100


def _is_lock_error(exc):
    return "locked" in str(exc).lower()


class JobStore:
    def __init__(self, path):
        # isolation_level=None：自动提交，事务边界全部显式控制。
        self.conn = sqlite3.connect(
            path, timeout=_BUSY_TIMEOUT_MS / 1000, isolation_level=None
        )
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=%d" % _BUSY_TIMEOUT_MS)
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._retry(lambda: self.conn.execute(SCHEMA))

    def add(self, titles):
        def op():
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                self.conn.executemany(
                    "INSERT INTO jobs (title) VALUES (?)", [(t,) for t in titles]
                )
            except BaseException:
                self.conn.execute("ROLLBACK")
                raise
            self.conn.execute("COMMIT")

        self._retry(op)

    def claim(self, worker):
        # 单条语句原子完成：子查询选出最靠前的 pending 任务并立即置为 claimed。
        # 语句级原子性保证两个并发 worker 不会拿到同一个 id。
        def op():
            row = self.conn.execute(
                """
                UPDATE jobs SET status = 'claimed', worker = ?
                WHERE id = (
                    SELECT id FROM jobs WHERE status = 'pending'
                    ORDER BY id LIMIT 1
                )
                RETURNING id
                """,
                (worker,),
            ).fetchone()
            return row[0] if row is not None else None

        return self._retry(op)

    def stats(self):
        counts = {"pending": 0, "claimed": 0, "total": 0}
        for status, n in self.conn.execute(
            "SELECT status, COUNT(*) FROM jobs GROUP BY status"
        ):
            counts[status] = n
            counts["total"] += n
        return counts

    def close(self):
        self.conn.close()

    def _retry(self, op):
        # 重试边界：整个语句/事务整体重试，且只针对锁冲突。
        # busy_timeout 已经挡住了绝大多数竞争，这里兜底 WAL 下偶发的
        # BUSY_SNAPSHOT 类错误（重试会拿到新快照，必然能推进）。
        delay = 0.001
        for _ in range(_MAX_RETRIES):
            try:
                return op()
            except sqlite3.OperationalError as exc:
                if not _is_lock_error(exc):
                    raise
                time.sleep(delay)
                delay = min(delay * 2, 0.05)
        raise sqlite3.OperationalError("database still locked after retries")
