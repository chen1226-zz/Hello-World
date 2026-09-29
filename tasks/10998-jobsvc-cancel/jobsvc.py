"""jobsvc.py — 可取消的后台任务服务.

修复要点: 客户端断开 / 请求取消 / 超时后, 取消信号通过 CancelToken
协作式地穿透每一层(阻塞 I/O、本地计算循环), 工作线程在 200ms 内退出,
不会留下"幽灵任务"占满 CPU.
"""
import hashlib
import threading
import time

__all__ = ["Cancelled", "CancelToken", "Job", "JobService"]

# 阻塞 I/O 的可中断等待时间片: 取消信号最迟在一个时间片内被观察到
IO_SLICE = 0.01
# 本地计算: 每个"计算单元"的工作量(纯 Python 迭代次数), 单元之间即检查点
UNIT_ITERS = 2000
# 每个计算单元附带一段 C 级耗时操作(sha256 会释放 GIL),
# 保证 200 个 CPU 密集线程并存时, 取消信号仍能被及时调度观察到
_UNIT_BUF = b"x" * 65536


class Cancelled(Exception):
    """取消信号到达检查点时抛出, 用于终止当前任务."""


class CancelToken:
    """协作式取消令牌; 子任务可关联父令牌, 父取消会向下传播."""

    def __init__(self, parent=None):
        self._event = threading.Event()
        self._parent = parent

    def cancel(self):
        self._event.set()

    def cancelled(self):
        if self._event.is_set():
            return True
        return self._parent is not None and self._parent.cancelled()

    def check(self):
        """检查点: 已取消则抛 Cancelled."""
        if self.cancelled():
            raise Cancelled()

    def wait(self, timeout):
        """可中断等待: 被取消时立即返回 False, 正常超时后返回 True."""
        deadline = time.monotonic() + timeout
        while not self.cancelled():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return True
            self._event.wait(min(remaining, IO_SLICE))
        return False


def blocking_io(token, duration):
    """模拟阻塞 I/O(读 socket / 等下游): 不用一次性不可中断的 sleep,
    改为在令牌上分段等待 —— 这是 I/O 层的检查点."""
    if not token.wait(duration):
        raise Cancelled()


def compute(token, units):
    """模拟耗时本地计算: 每个计算单元之间放置检查点;
    单元内的 sha256 为 C 级调用且释放 GIL, 避免取消信号被调度饿死."""
    acc = 0
    for i in range(units):
        token.check()  # 计算层检查点
        acc += hashlib.sha256(_UNIT_BUF).digest()[0]
        for j in range(UNIT_ITERS):  # 纯 Python 本地计算
            acc += (i * j) % 7
    return acc


class Job:
    def __init__(self, token):
        self.token = token
        self.done = threading.Event()
        self.result = None
        self.error = None
        self.thread = None

    def cancel(self):
        self.token.cancel()

    @property
    def cancelled(self):
        return isinstance(self.error, Cancelled)


class JobService:
    """接收请求并派生后台线程执行; 取消/超时后线程必须快速退出."""

    def __init__(self):
        self._lock = threading.Lock()
        self._jobs = set()

    def submit(self, units=20000, io_time=0.0, timeout=None, parent=None):
        token = CancelToken(parent)
        job = Job(token)
        job._timer = None
        if timeout is not None:
            job._timer = threading.Timer(timeout, token.cancel)
            job._timer.daemon = True
        with self._lock:
            self._jobs.add(job)
        job.thread = threading.Thread(
            target=self._run, args=(job, units, io_time), daemon=True)
        if job._timer is not None:
            job._timer.start()
        job.thread.start()
        return job

    def _run(self, job, units, io_time):
        try:
            job.token.check()  # 入口检查点: 启动即已取消则不干活
            if io_time:
                blocking_io(job.token, io_time)
            result = compute(job.token, units)
            job.token.check()  # 出口检查点: 取消优先, 已取消则丢弃结果
            job.result = result
        except Cancelled as exc:
            job.error = exc
        except Exception as exc:  # 后台异常不外抛, 记录到 job 上
            job.error = exc
        finally:
            if job._timer is not None:
                job._timer.cancel()
            job.done.set()
            with self._lock:
                self._jobs.discard(job)

    def active_count(self):
        with self._lock:
            return len(self._jobs)
