"""失败路径回归测试：用本机假上游验证转发服务不泄漏线程与文件句柄。

全部用例合计运行时间远小于 60 秒，可在 CI 中直接执行：
    python3 -m unittest discover -s tests
"""

import http.client
import os
import socket
import sys
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from forwarder import Forwarder, UpstreamError, fetch_with_retry


def fd_count():
    return len(os.listdir("/proc/self/fd"))


def free_port():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


class FakeUpstream:
    """本机假上游。mode: 'ok' 立即响应；'slow' 延迟响应；'blackhole' 永不响应。"""

    def __init__(self, mode="ok", delay=0.3):
        self.mode = mode
        self.delay = delay
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(64)
        self.port = self.sock.getsockname()[1]
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)

    def start(self):
        self._thread.start()
        return self

    def stop(self):
        self._stop.set()
        self.sock.close()
        self._thread.join(timeout=2)

    def _accept_loop(self):
        self.sock.settimeout(0.05)
        while not self._stop.is_set():
            try:
                conn, _ = self.sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self._serve, args=(conn,), daemon=True).start()

    def _serve(self, conn):
        try:
            conn.settimeout(5)
            data = b""
            while b"\r\n\r\n" not in data:
                chunk = conn.recv(4096)
                if not chunk:
                    return
                data += chunk
            if self.mode == "blackhole":
                while conn.recv(4096):  # 永不响应，直到转发端超时关闭
                    pass
                return
            if self.mode == "slow":
                time.sleep(self.delay)
            body = b"hello from upstream"
            conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: %d\r\n"
                         b"Connection: close\r\n\r\n%s" % (len(body), body))
        except OSError:
            pass
        finally:
            conn.close()


class LeakTestCase(unittest.TestCase):
    def setUp(self):
        self.base_threads = threading.active_count()
        self.base_fds = fd_count()

    def settled_deltas(self, slack=0, timeout=5.0):
        """轮询等待线程/句柄回落到基线附近，返回最终增量。"""
        deadline = time.time() + timeout
        while True:
            dt = threading.active_count() - self.base_threads
            df = fd_count() - self.base_fds
            if (dt <= slack and df <= slack) or time.time() >= deadline:
                return dt, df
            time.sleep(0.05)

    def assert_no_leak(self, slack=0):
        dt, df = self.settled_deltas(slack=slack)
        self.assertLessEqual(dt, slack, "threads leaked: %+d" % dt)
        self.assertLessEqual(df, slack, "fds leaked: %+d" % df)


class TestForwarder(LeakTestCase):
    def test_success_path(self):
        upstream = FakeUpstream("ok").start()
        fwd = Forwarder("127.0.0.1", 0, "127.0.0.1", upstream.port).start()
        conn = http.client.HTTPConnection("127.0.0.1", fwd.port, timeout=5)
        conn.request("GET", "/hello")
        resp = conn.getresponse()
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.read(), b"hello from upstream")
        conn.close()
        fwd.stop()
        upstream.stop()
        self.assert_no_leak()

    def test_upstream_timeout_no_leak(self):
        upstream = FakeUpstream("blackhole").start()
        fwd = Forwarder("127.0.0.1", 0, "127.0.0.1", upstream.port,
                        timeout=0.1, retries=1).start()
        for _ in range(30):
            conn = http.client.HTTPConnection("127.0.0.1", fwd.port, timeout=5)
            conn.request("GET", "/will-time-out")
            resp = conn.getresponse()
            self.assertEqual(resp.status, 504)
            resp.read()
            conn.close()
        fwd.stop()
        upstream.stop()
        self.assert_no_leak()

    def test_client_disconnect_no_leak(self):
        upstream = FakeUpstream("slow", delay=0.3).start()
        fwd = Forwarder("127.0.0.1", 0, "127.0.0.1", upstream.port,
                        timeout=1.0, retries=0).start()
        for _ in range(20):
            sock = socket.create_connection(("127.0.0.1", fwd.port), timeout=5)
            sock.sendall(b"GET /early HTTP/1.1\r\nHost: x\r\n\r\n")
            sock.close()  # 不读响应立即断开
        deadline = time.time() + 5  # 等服务端把断开的请求处理完
        while threading.active_count() > self.base_threads + 2 and time.time() < deadline:
            time.sleep(0.05)
        fwd.stop()
        upstream.stop()
        self.assert_no_leak()

    def test_retry_cancelled(self):
        # 取消标记在调用前已设置：一次都不应尝试
        cancel = threading.Event()
        cancel.set()
        with self.assertRaises(UpstreamError):
            fetch_with_retry("127.0.0.1", free_port(), "/x", timeout=0.05,
                             retries=3, cancel=cancel)
        # 重试进行到一半被取消：应立即停止而不是跑完全部重试
        upstream = FakeUpstream("blackhole").start()
        cancel2 = threading.Event()
        threading.Timer(0.35, cancel2.set).start()
        started = time.time()
        with self.assertRaises(UpstreamError):
            fetch_with_retry("127.0.0.1", upstream.port, "/x", timeout=0.2,
                             retries=20, cancel=cancel2)
        self.assertLess(time.time() - started, 2.0)
        upstream.stop()
        self.assert_no_leak()

    def test_retry_exhaustion_closes_fds(self):
        port = free_port()  # 无人监听的端口，连接立即被拒绝
        with self.assertRaises(UpstreamError):
            fetch_with_retry("127.0.0.1", port, "/x", timeout=0.05, retries=3)
        self.assert_no_leak()


if __name__ == "__main__":
    unittest.main()
