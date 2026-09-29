"""长跑压测：反复走失败路径（上游超时重试 + 客户端提前断开），验证无泄漏。

用法：python3 soak_test.py
期望输出：OK: 2000 requests, threads +0, fds +0

判定阈值：soak 结束并停止服务后，轮询最多 10 秒等待资源回落，
要求线程增量 <= 0、fd 增量 <= 0（允许最多 +2 的瞬时抖动窗口已在轮询中消化）。
依据：修复后每个请求的连接都在 finally 中关闭、处理线程随连接关闭而退出，
稳态下不应有任何净增长；基线在服务启动前测量，因此理论上增量应精确为 0。
"""

import http.client
import os
import socket
import sys
import threading
import time

from forwarder import Forwarder

TOTAL = 2000
WORKERS = 40
DISCONNECT_EVERY = 10  # 每 10 个请求中 1 个走“客户端提前断开”路径


def fd_count():
    return len(os.listdir("/proc/self/fd"))


class BlackholeUpstream:
    """假上游：接受请求但永不响应，迫使转发端走超时 + 重试的失败路径。"""

    def __init__(self):
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(128)
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
            threading.Thread(target=self._hold, args=(conn,), daemon=True).start()

    @staticmethod
    def _hold(conn):
        try:
            conn.settimeout(10)
            while conn.recv(4096):  # 永不响应，直到转发端超时后关闭
                pass
        except OSError:
            pass
        finally:
            conn.close()


def client_get(port, errors):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    try:
        conn.request("GET", "/soak")
        resp = conn.getresponse()
        resp.read()
        if resp.status != 504:
            errors.append("unexpected status %s" % resp.status)
    except Exception as exc:  # noqa: BLE001 - 压测中记录所有客户端异常
        errors.append(repr(exc))
    finally:
        conn.close()


def client_disconnect(port):
    sock = socket.create_connection(("127.0.0.1", port), timeout=10)
    try:
        sock.sendall(b"GET /soak HTTP/1.1\r\nHost: x\r\n\r\n")
    except OSError:
        pass
    finally:
        sock.close()  # 不读响应，立即断开


def worker(port, count, errors):
    for index in range(count):
        if index % DISCONNECT_EVERY == DISCONNECT_EVERY - 1:
            client_disconnect(port)
        else:
            client_get(port, errors)


def settle(base_threads, base_fds, timeout=10.0):
    deadline = time.time() + timeout
    while True:
        dt = threading.active_count() - base_threads
        df = fd_count() - base_fds
        if (dt <= 0 and df <= 0) or time.time() >= deadline:
            return dt, df
        time.sleep(0.1)


def main():
    base_threads = threading.active_count()
    base_fds = fd_count()

    upstream = BlackholeUpstream().start()
    fwd = Forwarder("127.0.0.1", 0, "127.0.0.1", upstream.port,
                    timeout=0.1, retries=1).start()

    errors = []
    per_worker = TOTAL // WORKERS
    threads = [threading.Thread(target=worker,
                                args=(fwd.port, per_worker, errors))
               for _ in range(WORKERS)]
    started = time.time()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    elapsed = time.time() - started

    fwd.stop()
    upstream.stop()

    dt, df = settle(base_threads, base_fds)
    done = per_worker * WORKERS
    if errors:
        print("FAIL: %d requests, %d client errors: %s"
              % (done, len(errors), errors[:3]))
        sys.exit(1)
    if dt <= 0 and df <= 0:
        print("OK: %d requests, threads %+d, fds %+d (%.1fs)" % (done, dt, df, elapsed))
        sys.exit(0)
    print("LEAK: %d requests, threads %+d, fds %+d" % (done, dt, df))
    sys.exit(1)


if __name__ == "__main__":
    main()
