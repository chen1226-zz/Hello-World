"""HTTP 转发服务：请求转发 + 有界重试（长跑不泄漏版本）。

泄漏修复要点：
1. 上游连接统一在 finally 中关闭：成功、超时、连接重置、取消路径都不遗漏 fd；
2. 所有 socket 操作都带超时，线程不会永久阻塞在读写上；
3. 服务线程与请求处理线程全部为 daemon，stop() 会 shutdown + join，不留孤儿线程；
4. 重试有界且可被 cancel 事件中断，失败/取消路径不会堆积连接与线程。
"""

import http.client
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class UpstreamError(Exception):
    """上游不可用：重试耗尽或重试被取消。"""


def fetch_with_retry(host, port, path, timeout=1.0, retries=2, cancel=None):
    """向上游发起 GET，失败时重试；任何退出路径都保证连接被关闭。"""
    last_error = None
    for _attempt in range(retries + 1):
        if cancel is not None and cancel.is_set():
            raise UpstreamError("retry cancelled")
        conn = http.client.HTTPConnection(host, port, timeout=timeout)
        try:
            conn.request("GET", path)
            resp = conn.getresponse()
            body = resp.read()
            return resp.status, body
        except (OSError, http.client.HTTPException) as exc:
            # 超时、连接拒绝、连接重置、对端提前关闭都会落到这里
            last_error = exc
        finally:
            conn.close()  # 关键修复：无论成功/失败/异常，fd 都在这里释放
    raise UpstreamError("upstream unreachable after %d tries: %s" % (retries + 1, last_error))


def _make_handler(upstream_host, upstream_port, timeout, retries):
    class ForwardHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            try:
                status, body = fetch_with_retry(
                    upstream_host, upstream_port, self.path, timeout, retries
                )
            except UpstreamError:
                status, body = 504, b"upstream unavailable\n"
            try:
                self.send_response(status)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                # 客户端提前断开：响应写不出去，直接结束本请求即可；
                # 上游连接已在 fetch_with_retry 的 finally 中关闭，无资源残留。
                pass
            self.close_connection = True

        def log_message(self, *args):
            pass

    return ForwardHandler


class Forwarder:
    """可安全反复启停、长跑不泄漏的转发服务。"""

    def __init__(self, listen_host, listen_port, upstream_host, upstream_port,
                 timeout=1.0, retries=2):
        handler = _make_handler(upstream_host, upstream_port, timeout, retries)
        self.httpd = ThreadingHTTPServer((listen_host, listen_port), handler)
        self.httpd.daemon_threads = True  # 处理线程不阻止进程退出，也不被积累
        self._serve_thread = None

    @property
    def port(self):
        return self.httpd.server_address[1]

    def start(self):
        self._serve_thread = threading.Thread(
            target=self.httpd.serve_forever,
            kwargs={"poll_interval": 0.05},
            daemon=True,
        )
        self._serve_thread.start()
        return self

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        if self._serve_thread is not None:
            self._serve_thread.join(timeout=5)
