# fetch — 带重试的线程安全 HTTP 客户端

## 现象

`fetch.py` 在多个线程之间共享一个连接对象（keep-alive 长连接）。低并发下
正常，高并发下偶发：

- 响应字段/body 串到别的请求上（「串包」）；
- body 被截断；
- 同时打开的套接字数缓慢增长（fd 泄漏）。

## 串包机理

HTTP/1.1 的一条 keep-alive 连接是一条**严格有序的字节流**，请求与响应必须
一一配对，且每次响应都必须完整读完，才能发下一个请求。旧实现只共享连接对象，
但没有任何串行化保证，于是发生交错：

1. 线程 A 在同一 socket 上 `request()` 到一半，线程 B 也对同一 socket
   `request()`，两批请求行/请求头在字节流里交错，服务端解析出垃圾或错配。
2. 线程 A 只 `read()` 了部分响应（或拿到 headers 就返回），连接立刻被线程 B
   复用；A 剩余的响应字节（body、chunked 的后续 chunk、下一条响应的头部）被 B
   当作自己响应的开头读出 —— 这就是「字段串包、body 被截断」。
3. 异常/超时路径没有 `close()`：一个响应已经损坏的连接回到池里，后续使用者
   读到的是上一条请求的残余数据，错配被反复放大。

fd 缓慢增长的原因：未读完或出错的连接既不能安全复用又没有关闭；为了隔离
脏连接只能不断新建 socket，旧 socket 一直挂着，直到 GC/对端 RST 才回收，
因此打开句柄数单调上升、高并发才显现。

## 修复

- **连接对象不再裸共享**：每个 `(scheme, host, port)` 一个空闲连接池，连接
  从池中取出后只属于当前这一次请求，用完再放回；池操作用锁保护，且锁内只做
  出/入队，不做网络 I/O，不会串行化全部请求。
- **响应体在连接归还前一次性读净**（`response.read()`），chunked 与
  Content-Length 都一样处理，从根上杜绝「残留下一条响应」。
- **任何错误都关闭该连接**：超时、`IncompleteRead`（中途断流）、其他
  HTTP/OSError 都不把脏连接放回池，避免脏数据扩散也避免 fd 泄漏。
- **重试**：连接建立阶段的失败对所有方法重试；已发出后仅对幂等方法
  （GET/HEAD/PUT/DELETE/OPTIONS/TRACE）重试，指数退避。
- **重定向**：301/302/303 降级为 GET 去 body，307/308 保留方法与 body，
  超过 `max_redirects` 抛 `TooManyRedirects`；中间每一跳的响应体同样读净。
- **池有上限**（`max_pool_per_host`），多余连接直接关闭；`Client.close()`
  关闭全部空闲连接，支持 `with Client()`。

## 导出 API（保持不变）

- `Client(timeout=10, retries=3, max_redirects=5, max_pool_per_host=16)`
  - `client.fetch(url, method="GET", headers=None, body=None)`
  - `client.get(url)` / `client.post(url, body=...)`
  - 返回 `Response(status, headers, body)`，`body` 为 bytes，含 `ok` 属性
  - `client.close()` / 上下文管理器
- 模块级 `fetch(url, ...)`、`get(url)`、`post(url, ...)`
- 异常：`FetchError`、`TimeoutError`、`TooManyRedirects`

## 验收

```
python3 interop_stress.py
# OK: 2000 requests, 0 mismatch, fds back to baseline

python3 -m unittest discover -s tests
# OK
```

仅使用标准库，只连本机 `fake_server.py`，不联网、无第三方依赖。
