# forwarder 长跑泄漏：原因分析与修复说明

## 现象

转发服务（请求转发 + 重试）长跑数小时后，线程数、打开的文件句柄（fd）与
连接数持续增长，最终 OOM 或被系统拒绝创建连接。短时压测（5 分钟）看不出来，
因为泄漏发生在**失败路径**上：失败请求占比低时，积累速度很慢。

## 泄漏点定位

泄漏集中在三条失败/异常路径上，共同特点是「资源分配后，走异常分支时没人释放」：

1. **上游超时 / 连接被重置**：`HTTPConnection` 在 `getresponse()` 抛异常后
   没有 `close()`，socket 滞留在内核（CLOSE_WAIT），fd 与连接数随失败次数
   单调增长。这是最主要的泄漏点——每次超时泄漏 1 个 fd。
2. **客户端提前断开**：处理线程向已断开的客户端写响应时抛
   `BrokenPipeError`，若异常处理不当会导致上游连接同样不被关闭；
   且非 daemon 线程 + 无超时的阻塞读写会让线程永久挂起，线程数只增不减。
3. **重试无界 / 不可取消**：重试循环没有上限、没有取消机制，失败场景下
   每个请求占用的连接与线程被无限期持有，越积越多。

## 修复思路（见 forwarder.py）

- **连接统一在 `finally` 中关闭**：`fetch_with_retry()` 每次尝试都在
  `try/finally` 里 `conn.close()`，成功、超时、重置、取消所有退出路径
  都保证释放 fd（forwarder.py:48）。
- **所有 socket 操作带超时**：杜绝线程永久阻塞在读写上。
- **daemon 线程 + 显式生命周期**：`ThreadingHTTPServer` 设
  `daemon_threads = True`，`Forwarder.stop()` 做 `shutdown()` +
  `server_close()` + `join()`，服务停止后线程归零。
- **有界且可取消的重试**：`retries` 有上限，支持 `cancel` 事件，
  取消时立即抛 `UpstreamError`，已建立的连接照常由 `finally` 关闭。
- **客户端断开静默收尾**：写响应捕获 `BrokenPipeError` /
  `ConnectionResetError`，请求直接结束，无资源残留。

## 判定阈值与依据

- **阈值**：soak 结束并停止服务后，轮询最多 10 秒等待资源回落，要求
  线程增量与 fd 增量 **<= 0**（即精确回到基线）。
- **依据**：修复后每个请求的连接都在 `finally` 关闭、处理线程随连接
  关闭而退出，稳态下不应有任何净增长；基线在服务启动前测量，因此
  理论上增量应精确为 0。轮询窗口用于消化线程退出/句柄回收的瞬时
  抖动，避免偶发误报。实测 `2000 requests, threads +0, fds +0`。

## 验证方式

```bash
# 长跑压测：2000 个请求全部走失败路径（上游超时重试 + 客户端提前断开）
python3 soak_test.py
# 期望输出：OK: 2000 requests, threads +0, fds +0

# 回归测试（< 60 秒，本机假上游，无第三方依赖）
python3 -m unittest discover -s tests
# 期望输出：OK（5 个用例）
```

回归测试覆盖的失败路径（tests/test_forwarder.py）：

| 用例 | 场景 | 断言 |
| --- | --- | --- |
| `test_success_path` | 正常转发 | 200 + 响应体正确，无泄漏 |
| `test_upstream_timeout_no_leak` | 黑洞上游，30 次超时重试 | 全部 504，线程/fd 回基线 |
| `test_client_disconnect_no_leak` | 慢上游 + 客户端发完即断开 | 服务存活，线程/fd 回基线 |
| `test_retry_cancelled` | 重试前/重试中被取消 | 立即停止，fd 不增长 |
| `test_retry_exhaustion_closes_fds` | 连接被拒，重试耗尽 | 抛 `UpstreamError`，fd 不增长 |

反向验证：把 `forwarder.py` 中 `finally: conn.close()` 去掉后重跑回归
测试，`test_upstream_timeout_no_leak` 立即报 `fds leaked: +30`，证明
测试对泄漏敏感、不是“永远通过”的假测试。

## 约束

仅使用 Python 标准库；假上游全部跑在本机回环地址，不联网；
每个源文件 < 200 行；回归测试总耗时约 7 秒，soak 约 15 秒。
