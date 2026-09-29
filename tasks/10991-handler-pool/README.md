# handler 对象池：跨请求数据泄漏修复

## 现象

`handler.py` 用对象池复用 `Request` / `Response`。偶发把上一个请求的
用户名、金额、响应头甚至响应体带进下一个请求；**请求体越大越容易复现**，
而小数据量单测全绿。

```text
请求 A: echo  user=alice, amount=10000, body=64KiB
请求 B: ping  user="",     amount=0,      body=""
响应 B 却携带: username=alice, amount=10000, X-Echo: 1, body=A 的 64KiB
```

`leak_demo.py` 用一大一小两个请求交替 1000 轮稳定复现旧实现。

## 泄漏路径（旧实现的四个缺陷）

1. **复用前没有完整 reset。** 池只在「取对象」时按「新对象」填字段，
   小请求只会覆盖自己用到的槽位；上一轮的 `username` / `amount` /
   `headers` / `fields` 残留原样出现在响应里。
2. **只增不减的容器。** `headers` / `fields` 是池化的 dict，处理代码用
   `["k"] = v` 增键却从不删键；大请求写入的键在小请求中永远存活。
   数据量越大，填充的键/缓冲区越多，命中残留的概率越高。
3. **响应体 `bytearray` 不清空。** `extend()` 追加而非覆盖，响应体长度
   单调累积；这也是「体量越大越容易出现」的直接原因。
4. **异常 / 取消路径漏回收或先回收后使用。** 旧代码要么在出错时直接
   `return` 跳过回收（对象带着脏数据留在池里），要么在 `finally` 外提前
   归还，下一个借取者与当前写操作并发读到半重置对象。

## 修复

### 1. 明确的重置契约

复用前 `reset()` 必须让对象与 `__init__` 后完全一致：

| 对象 | 字段 | 重置动作 |
| --- | --- | --- |
| `Request` | `username` | 置 `""` |
| | `amount` | 置 `0` |
| | `headers` | `dict.clear()`（就地清空） |
| | `body` | `bytearray.clear()`（清空内容，**保留容量**） |
| `Response` | `status` | 置 `200` |
| | `error` | 置 `""` |
| | `headers` / `fields` | `dict.clear()` |
| | `body` | `bytearray.clear()`（保留容量） |

### 2. 唯一回收点，覆盖正常 / 异常 / 取消

`BorrowPool.borrow()` 是唯一取还入口，`try/finally` 保证三条路径都归还；
`reset()` 在池锁内、入空闲列表前执行，下一个借取者一定拿到干净对象：

```python
with self.pool.borrow() as (request, response):
    self._populate_request(...)
    self._dispatch(...)          # raise HandlerError / CancelledError 也安全
    return self._snapshot(response)
# finally: _put_back(Request) + _put_back(Response)，内部先 reset 再入池
```

### 3. 接口不变，分配次数不上升

- `Handler.handle(...)` 的位置/关键字参数、返回字段不变（`Result` 命名元组）。
- 池化对象本体从不外泄：返回的是出站小载荷的浅快照 `Result`，
  `bytes(response.body)` 只是把已有缓冲线性拷出（本来就要产生出站字节），
  **不是每轮深拷贝**。
- 大 `bytearray` 清空后保留容量，重复请求体不再反复向系统申请内存。

`leak_demo.py` 的自证输出：预热后 1000 轮 `+0` 个池对象分配，
稳态后 900 轮 tracemalloc 驻留增量为有界噪声（约 0.8KiB）。

## 运行

```bash
python3 leak_demo.py                  # OK: 1000 rounds, 0 leaked fields
python3 -m unittest discover -s tests # OK
```

仅用标准库（`threading`、`contextlib`、`tracemalloc`、`unittest`、
`concurrent.futures`），Python 3.14 可直接运行。
