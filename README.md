# poolleak — sync.Pool 复用未清零导致的跨请求数据泄漏

一个仅依赖 Go 1.22 标准库的最小复现：handler 用 `sync.Pool` 复用请求/响应
结构体，但回收时没有完整重置，导致上一个请求的用户名、金额、标签等数据被
带进下一个请求的响应。

## 快速验证

```sh
make leak   # 交替执行两个请求，连续 1000 轮断言零泄漏
make test   # 单元测试（顺序/并发/异常/取消/重置契约）
make race   # go test -race
make bench  # 分配次数基准
```

## 泄漏路径

`sync.Pool` 的 `Get` 只会复用调用方 `Put` 回去的对象，**它不做任何清零**。
缺陷版本里有三条独立的泄漏路径：

1. **回收路径遗漏（正常 vs 异常/取消不一致）**
   `releaseResponse` 只在成功分支重置个别字段；解析错误、后端报错、请求被
   取消（`ctx.Done()`）三个分支直接 `respPool.Put(resp)`，对象带着完整旧数据
   回到池中。下一个拿到它的请求直接继承上一个响应的全部内容。

2. **只重置部分字段**
   成功分支只清了 `Status/Note/Amount`，`Headers`（map）、`Tags`（slice）、
   `Payload`（[]byte）全部原样保留。同时填充逻辑是
   `resp.Tags = append(resp.Tags, req.Tags...)`——新请求的标签被**追加**到旧
   标签后面；新请求没有标签时，旧标签一个不少地出现在 `X-Tags` 头和响应体里。
   请求侧 `req.Tags` 同理。

3. **切片保留底层数组，旧数据通过"长度"重新可见**
   渲染响应体时复用了 `resp.Payload` 的底层数组，但只 `copy` 了新内容的前缀，
   返回的切片长度仍是旧（大）响应的长度：

   ```
   cap/length: 大响应 A 写入 [ user=alice...MARKER... ][ 尾部 padding ]
   小响应 B:   copy 前缀 [ user=bob&amount=7 ][ MARKER... 尾部仍然可见 ]
   ```

   `canonicalQuery` 中的 scratch 缓冲区同理：`copy` 覆盖前缀却保留旧长度，
   旧字节被拼进 `X-Canonical` 响应头。

**为什么"数据体量越大越容易出现"**：请求 A 越大，池化数组的 cap 越大，请求 B
越不可能触发扩容分配；一旦不扩容，B 就直接在 A 的底层数组上读写，旧尾部必然
外溢。GC 关闭（或高负载下对象长期驻留池内）时泄漏稳定复现；这也解释了
"单元测试全绿"——小负载下对象可能被 GC 清空、或每次刚好扩容成新数组，问题被
掩盖。`make leak` 用 `debug.SetGCPercent(-1)` + 大/小请求交替固定复现。

## 修复：复用前的重置契约

核心不变量：**任何池化对象在对下一个请求可见之前，必须完整重置。** 重置不是
把 slice 切短就完事——通过对象可达的每个字节都必须不可见。

`Request.Reset()` 必须清零的字段：

| 字段 | 处理 |
| --- | --- |
| `User` | 置 `""` |
| `Amount`、`Pad` | 置零 |
| `ctx` | 解关联（不能让旧请求的取消信号被新请求看到） |
| `Tags`、`Notes` | 截到 `[:0]`，保留底层数组 |
| `Scratch` | 截到 `[:0]` **且** 对 `[:cap]` 全部写零 |

`Response.Reset()` 必须清零的字段：

| 字段 | 处理 |
| --- | --- |
| `Status`、`Amount` | 置零 |
| `Note` | 置 `""` |
| `Tags` | 截到 `[:0]`，保留底层数组 |
| `Headers` | `delete` 全部条目，map 本身复用（稳态不重新分配桶） |
| `Payload` | 截到 `[:0]` **且** 对 `[:cap]` 全部写零 |

底层数组必须保留（这是池化省分配的意义），但 `[:0]` 之后旧字节仍然物理存在；
额外把 `[:cap]` 清零是纵深防御：即使将来有人写出"只 copy 前缀、不重新定长"的
代码，也无法再把旧数据序列化出去。

### 回收路径如何做到全覆盖

- 两个 `sync.Pool` 变量包级私有，外部无法绕过包代码直接 `Put`。
- `recycleRequest` 和 `Handler.recycleResponse` 是**唯一**调用 `Put` 的漏斗，
  且都先 `Reset` 再 `Put`。
- `Handle` 用 `defer recycleRequest(req)` 回收请求；响应用一个
  `recycleResp bool` + `defer` 守卫：解析错误、后端错误、`ctx` 取消三条失败
  分支返回时统一走同一个重置漏斗；成功时把所有权移交给调用方，调用方通过
  `Release` 进入同一个漏斗。`ServeHTTP` 用 `defer h.Release(resp)` 兜底。
- `AcquireRequest`/`AcquireResponse` 取对象时也会 `Reset` 一次：即使将来新增
  回收路径时漏写重置，泄漏也不会跨请求发生。

填充侧也改为"从空切片开始追加"（`resp.Tags`/`req.Tags` 在 Reset 后 len 为 0），
渲染改为 `append(buf[:0], 新内容...)`——返回长度严格等于新写入长度，旧尾部
不再可达。

### 所有权契约（接口保持不变）

`Handle` 返回非 nil 的 `*Response` 时，调用方必须在读过后**恰好调用一次**
`Release`；返回 error 时响应已在内部回收，调用方不得再碰。

## 性能

修复没有引入每请求深拷贝，底层数组与 map 全部继续复用：

```
BenchmarkPoolReset-16              174 ns/op      0 B/op      0 allocs/op
BenchmarkHandleAlternating-16     2709 ns/op   5711 B/op     16 allocs/op
```

`Handle` 的分配全部来自 `net/url` 查询串解析和 `strconv`/字符串拼接，缺陷版本
与修复版本一致；池的获取、重置、回收路径为 0 分配。

## 测试

- `TestSequentialNoLeak`：`make leak` 的测试内版本，1000 轮大/小请求交替，
  断言响应体与响应头均不含任何前一个请求的标记。
- `TestLargeThenSmall`：大响应（16 KB）后跟小响应，专打"底层数组旧尾部"。
- `TestErrorPathResets` / `TestBackendErrorPathResets` /
  `TestCanceledPathResets`：解析错误、后端错误、取消三条异常回收路径后，
  再取池化对象必须是干净的。
- `TestConcurrentReuseNoLeak`：32 goroutine × 100 次请求（`-race` 下运行），
  两种身份互不可见。
- `TestResetContract`：直接验证重置契约——长度为零、底层数组 cap 保留（不
  退化为新分配）、底层字节全部为零、map 条目删除、复用后旧条目不复现。
