# Ingest Gateway（有界队列版）

接收 HTTP 请求 → 入内存队列 → 固定 worker 池转发给上游。仅使用 Go 1.22 标准库。

## 对外 API（保持不变）

- `POST /ingest`：入队成功返回 `202 Accepted`；超载返回 `429 Too Many Requests`（带 `Retry-After: 1`）；body 超过 1 MiB 返回 `400`。
- `GET /metrics`：返回 JSON 指标（见下）。
- 转发语义：异步 fire-and-forget；上游失败计入丢弃指标，不回传给客户端。

## 根因分析

突发流量下内存单调上涨直到 OOM、P99 恶化，是三个初始缺陷叠加的结果：

1. **无界 channel/队列**。到达速率超过上游排空速率时，请求在队列中无限堆积。每个积压请求持有 body 与相关对象，堆内存随积压量线性增长直到 OOM；同时队列里最老的请求要等很久才被处理，排队时间被计入 P99，延迟随积压深度恶化。
2. **worker 池无上限**。每来一个请求（或每个积压任务）就新建 goroutine 去打上游，突发时同时存在数千个 goroutine，每协程栈（KB 级）+ 每个 `http.Client` 调用的缓冲进一步放大内存，并把慢上游压得更慢，形成正反馈。
3. **对慢上游缺少超时**。上游变慢后请求永久挂在 worker 上，worker 永不释放，排空速率降为 0，队列只进不出。

低流量时到达率 ≤ 排空率，队列深度接近 0，三个缺陷都不触发，因此"一切正常"。

## 修复方案

- **有界队列**：`make(chan job, QUEUE_CAPACITY)`，默认 512。内存上限 ≈ `队列容量 × (请求 body + 对象开销)`，与到达速率解耦。
- **固定 worker 池**：默认 16 个长期存活的 worker，上游并发被硬上限约束，慢上游不会被 goroutine 洪峰二次压垮。
- **每请求超时**：`UPSTREAM_TIMEOUT`（默认 2s），超时/上游错误释放 worker 并计入丢弃，保证排空速率有下界。
- **入队 body 大小限制**：1 MiB，防止单请求放大内存。

## 超载策略：快速失败（429 reject）

队列满时用非阻塞 `select` 入队，失败立即返回 `429 Too Many Requests` 并带 `Retry-After`。

在三种候选策略中选择快速失败的理由：

- **拒绝（fail-fast，本方案）**：延迟有上界（不入队就不排队），内存由队列容量硬约束，客户端可凭 429 做退避重试或削峰。代价是突发时损失一部分请求，需要客户端有重试/降级。
- 无限等待排队：等价于把压力从内存转移到延迟，违背 P99 目标，且客户端会先超时重放，雪上加霜。
- 静默丢弃旧请求（drop-oldest）：不向客户端暴露失败，调用方误以为成功，对数据类 ingest 不安全；显式 429 保留了"谁失败了"的信息，丢弃仅发生在"已接受但上游最终失败"这一无法同步通知的路径上，并计入 `dropped_total`。

## 指标（GET /metrics）

```json
{
  "queue_length": 0,
  "accepted_total": 5248,
  "rejected_total": 5600,
  "dropped_total": 0,
  "forwarded_total": 5245
}
```

- `queue_length`：当前排队长度（有界，[0, QUEUE_CAPACITY]）。
- `rejected_total`：因队列满快速失败（429）的数量——超载信号，应用于报警/扩容。
- `dropped_total`：已接受但转发失败（上游超时/连接错误/5xx）的数量——数据损失信号，应配合重试或死信。

## 运行

```bash
UPSTREAM_URL=http://upstream.example/ingest \
LISTEN_ADDR=:8080 \
QUEUE_CAPACITY=512 WORKERS=16 UPSTREAM_TIMEOUT=2s \
go run ./cmd/ingest
```

## 验证

```bash
make test    # 单元测试（突发/慢消费者/慢上游超时/恢复/指标/超大 body）
make race    # 竞态检测
make burst   # 5s 内 10x 突发，输出堆峰值倍数与突发期 P99，未达标则非零退出
```

`make burst` 在同进程内运行 mock 上游 + 真实 ingest：基线 200 rps × 2s → 突发 2000 rps × 5s（10 倍）→ 恢复 200 rps × 2s。一次实测：

```
burst x10: total=9998 2xx=4398 429=5600 P99=1.5ms
baseline heap=2.9MiB peak heap=5.1MiB ratio=1.74x  PASS (<2.0x)
burst P99=1.5ms PASS (<200ms); post-recovery queue_len=0 PASS
upstream peak in-flight=16（等于 worker 数，无协程爆炸）
```

## 取舍与后续可选项

- 容量参数是"延迟 × 突发吸收"的权衡：队列越大可吸收越深的突发，但最坏排队时间越长。当前默认值使最坏排队时间 ≈ `队列/排空率`，在 20ms 上游延迟下约 640ms。
- 单机内存换来了背压，但拒绝压力会前移到客户端；生产上通常在前面再叠一层令牌桶/配额，或把 429 率接入自动扩容。
- 当前是内存队列、异步语义，进程崩溃会丢失已接受但未转发的请求。如需 at-least-once，可把队列换成持久化日志/WAL（接口不变）。
- worker 数与队列容量目前为静态配置，未做自适应；过载时优先保证存活而非吞吐。
