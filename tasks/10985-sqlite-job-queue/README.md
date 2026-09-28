# SQLite Job Queue — 并发修复说明

`jobstore.py` 用标准库 `sqlite3` 实现多进程/多线程共享的任务队列。
本目录是修复后的版本，下面说明并发方案并对比修复前后的行为。

## 运行方式

```bash
python3 run_stress.py                 # 期望: OK: 200 tasks, each claimed once
python3 -m unittest discover -s tests # 期望: OK
```

## 修复前的问题

原实现（已随本次修复替换）的典型写法是：

- 所有 worker 共享/混用连接，默认 journal 模式（rollback journal）；
- 不设 `busy_timeout`，锁冲突立即抛 `sqlite3.OperationalError: database is locked`；
- 领取任务分两步：`SELECT` 一个 pending 任务，再 `UPDATE` 状态，
  且用默认的 deferred 事务（读锁先升级写锁）；
- 出错时事务被回滚，但调用方不重试、不记录，状态更新**静默丢失**。

并发一高就出现两类故障：

1. `database is locked`：默认 journal 模式下写者排斥所有读者，
   锁冲突没有任何等待窗口，直接报错。
2. 统计对不上：两个 worker 的 deferred 事务都先拿了读锁，
   升级写锁时互相死锁，SQLite 强制回滚其中一个；
   回滚后没有重试，任务卡在中间态或更新丢失，最终计数不一致。

## 修复后的并发方案

| 维度 | 方案 |
| --- | --- |
| 连接策略 | 每个进程/线程各自 `JobStore(path)`，一个实例一条私有连接，不跨 worker 共享连接对象 |
| 日志模式 | `PRAGMA journal_mode=WAL`：读者不阻塞写者，写者不阻塞读者 |
| 锁等待 | `PRAGMA busy_timeout=30000`（连接 `timeout` 同步设置），锁冲突时等待而非立即报错 |
| 事务粒度 | 所有事务只包一条链路的最小操作；`claim` 用 `BEGIN IMMEDIATE`  upfront 拿写锁，select→update→审计插入在同一事务内原子提交 |
| 重试边界 | `_run()` 只对 `OperationalError` 且消息含 "locked" 的瞬时错误重试，指数退避（5ms 起、上限 0.5s、最多 5 次）；其他异常直接抛出 |
| 崩溃恢复 | `requeue_expired(lease)` 把租约过期的 running 任务重置为 pending，由其他 worker 重新领取 |

关键点：`claim` 的原子性靠 `BEGIN IMMEDIATE` 事务保证——写锁在事务
开始时就拿到，两个 worker 不可能同时选中同一任务；`claims` 审计表与
状态更新同事务提交，所以"每个任务只被领一次"可以被精确验证。

## 为什么不能只靠"加重试次数"

1. **重试治不了死锁回滚**：deferred 事务的读锁升级死锁是结构性问题，
   重试只是让同样的竞态再撞一次；高并发下失败率不收敛，只是延迟变高。
2. **静默丢失不是重试能发现的**：原 bug 的核心是回滚后调用方根本不知道
   更新没生效。加重试次数不改变"哪些错误值得重试、重试的边界在哪"
   这个设计问题——非锁错误（约束冲突、磁盘满）重试一万次也是浪费。
3. **正确性应来自机制而非概率**：WAL 消除读写互斥、`busy_timeout` 给锁
   一个等待窗口、`BEGIN IMMEDIATE` 消除升级死锁，三者把"会失败的场景"
   直接删掉；重试只作为极端情况下（如 WAL 检查点竞争）的兜底安全网。

## 修复前后行为对比

| 场景 | 修复前 | 修复后 |
| --- | --- | --- |
| 8 进程抢 200 任务 | 偶发 `database is locked`，需人工介入 | 稳定输出 `OK: 200 tasks, each claimed once`（连续 5 次通过） |
| 锁冲突 | 立即抛错 | `busy_timeout` 内等待，几乎不暴露给调用方 |
| 事务回滚 | 静默丢失更新，统计对不上 | 瞬时锁错误有限重试；回滚必然伴随异常上抛 |
| 同一任务被领取 | 可能两个 worker 同时领到 | `BEGIN IMMEDIATE` 原子领取，`claims` 表可审计每任务恰好一次 |
| worker 崩溃 | 任务永远卡在 running | `requeue_expired` 按租约回收重投 |
| 长时间运行 | 连接/句柄随重连泄漏 | 一实例一连接，`close()`/上下文管理器释放，fd 数稳定（有测试覆盖） |

## 测试覆盖（tests/test_jobstore.py）

- 基本流程：入队/领取/完成、空队列、FIFO 顺序、越权完成被拒绝；
- 多进程竞争：4 进程抢 60 任务，断言每个任务恰好被领一次、最终全部 done；
- 崩溃恢复：过期 running 任务被回收重投、未过期任务不被误回收；
- 资源泄漏：100 次开关连接后 fd 数不增长、单连接 500 次操作稳定。

全部测试 1 秒内跑完（限制 60 秒），仅使用标准库。
