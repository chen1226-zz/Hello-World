# 10985 · sqlite 任务队列（jobstore）

`jobstore.py` 用 sqlite3 做任务队列，多个 worker 进程同时抢任务。

接口：`JobStore(path).add(titles)` / `.claim(worker)` / `.stats()`。

## 运行

```
python3 run_stress.py                  # 期望输出: OK: 200 tasks, each claimed once
python3 -m unittest discover -s tests  # 期望: OK
```

## 修复前的 bug

旧实现有两个互相独立的问题，叠加后出现"偶发 locked + 统计对不上"：

1. **领取不是原子的**：`claim()` 先 `SELECT` 一个 pending 任务，`sleep` 之后再
   `UPDATE`。两个 worker 会选中同一个 id 并各自更新成功 → 同一任务被重复领取
   （压测下 200 个任务出现 775 次领取记录）。
2. **异常被静默吞掉**：`except sqlite3.OperationalError: return None` 把
   `database is locked` 伪装成"队列空了"，worker 提前退出 → 部分任务永远没人领，
   最终 `claimed` 统计对不上；且默认 journal 模式 + 无 busy_timeout，写锁一冲突
   就立刻报错。

## 并发方案（修复后）

- **连接策略**：每个进程/线程各自 `JobStore(path)` 持有自己的连接，不跨线程共享
  （sqlite3 默认 `check_same_thread=True`）。连接是短事务、长连接复用。
- **WAL 模式**：`PRAGMA journal_mode=WAL`，读者不阻塞写者、写者不阻塞读者，
  多进程下只剩"写-写"需要排队；配 `synchronous=NORMAL`（WAL 下仍保证崩溃安全）。
- **busy_timeout=30s**：锁冲突时由 SQLite 内部等待排队，而不是立刻抛
  `OperationalError`，绝大多数竞争在这一层就被吸收。
- **事务粒度**：`claim()` 用**单条** `UPDATE ... WHERE id = (SELECT ... LIMIT 1)
  RETURNING id` 完成"选任务 + 置状态"，语句级原子，不存在 SELECT/UPDATE 之间的
  窗口，两个 worker 物理上不可能领到同一任务；`add()` 用显式
  `BEGIN IMMEDIATE / COMMIT`，失败即 `ROLLBACK`。
- **重试边界**：只对整个语句/事务、只针对 `OperationalError(locked)` 做指数退避
  重试（1ms 起步、上限 50ms、最多 100 次），兜底 WAL 下偶发的 `BUSY_SNAPSHOT`
  （重试会拿到新快照，必然能推进）。其他异常直接抛出，绝不静默返回 `None`。

### 为什么不能只靠"加重试次数"

重试只能缓解"锁冲突报错"这一个症状，治不了根因：

- 旧代码的正确性漏洞是 **SELECT 与 UPDATE 之间存在窗口**，重试多少次都不会
  改变两个 worker 选中同一行的事实——重复领取不是锁问题，是事务粒度问题；
- 无限重试会把"队列已空"和"暂时被锁"混为一谈，worker 永远退不出或过早退出；
- 不开 WAL / busy_timeout 时，重试只是让多个进程更密集地互相冲撞，锁竞争
  反而加剧。正确顺序是：先把操作做成原子（消除竞争窗口），再用 busy_timeout
  吸收正常排队，重试只作为最后兜底。

## 修复前后行为对比

| 场景 | 修复前 | 修复后 |
| --- | --- | --- |
| 4 进程抢 200 任务 | 大量重复领取（775 次领取记录），偶发 `database is locked` | 每个任务恰好被领一次，`OK: 200 tasks, each claimed once` |
| 锁冲突 | 立刻抛错并被吞成 `None`，worker 提前退出、任务丢失 | busy_timeout 排队 + 有界重试，冲突被透明吸收 |
| 崩溃（事务未提交） | 无测试覆盖 | 进程退出即回滚，任务保持 `pending` 可被重新领取 |
| 最终统计 | `claimed` 经常对不上 | `claimed == total`，无 pending 残留 |

## 测试

`tests/test_jobstore.py` 在原有 3 个用例基础上新增：

- `test_multiprocess_claims_unique`：4 进程并发抢 60 个任务，断言无重复、无丢失、
  最终统计一致；
- `test_crash_recovery`：子进程事务未提交即 `os._exit`，断言任务回滚为 pending
  且可被后续 worker 领取；
- `test_no_handle_leak`：反复打开/关闭 300 次连接，断言 `/proc/self/fd` 数量不增长。
