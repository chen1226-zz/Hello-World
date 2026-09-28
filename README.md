# 消费端幂等：重复投递、重启、并发下只生效一次

消费者从上游（at-least-once）拉取记账事件并写入 SQLite。上游保证
*至少送达一次*，因此同一 `event_id` 可能被投递多次、乱序到达，也可能在
消费者重启后被整体重放。目标：**同一事件的业务副作用有且仅有一次**。

## 复现

```bash
make test     # 全部测试（重复投递 / 双消费者并发 / kill -9 后重启）
make replay   # 同一批事件投递两遍（乱序 + 重复 event_id），比对汇总值
```

`make replay` 输出示例（20 个唯一事件，42 行投递）：

```
single pass (ground truth)   total=  2100
legacy after replay          total=  3900   # 原实现：重复记账，汇总偏大
fixed after replay           total=  2100   # 修复后：与只处理一遍完全一致
```

## 原实现为什么失效

原实现保留在 `idempotent_consumer/legacy.py`，有两个致命缺陷：

### 1. 幂等键里掺了时间戳

```python
dedup_key = event_id + "|" + received_at   # received_at = 本机收到时的当前时间
```

幂等键必须是**上游分配、在所有重复投递之间保持不变**的标识。原实现把
“本机收到的时刻”拼进键里：同一条事件每次重投的 `received_at` 都不同，
键也就每次都不同，去重表永远查不到、永远插入成功。逐条处理正常（每个键
只出现一次），整体重放必现翻倍。

正确做法：幂等键就是上游 `event_id`（若业务上同一事件可能含多个子操作，
可用上游命名空间 + 上游事件号组合，但绝不能混入消费方本地时间、投递轮次、
自增 ID 等易变成分）。

### 2. “查重 / 写去重记录 / 写业务”分散在多个事务里

原实现分别 `commit` 三次：

- **TOCTOU 竞态**：两个消费者同时 `SELECT` 都得到“没处理过”，随后各自
  插入业务行——并发消费时同一事件被扣两次；
- **崩溃窗口**：若在去重记录 `commit` 之后、业务行 `commit` 之前被
  `kill -9`，重启后事件被判定“已处理”而业务效果永久丢失（反过来则是
  业务生效了却没有去重记录，重放时再扣一次）；
- 业务表本身对 `event_id` 没有 `UNIQUE` 约束，数据库层没有任何兜底。

## 修复设计

### 幂等键与唯一约束

```sql
CREATE TABLE processed_events (
    event_id     TEXT PRIMARY KEY          -- 幂等键 = 上游 event_id
);

CREATE TABLE ledger_entries (
    id         INTEGER PRIMARY KEY,
    event_id   TEXT NOT NULL UNIQUE,       -- 业务表再兜底一层
    account    TEXT NOT NULL,
    amount     INTEGER NOT NULL,
    emitted_at TEXT NOT NULL
);
```

- `processed_events.event_id` 主键/唯一约束是**真正的去重保证**：由存储
  引擎原子裁决，应用层“先查后插”的判断只是快路径，竞态下以约束冲突
  为准；
- `ledger_entries.event_id` 再加 `UNIQUE` 作为纵深防御：业务写入自身也
  不可能重复，与去重表互不依赖。

### 单事务原子提交（`idempotent_consumer/consumer.py`）

```sql
BEGIN IMMEDIATE;
INSERT INTO processed_events(event_id) VALUES (?) ON CONFLICT DO NOTHING;
INSERT INTO ledger_entries(...);
COMMIT;
```

- 去重记录与业务写入在**同一个事务**里提交。`kill -9` / 断电时 SQLite
  保证原子性：要么两行都在，要么都不在，不会出现“去重了但没落库”或
  “落库了但没去重”；
- `BEGIN IMMEDIATE` 一开始就拿写锁，配合 `PRAGMA busy_timeout=10000`
  和 WAL，两个消费者处理同一事件时被数据库串行化：抢到键的提交，
  另一个的 `INSERT ... ON CONFLICT DO NOTHING` 影响行数为 0，回滚并按
  重复处理——副作用恰好一次；
- 连接使用 `isolation_level=None` 显式控制事务（`BEGIN`/`COMMIT`），
  避免 Python sqlite3 隐式事务把边界搞错；
- `synchronous=FULL` + WAL：已提交的数据在崩溃后不丢，未提交的事务
  随连接死亡自动回滚。

## 代码结构

- `idempotent_consumer/events.py` — 事件模型与重放流（乱序 + 重复行）
- `idempotent_consumer/db.py` — schema、连接参数、汇总查询
- `idempotent_consumer/consumer.py` — 修复后的幂等消费者（单事务）
- `idempotent_consumer/legacy.py` — 原失效实现，仅用于复现与对照
- `scripts/replay.py` — `make replay` 的双投比对脚本
- `tests/` — 四类场景测试（见下）

## 测试覆盖

| 测试 | 场景 |
| --- | --- |
| `tests/test_idempotency.py` | 单遍基线；两遍投递（含乱序、重复 `event_id`）后汇总与单遍完全一致；“跑一半重启 + 上游重发”仍幂等；纯乱序不改变汇总 |
| `tests/test_concurrency.py` | 启动**两个真实消费者进程**消费同一批事件：恰好 20 条生效、20 条判重，无约束错误，汇总等于单遍 |
| `tests/test_crash_recovery.py` | CLI 在某事件业务写入后、`COMMIT` 前 `kill -9`：该事件两行皆无，此前事件持久化；重启重放后无丢失、无重复 |
| 同文件 `LegacyRegressionTests` | 钉住原实现两个失败：重放后 42 行 / 汇总 3900；两个事务之间崩溃留下“有去重标记、无业务行” |
