# 11002 · 锁顺序不一致导致死锁（engine）

接口：`Engine()` 的 `.transfer(src, dst, amount)` / `.reindex(key)` / `.snapshot()`。
内部有两把锁：账户锁（`account_lock`）与索引锁（`index_lock`）。

| 文件 | 说明 |
| --- | --- |
| `engine.py` | 已修复的模块（统一加锁顺序） |
| `deadlock.py` | 复现脚本：反复交叉触发两条路径 |
| `tests/test_engine.py` | unittest 用例（含环形等待回归测试） |

## 根因

两条代码路径以**相反顺序**嵌套获取同两把锁，构成环形等待：

- `transfer`：`account_lock` → `index_lock`
- `reindex`（修复前）：`index_lock` → `account_lock`  ← 违反统一锁序

高并发下线程 A 持有 `account_lock` 等待 `index_lock`，线程 B 持有
`index_lock` 等待 `account_lock`，双方互等、永不释放，且都不抛异常——
表现为整个服务卡死，`faulthandler` 可见全部线程停在锁等待上。

## 锁层级（全库统一约定）

```
层级  锁            保护的数据
L1    account_lock  self.accounts
L2    index_lock    self.index
```

锁序图（只允许沿箭头方向在持锁状态下再取锁）：

```
                ┌──────────────────────────────┐
                │           Engine             │
                │                              │
  需要两把锁时  │  account_lock ──▶ index_lock │
   的唯一顺序   │     (L1)            (L2)     │
                │                              │
                │  index_lock ──✖──▶ account_lock   禁止！
                └──────────────────────────────┘
```

禁止事项：

- 禁止在持有 `index_lock` 时再获取 `account_lock`（反向嵌套）。
- 禁止新增任何违反 L1 → L2 层级的嵌套取锁路径；新锁只能追加到层级末尾。
- 禁止用「一把全局锁」替换这两把锁来“修复”——会打没并发度。

## 修复方式

`reindex` 改为与 `transfer` 相同的顺序：先 `account_lock`，后 `index_lock`
（见 `engine.py`）。两条路径现在沿同一方向嵌套，等待图中不存在环，
死锁四必要条件中的「环形等待」被消除，故不会死锁。

并发度说明：仍保留两把独立的锁，未引入全局锁；单路径压测
（8 线程 × 2000 次）修复前后耗时分别为 transfer 5.34s→5.26s、
reindex 5.49s→5.42s，吞吐变化在 ±20% 以内（实测约 -1.4%，属噪声）。

## 运行

```
python3 deadlock.py                    # 期望：OK: 5000 runs, no deadlock
python3 -m unittest discover -s tests  # 期望：OK
```

回归测试 `test_no_circular_wait_under_contention` 在调度压力下交叉触发两条
路径，若有线程在限时内未结束则用 `faulthandler` 转储全部线程栈并判定失败；
在修复前的实现上约 4 秒内即可复现死锁（远低于 60 秒上限）。
