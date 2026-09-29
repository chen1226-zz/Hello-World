# 11002 · 锁顺序不一致导致死锁（engine）

接口：`Engine()` 的 `.transfer(src, dst, amount)` / `.reindex(key)` / `.snapshot()`。
内部有两把锁：账户锁与索引锁。

| 文件 | 说明 |
| --- | --- |
| `engine.py` | 已修复的模块 |
| `deadlock.py` | 复现脚本：反复交叉触发两条路径 |
| `tests/test_engine.py` | unittest 用例（含死锁回归测试） |

## 已知现象

高并发下偶发整个服务卡死——所有线程都阻塞、只能重启进程；没有异常抛出，用
`faulthandler` 能看到全部停在锁等待上。

## 根因

两条代码路径加锁顺序相反，构成环形等待：

- `transfer`：`account_lock` → `index_lock`
- `reindex`（修复前）：`index_lock` → `account_lock`  ← 违规点

线程 A 持有 `account_lock` 等 `index_lock`，线程 B 持有 `index_lock` 等
`account_lock`，互相等待、永不释放。

## 锁层级（全模块统一约定）

```
锁序图（只允许沿箭头方向加锁）：

    account_lock (L1) ──► index_lock (L2)

    transfer:  L1 → L2   ✔
    reindex:   L1 → L2   ✔（修复前为 L2 → L1，与 transfer 构成环）
```

规则与禁止事项：

- 任何需要同时持有两把锁的路径，必须先 `account_lock` 后 `index_lock`。
- 禁止在持有 `index_lock` 时申请 `account_lock`（反向获取）。
- 禁止在持锁期间调用可能再取锁的外部回调；新增锁必须先声明其层级。
- 修复未引入全局单锁：两条路径仍各自持锁完成真实工作，并发度不变。

## 运行

```
python3 deadlock.py                       # 期望：OK: 5000 runs, no deadlock
python3 -m unittest discover -s tests     # 期望：OK
```
