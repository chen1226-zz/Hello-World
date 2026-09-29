# walstore 崩溃恢复修复

append-only KV 存储：写入先追加 WAL，再周期性刷成段文件（segment），启动时重放 WAL 恢复索引。

## 根因

修复前的实现有三个缺陷，正常退出从不触发、SIGKILL 后概率性出现：

1. **fsync 顺序错误**：`put()` 先 `fsync(wal)` 再 `write(record)` 就返回成功。
   fsync 落盘的是*上一条*记录，当前这条只躺在页缓存里；SIGKILL 后最后
   若干条已确认写入丢失（丢多少取决于内核回写时机，所以是"偶尔"）。
2. **记录无校验**：WAL 记录没有 CRC。进程在写一条记录的*中途*被杀时，
   文件里留下半条记录；恢复时不校验直接读，于是 key 正确、value 被截断。
3. **段/WAL 切换顺序错误**：先截断 WAL 再写段文件（且段文件非原子替换、
   目录未 fsync）。在两次刷段之间崩溃，段文件没持久、WAL 又已被清空，
   整段数据丢失；恢复时 WAL 与段区间重叠还会把旧值重放覆盖新值。

## 持久化顺序（修复后）

### 写入路径

```
put(key, value)
  │
  ├─ 1. encode: [magic|crc32|seq|klen|vlen|key|value]   crc 覆盖 seq+len+payload
  ├─ 2. os.write(wal_fd, record)          先进入页缓存
  ├─ 3. os.fdatasync(wal_fd)              ★ 记录落盘后才允许返回
  ├─ 4. index[key] = value                更新内存索引
  └─ 5. return  ← 只有走到这里，调用方才收到"成功"

每 flush_every 次 put 触发刷段：
  ├─ 1. 写 segment.tmp（全量快照 + max_seq + 整体 crc32）
  ├─ 2. os.fsync(tmp)                     段内容落盘
  ├─ 3. os.replace(tmp, segment.dat)      原子替换
  ├─ 4. os.fsync(dir)                     让 rename 本身落盘
  ├─ 5. 删除旧 wal.log + os.fsync(dir)    ★ 段已 durable 之后才允许清 WAL
  └─ 6. 新建空 wal.log
```

### 恢复路径（幂等，恢复途中再崩溃也安全）

```
启动 _recover()
  │
  ├─ 1. 删除残留的 segment.tmp（上次刷段在 rename 前被杀）
  ├─ 2. 读 segment.dat：校验 magic + 整体 crc32，载入 kv，记下 max_seq
  ├─ 3. 顺序扫描 wal.log，逐条校验：
  │       magic 错 / 头部不完整 / payload 截断 / crc 不匹配
  │       → 立即停止，该位置之后视为"撕裂尾部"
  │       seq ≤ max_seq 的记录 → 跳过（段/WAL 区间重叠，段里的更新）
  │       其余记录 → 应用进索引
  ├─ 4. ftruncate(wal, 最后一个好记录的末尾) + fsync
  │       清掉撕裂尾部，保证后续追加不会跟在垃圾后面
  └─ 5. 以 O_APPEND 打开 WAL，继续服务
```

关键不变式：**任何一条向调用方确认成功的写，必须先完成 WAL 的
`write + fdatasync`**；**WAL 只有在替代它的段文件完全 durable
（内容 fsync + 原子 rename + 目录 fsync）之后才允许被清除**。
恢复只做"读 + 截断尾部"两类操作，重复执行结果相同，因此恢复过程中
再次崩溃不会扩大损失。

## 吞吐对比（bench.py，本机实测，2000 次写 × 5 次取中位数）

| 实现 | 吞吐 | 相对修复前 |
|---|---|---|
| buggy（修复前：先 fsync 后写） | 964 ops/s | 基准 |
| **fixed（本修复：写后 fdatasync WAL）** | **992 ops/s** | **+2.9%** |
| brute（每次写 fsync 所有文件+目录） | 871 ops/s | −9.7% |

修复只把 fsync 挪到正确位置并加上 CRC，每次写仍只 fsync WAL 一个文件，
吞吐不降反微升（远优于 −20% 红线）；而"每次写全量 fsync 所有文件"的
粗暴方式反而更慢。复现：`python3 bench.py`

## 验证

```
python3 crash_test.py              # OK: 500 rounds, 0 lost, 0 truncated
python3 -m unittest discover -s tests   # 6 个用例全部 OK（<1s）
```

测试覆盖：写入过程中崩溃（`test_crash_during_write`）、WAL 尾部残记录
三种形态（`test_torn_wal_tail`：半头部/半 payload/CRC 错）、段与 WAL
区间重叠（`test_segment_wal_overlap`）、恢复过程中再次崩溃
（`test_crash_during_recovery`）。
