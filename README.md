# event-time-stream-engine-go

一个可在本地运行的流处理引擎原型：用本地 JSONL 文件模拟数据源与写出存储，
实现**事件时间窗口聚合、水位线与迟到处理、恰好一次输出、检查点恢复、背压与并发安全**，
使乱序、迟到与故障恢复之后的结果保持一致且可解释。

## 运行

```bash
# 一次性跑完（自动生成含乱序/重复/迟到的输入）
go run ./cmd/streamengine -dir demo-data -events 2000

# 模拟中途崩溃并从检查点恢复
go run ./cmd/streamengine -dir demo-data -events 2000 -crash

# 测试（含竞态检测）
go test ./... -v
go test -race ./internal/engine/
```

工作目录下的文件：

| 文件 | 作用 |
|---|---|
| `input.jsonl` | 输入事件（每行一个 JSON：`id/key/event_time/value`） |
| `results.jsonl` | 已提交的窗口结果（每条带 `checkpoint_id`） |
| `late.jsonl` | 被旁路的迟到事件（带原因与当时水位线） |
| `checkpoint.json` | 检查点（位点 + 水位 + 窗口状态 + 去重集合，含 CRC 校验） |

## 时间与水位语义

- **事件时间**：窗口归属只由事件自带的 `event_time`（毫秒）决定，
  与处理时刻无关；乱序到达不会改变窗口归属。
- **窗口**：固定滚动窗口 `[start, start+size)`，按 `size` 对齐（负时间戳亦正确）。
- **水位线**：每个分片（分区）记录已见最大事件时间，
  全局水位线 = `min(各分区最大事件时间) - 允许迟到时长`，只增不减。
  任何分区尚未产生数据时，水位线为最小值（不提交任何窗口）。
- **窗口提交**：当水位线越过窗口右端点，窗口在下一个检查点完成并写出，
  写出后不再修改。输入耗尽时所有分区水位推到正无穷，剩余窗口在最终检查点全部提交。
- **迟到判定**：`event_time <= 当前水位线` 的事件为迟到。
  迟到事件**不静默丢弃、不回改已提交窗口**，而是带原因
  （`event_time X <= watermark Y`）旁路到 `late.jsonl`。

## 一致性保证

### 恰好一次输出

- 检查点覆盖：源读取位点（字节偏移）、各分区水位、窗口累计状态、去重 ID 集合。
- 提交流程：**先写输出（带检查点 ID），再保存检查点**。
  崩溃发生在两者之间时，恢复端用 `TruncateTo` 回滚未确认检查点的输出，
  因此已提交结果不重复、未提交结果不丢失，恢复后各窗口累计值与全局统计自洽。
- 恢复流程：加载检查点 → 恢复窗口/水位/去重集合 → 回滚未确认输出 → 源定位到位点。
- 检查点文件损坏（CRC 不匹配）或版本不符时，恢复**明确失败**
  （`ErrCheckpointCorrupt` / `ErrCheckpointVersion`），绝不从中间静默继续。

### 去重

- 按事件 `id` 去重（分片存储，按 ID 哈希路由，与 key 无关），
  重复事件只被计入一次；去重集合随检查点持久化，恢复后仍生效。

### 背压与资源上限

- 生产者与处理协程之间是**有界队列**（`QueueCapacity`）：
  消费跟不上时生产者阻塞，待处理数据不会无界堆积。
- 每次阻塞计入 `BackpressureBlocks` 并打印日志，可观测。
- 取消时若事件尚未入队，读取位点回退，保证不丢事件。

### 并发与状态安全

- 窗口状态按 `key` 哈希分片，同一 key（因而同一窗口）只由持有该分片锁的协程更新。
- 检查点采用屏障：暂停生产者 → 等待在途事件清空 → 按固定顺序锁全部分片做快照，
  无竞态、无半更新、无窗口错位（`go test -race` 验证）。

## 本地验证方法

```bash
# 单元 + 集成测试：乱序、重复、迟到、崩溃重启、检查点损坏/版本不符、背压、并发一致性
go test ./internal/engine/ -v          # 日志含事件、窗口判定与恢复依据
go test -race -count=3 ./internal/engine/

# 端到端演示：观察 stats 与全局累计
go run ./cmd/streamengine -dir /tmp/demo -events 2000 -crash
```

关键测试：

| 测试 | 覆盖 |
|---|---|
| `TestOutOfOrderDuplicatesLate` | 乱序归属、重复去重、迟到旁路（对照独立模型） |
| `TestLateEventDoesNotModifyCommittedWindow` | 已提交窗口不被迟到事件回改 |
| `TestCrashRecovery` | 中途取消模拟崩溃，恢复后与无崩溃基线逐窗口一致 |
| `TestSinkTruncateRollback` | 未确认检查点的输出被回滚 |
| `TestCheckpointCorrupt` / `TestCheckpointVersionMismatch` / `TestRecoverFromCorruptCheckpointFails` | 检查点失效可区分、不静默继续 |
| `TestBackpressureObservable` | 队列容量 1 时背压可观测且结果仍正确 |
| `TestConcurrentShardsConsistency` | 8 分片 8 协程下无重复输出、计数自洽（`-race`） |

## 已知限制（本地模拟范畴）

- 检查点与输出文件需同时可用（若检查点文件整体丢失，只能全新开始并清空输出）。
- 去重集合随事件数线性增长（真实系统可用布隆过滤器/状态 TTL）。
- 单源单文件顺序读取；多源需要为每个源维护独立位点。
