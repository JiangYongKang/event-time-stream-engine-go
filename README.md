# event-time-stream-engine-go

一个可在本地运行的流处理引擎原型：用本地 JSONL 文件模拟数据源与写出存储，
实现**事件时间窗口聚合、水位线与迟到处理、恰好一次输出、检查点恢复、背压与并发安全**，
使乱序、迟到与故障恢复之后的结果保持一致且可解释。

本版在此基础上补强了**写出阶段与恢复阶段的容错**：

- 写出失败（磁盘写失败、写到一半进程被杀）后，未确认的窗口结果与迟到记录**不丢、不重**，
  支持同代重试与重启重放，重跑结果与“全程一次成功”逐窗口一致；
- 检查点**保留最近多代**，可指定从任意历史代数恢复；**升级前的 v1 检查点仍可读可续跑**；
  “读不了 / 文件损坏 / 版本不兼容”三类错误明确区分，绝不从中间硬接；
- 检查点代数、待重试写出积压、待处理队列都有**明确上限与触发策略**，并带可见计数。

## 运行

```bash
# 一次性跑完（自动生成含乱序/重复/迟到的输入）
go run ./cmd/streamengine -dir demo-data -events 2000

# 模拟中途崩溃并从检查点恢复
go run ./cmd/streamengine -dir demo-data -events 2000 -crash

# 写出阶段前 N 次提交注入失败（演练同代重试/重放）
go run ./cmd/streamengine -dir /tmp/demo-fail -events 500 -sink-fail 3

# 保留最近 2 代检查点；从第 1 代恢复（比它新的代数会被裁剪）
go run ./cmd/streamengine -dir /tmp/demo-rid -events 400 -cp-generations 3
go run ./cmd/streamengine -dir /tmp/demo-rid -events 400 -restore-id 1

# 测试（含竞态检测）
go test ./... -v
go test -race -count=3 ./internal/engine/
```

命令行开关：

| 开关 | 含义 |
|---|---|
| `-cp-generations N` | 保留的检查点代数上限（默认 3），更旧的代数在每次保存后裁剪 |
| `-restore-id G` | 从第 G 代检查点恢复（默认 0 = 最新一代）；代数不存在时明确报错 |
| `-sink-fail N` | 写出阶段前 N 次提交注入错误，用于演练重试/重放与日志可观测性 |

工作目录下的文件：

| 文件 | 作用 |
|---|---|
| `input.jsonl` | 输入事件（每行一个 JSON：`id/key/event_time/value`） |
| `results.jsonl` | 已提交的窗口结果（每条带 `checkpoint_id`） |
| `late.jsonl` | 被旁路的迟到事件（带原因与当时水位线） |
| `checkpoint.json.d/cp-<20位ID>.json` | 检查点代数目录（位点 + 水位 + 窗口状态 + 去重集合，含 CRC 校验） |
| `checkpoint.json` | 仅在升级前存在：v1 单文件检查点，首次 v2 写出后自动归档删除 |

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

### 恰好一次输出与写出失败恢复

- 检查点覆盖：源读取位点（字节偏移）、各分区水位、窗口累计状态、去重 ID 集合。
- 提交流程：**先写输出（带检查点 ID），再保存检查点**；检查点 ID 只有在两步都成功后才前进。
- **按代幂等提交**：sink 的 `Commit(cpID, …)` 先读回已有记录，剔除所有 `cpID` 及更新代的残留
  （包括上一次失败/被杀留下的完整行与写了一半的**残缺尾行**），再把本批整体 `tmp+fsync+rename`
  原子重写。因此同一代无论重试多少次，都不会出现重复行。
- **写失败重试**：提交在检查点屏障内按退避重试（`MaxCommitRetries` / `CommitRetryBackoff`）。
  每次重试在日志中打印 `SINK RETRY cp=… try=… replaying N window result(s), M late record(s)`，
  并计入 `SinkRetries` / `SinkReplayResults` / `SinkReplayLates`。
- **批次保留不丢**：重试在当次检查点内仍失败时，这批结果/迟到记录退回待写出队列，
  下一个检查点用**同一个 cpID** 重放；窗口状态与位点没有前进，所以重放内容确定一致。
- **进程在 commit 后、checkpoint 落盘前被杀**：该批输出属于未确认代，重启时
  `TruncateTo(cpID)` 回滚（连同残缺尾行），再从检查点位点重放，结果不重不漏。
- 恢复流程：选择检查点代数 → 恢复窗口/水位/去重集合 → 回滚未确认输出 → 源定位到位点。

### 多代检查点与按代恢复

- 每代检查点写入不可变文件 `checkpoint.json.d/cp-<ID>.json`（tmp+fsync+rename），
  默认保留最近 3 代（`MaxCheckpoints` / `-cp-generations`），更旧的在保存后裁剪。
- 默认从**最新代数**恢复；也可用 `RestoreCheckpointID` / `-restore-id` 指定从历史代数恢复。
  指定旧代时，比它更新的代数在恢复时**立即裁剪删除**（避免之后误用），随后从该代位点重新计算，
  最终结果与从头不中断一致（不重复计数、不遗漏）。
- 恢复日志明确给出依据：`RECOVER using … generation=G resume_offset=… watermark=… open_windows=…`，
  并逐窗口打印恢复时的 `count/sum`，同时记录被裁剪的更新代数数量。
- 指定的代数不存在时返回 `checkpoint not found`，**不会静默回退到最新代数**。

### 版本兼容与错误区分

- 当前检查点版本为 **v2**（仅在 v1 上新增可选字段），可读版本下限为 **v1**：
  **升级前老版本写出的 `checkpoint.json` 单文件仍可直接恢复并接着跑**，
  第一次成功写出 v2 后旧单文件自动归档删除（日志可见）。
- 读取失败严格区分三类，绝不从中间硬接：
  - 文件不存在 → `ErrCheckpointNotFound`（首次启动正常）；
  - 权限/IO 等“读不了” → 返回带路径的原始 OS 错误（**不**归类为损坏）；
  - CRC 不匹配、JSON 无法解析 → `ErrCheckpointCorrupt`；
  - 版本低于 v1 或高于 v2 → `ErrCheckpointVersion`。
- 最新代数损坏时直接失败，不会静默改用更旧代数“凑着跑”。

### 去重

- 按事件 `id` 去重（分片存储，按 ID 哈希路由，与 key 无关），
  重复事件只被计入一次；去重集合随检查点持久化，恢复后仍生效。

### 背压与资源上限

三类随时间增长的资源都有明确上限、触发策略与可见标记（`Stats` 字段 + 日志）：

| 资源 | 上限配置 | 触发策略 | 可观测标记 |
|---|---|---|---|
| 待处理事件队列 | `QueueCapacity` | 队列满时**阻塞生产者**（背压），不丢事件 | `BackpressureBlocks` + `BACKPRESSURE` 日志 |
| 待重试写出的窗口结果 | `MaxPendingResults`（默认 10000） | 撞限**强制触发一次写出**并背压阻塞；写出持续失败到重试上限则**明确拒绝停机** | `PendingLimitHits` + `PENDING LIMIT HIT`；停机 `SinkFatal`=1 且返回 `ErrSinkUnavailable` |
| 待重试写出的迟到记录 | `MaxPendingLates`（默认 10000） | 同上 | 同上 |
| 检查点代数 | `MaxCheckpoints`（默认 3） | 保存后裁剪更旧代数；恢复旧代时裁剪更新代数 | `CheckpointEvictions` + 代数目录 |

写出端永久不可用时，引擎返回 `ErrSinkUnavailable` 停机并保留全部待写出数据；
磁盘恢复后重启即可从未确认代继续重放，内存与磁盘不会无限增长。

### 并发与状态安全

- 窗口状态按 `key` 哈希分片，同一 key（因而同一窗口）只由持有该分片锁的协程更新。
- 检查点采用屏障：暂停生产者 → 等待在途事件清空 → 按固定顺序锁全部分片做快照，
  无竞态、无半更新、无窗口错位（`go test -race` 验证）。

## 本地验证方法

```bash
# 单元 + 集成测试：乱序、重复、迟到、崩溃重启、写出失败重试/重放、
# 多代恢复、v1 兼容、损坏/不可读/版本不符、资源上限、背压、并发一致性
go test ./internal/engine/ -v          # 日志含事件、窗口判定、重试与恢复依据
go test -race -count=3 ./internal/engine/

# 端到端演练 1：写出阶段前 3 次提交失败，观察自动重试/重放与最终累计
go run ./cmd/streamengine -dir /tmp/demo-fail -events 500 -sink-fail 3
#   关注日志：SINK FAIL / SINK RETRY ... replaying N window result(s), M late record(s)
#   关注输出：stats 行的 sink_retries / replay_results / replay_lates / sink_fatal

# 端到端演练 2：多代检查点 + 指定旧代恢复
go run ./cmd/streamengine -dir /tmp/demo-rid -events 400 -cp-generations 3
ls /tmp/demo-rid/checkpoint.json.d/
go run ./cmd/streamengine -dir /tmp/demo-rid -events 400 -restore-id 1
#   关注日志：RECOVER explicitly requested / pruned N generation(s) /
#             RECOVER using generation=1 resume_offset=… + 逐窗口状态
```

关键测试：

| 测试 | 覆盖 |
|---|---|
| `TestOutOfOrderDuplicatesLate` | 乱序归属、重复去重、迟到旁路（对照独立模型） |
| `TestLateEventDoesNotModifyCommittedWindow` | 已提交窗口不被迟到事件回改 |
| `TestCrashRecovery` | 中途取消模拟崩溃，恢复后与无崩溃基线逐窗口一致 |
| `TestSinkFailureRetryNoDuplicates` | 写出连续失败后同代重试成功，结果与基线一致、不重复 |
| `TestSinkPartialWriteReplayIdempotent` | 写一半失败（残缺尾行）后幂等重放，残留伪造记录不出现 |
| `TestCrashAfterCommitReplaysUnconfirmed` | commit 后/checkpoint 落盘前被杀，重启回滚+重放，结果与迟到数与基线一致 |
| `TestLateRecordsReplayedOnSinkFailure` | 迟到记录随失败批次同代重放，恰好一份、原因完整 |
| `TestCheckpointGenerationRetention` | 只保留最近 N 代，裁剪计数可观测 |
| `TestRestoreFromOlderGeneration` | 指定历史代数恢复并跑完，与从头跑逐窗口/累计/迟到一致 |
| `TestRestoreSpecifiedGenerationMissing` | 指定不存在的代数明确失败，不静默回退 |
| `TestPruneFromDirectly` | 恢复旧代时更新代数被裁剪、计数正确 |
| `TestLegacyV1CheckpointCompatible` | 升级前 v1 单文件检查点可读可续跑，v2 写出后旧文件归档 |
| `TestCheckpointUnreadableVsCorrupt` | 权限“读不了”与内容“损坏/版本不符”明确区分 |
| `TestPendingLimitRejectsWhenSinkDown` | pending 撞限有计数，写出持续失败时返回 `ErrSinkUnavailable` 停机 |
| `TestPendingLimitFlushesAndRecovers` | pending 撞限强制写出，sink 恢复后正常完成且结果正确 |
| `TestSinkTruncateRollback` | 未确认检查点的输出被回滚 |
| `TestCheckpointCorrupt` / `TestCheckpointVersionMismatch` / `TestRecoverFromCorruptCheckpointFails` | 检查点失效可区分、不静默继续 |
| `TestBackpressureObservable` | 队列容量 1 时背压可观测且结果仍正确 |
| `TestConcurrentShardsConsistency` | 8 分片 8 协程下无重复输出、计数自洽（`-race`） |

## 已知限制（本地模拟范畴）

- 检查点代数与输出文件需同时可用（若代数目录整体丢失，只能全新开始并清空输出）。
- 去重集合随事件数线性增长（真实系统可用布隆过滤器/状态 TTL）。
- 单源单文件顺序读取；多源需要为每个源维护独立位点。
- 重试为同步退避（本地文件语义）；真实远端存储通常需要异步重试队列与死信处理。
