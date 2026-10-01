# event-time-stream-engine-go

一个可在本地运行的流处理引擎原型：用本地 JSONL 文件模拟数据源与写出存储，
实现**事件时间窗口聚合、水位线与迟到处理、恰好一次输出、写出失败续写、
写出中途被杀的半截残行修复、多代检查点与指定代恢复、资源上限与背压、并发安全**，
使乱序、迟到、写出故障与崩溃恢复之后的结果保持一致且可解释。

## 运行

```bash
# 一次性跑完（自动生成含乱序/重复/迟到的输入）
go run ./cmd/streamengine -dir demo-data -events 2000

# 模拟中途崩溃并从检查点恢复
go run ./cmd/streamengine -dir demo-data -events 2000 -crash

# 模拟写出阶段“写到一半磁盘失败”（前 3 次提交各落盘 1 条后失败，引擎自动续写补齐）
go run ./cmd/streamengine -dir demo-fault -events 600 -sink-faults 3

# 保留 5 代检查点，并从指定的历史代（第 2 代）恢复
go run ./cmd/streamengine -dir demo-gen -events 600 -retain-checkpoints 5
go run ./cmd/streamengine -dir demo-gen -events 600 -retain-checkpoints 5 -recover-from 2

# 测试（含竞态检测）
go test ./... -v
go test -race ./internal/engine/
```

工作目录下的文件：

| 文件 | 作用 |
|---|---|
| `input.jsonl` | 输入事件（每行一个 JSON：`id/key/event_time/value`） |
| `results.jsonl` | 已提交的窗口结果。每行是带 CRC32 校验的封装 `{v,crc,p}`，业务字段在 `p` 里（含 `checkpoint_id`）；旧版写出的裸记录行仍可直接读取 |
| `late.jsonl` | 被旁路的迟到事件（带原因与当时水位线，行格式与 `results.jsonl` 相同） |
| `checkpoint.json` | 最新一代检查点的指针副本（兼容老版本布局与工具） |
| `checkpoint-NNNNNNNNNN.json` | 每代一个不可变检查点文件（位点+水位+窗口状态+去重集合+本代输出批次，含 CRC） |

## 时间与水位语义

- **事件时间**：窗口归属只由事件自带的 `event_time`（毫秒）决定，
  与处理时刻无关；乱序到达不会改变窗口归属。
- **窗口**：固定滚动窗口 `[start, start+size)`，按 `size` 对齐（负时间戳亦正确）。
- **水位线**：每个分片（分区）记录已见最大事件时间，
  全局水位线 = `min(各分区最大事件时间) - 允许迟到时长`，只增不减。
- **窗口提交**：当水位线越过窗口右端点，窗口在下一个检查点收割并写出，
  写出后不再修改。窗口在**检查点代保存成功之前**仍保留在状态中（崩溃兜底）。
  输入耗尽时所有分区水位推到正无穷，剩余窗口在最终检查点全部提交。
- **迟到判定**：`event_time <= 当前水位线` 的事件为迟到。
  迟到事件**不静默丢弃、不回改已提交窗口**，而是带原因旁路到 `late.jsonl`。

## 写出阶段故障：断点续写（不丢、不重）

每条写出记录都有稳定身份：窗口结果 = `key@[start,end)`，迟到记录 = 事件 `id`。
提交一批（`CommitBatch(checkpointID, results, lates)`）时：

1. **修补残行**：裁掉输出文件末尾写到一半的残缺行（进程在 write 中途被杀的情形）；
2. **收集已写身份**：扫描文件中已属于本批（同 checkpointID）的记录；
3. **只补缺失**：已存在的记录跳过，未写的记录逐条 append+flush，全部 fsync 后才算成功。

因此无论“磁盘写一半失败后进程内重试”，还是“进程被杀后重启重放”，
同一批记录都能安全重试任意次，绝不会写出第二份。提交报告 `CommitReport`
与日志会明确说明：`SINK RETRY`（失败待续写）、`SINK RESUME`（续写了多少、
跳过了多少已落盘记录 `replayed`、是否修补了残行 `repaired_tail`）。

故障注入：`FileSink.FailNextCommits(n)` / `FailNextCommitsAfter(n, after)`
让接下来 n 次提交在写出 after 条**完整记录**后返回 `ErrSinkFault`，用于演练磁盘失败；
`FileSink.FailNextTornWrites(n, after, half)` 则在写出 after 条完整记录后，
把下一条记录只落盘前 half 个字节（**无换行**）再失败，确定性地制造
“进程在 write 中途被杀、文件末尾留下半截残行”的现场。

## 半截残行的处理：修复补齐，而不是一启动就报错

进程可能在**写出一批窗口结果或迟到记录的中途被强杀**，导致文件末尾只落下
一部分字节。引擎重启进入恢复流程时，会先对 `results.jsonl` 与 `late.jsonl`
做尾部检查（`FileSink.RepairTails`），然后才做回滚/批次重放：

- 最近一次一致检查点之前的已确认输出原样保留；
- 末尾**没有换行符**的半截内容被认定为“写到一半的残行”，
  裁掉（truncate + fsync）并打日志
  `RECOVER trimmed partial trailing write ... results=<bool> lates=<bool>`；
- 随后这批未确认结果从最近一致检查点**幂等补齐**：已落盘的整行按身份跳过，
  被裁掉/没写完的部分补写，因此**一条不丢、也不重复写**；补齐后再整体重跑一遍，
  同一批结果全部走“已存在跳过”，输出仍然不重复。
- **首次运行、还没存下任何检查点就被杀**也适用：恢复时发现无检查点，
  同样先裁掉残行，再把未确认的历史输出整体回滚（`TruncateTo(0)`），从头重放。

该行为在结果文件残行、迟到文件残行、以及两个文件**同时**残行时都成立
（见 `TestRecoveryAfterPartialTornLines` 三个子用例与
`TestRecoveryFirstRunNoCheckpointPartialTail`）。

### 半截残行 vs 真正损坏：明确区分，绝不硬接

为了把“可以修补的半截残行”和“真正损坏的文件”区分开，输出每行都带
**CRC32 封装** `{v,crc,p}`（旧版裸记录行仍可读）：

| 现场 | 特征 | 处理 |
|---|---|---|
| 半截残行（可修复） | 文件末尾**没有换行**：一次 write 没写完，JSON 中不会含裸换行，所以残行一定无换行 | 裁掉残字节，按检查点批次补齐 |
| 真正损坏（不可硬接） | 行**以换行完整结束**，却无法解析、封装版本未知，或 **CRC 校验不匹配** | 保留文件原样，返回 `ErrSinkCorrupt` 明确报错 |

真损坏时日志为
`RECOVER sink file genuinely corrupt (a complete line failed to parse/verify): ... refusing to treat it as a partial tail`，
与“修补残行”的日志一眼可分。结果、迟到记录、检查点三者的既有语义不因此改变：
检查点损坏仍走 `ErrCheckpointCorrupt`，输出文件损坏走 `ErrSinkCorrupt`。


## 多代检查点与指定代恢复

- 每次检查点写一个不可变代文件，保留最近 `RetainCheckpoints` 代（默认 3），
  超出按 ID 升序删除；删除数量计入 `CheckpointPrunes` 并在日志中可见。
- `Config.RecoverFromGeneration`（CLI: `-recover-from`）可指定从哪一代恢复。
  - 恢复日志说明用的是哪一代（`latest`/`requested`）、格式版本、
    从哪个源位点（字节偏移）接上、当时存活窗口状态、去重集合大小；
  - 该代**之后的“未来代”会被清理**（计入 `ForkPrunes`），保证线性重放、不重复计数；
  - 该代记录的提交批次会被幂等重放（`RECOVER replay ... replayed/rewritten`）。
- 指定的代不存在时返回 `ErrCheckpointNotFound` 并列出可用代数，
  **绝不静默改从最新代硬接**。

### 与老版本的兼容范围

- 检查点当前写出格式为 **v2**，读取支持 **v1 与 v2**：
  - v1 老文件（升级前磁盘上的单个 `checkpoint.json`）首次启动仍可直接加载、继续；
  - 第一次保存 v2 代文件即自动迁移到多代布局（日志含
    `migrated from legacy single-file checkpoint layout`）；
  - v2 新增字段（`pending_results/pending_lates/created_at_unix`）均 `omitempty`，
    v1 文件缺省这些字段按“无未确认批次”处理。
- **读不了的情况明确区分、绝不硬接**：
  文件损坏/CRC 不匹配 → `ErrCheckpointCorrupt`；版本不在支持区间 → `ErrCheckpointVersion`；
  指定代缺失 → `ErrCheckpointNotFound`（均带文件路径与可用代数信息）。

## 资源上限（长时间运行不无限增长）

| 资源 | 上限配置 | 到上限的策略 | 可观测 |
|---|---|---|---|
| 待处理事件队列 | `QueueCapacity` | **阻塞**生产者（背压） | `BackpressureBlocks` + 日志 |
| 待写出记录（窗口结果+迟到记录） | `MaxPendingOutput` | 提前触发写出；到期窗口**延后**留在状态等下轮提交；写出失败自动重试 | `FlushTriggers`、`WindowDefers`、`SinkRetries`、`SinkReplays` |
| 迟到记录溢出 | `LateOverflow=reject/block` | `reject`（默认）：明确拒绝并计数；`block`：阻塞到提交腾出空间 | `LateRejected` + `LATE-REJECTED` 日志 |
| 检查点代数 | `RetainCheckpoints` | 按 ID 删除最旧代 | `CheckpointPrunes`；恢复分叉清理 `ForkPrunes` |

拒绝策略下迟到事件会被显式标记（计数 + 日志），不静默丢失；
阻塞策略与检查点屏障不形成死锁（等待期间不持任何分片锁）。

## 一致性保证（恰好一次）

- 检查点覆盖：源读取位点、各分区水位、窗口累计状态、去重集合、本代提交批次。
- 提交流程：**先写输出（带检查点 ID 与行级 CRC），再保存检查点代**。
  - 写输出失败：批次保留，按记录身份续写，不推进检查点；
  - 进程在 write 中途被杀留下无换行半截残行：恢复先裁掉残行，
    再按下述回滚/重放路径补齐，未确认批次一条不丢、不重；
  - 输出已写、检查点未保存时崩溃：恢复端 `TruncateTo` 回滚该批，
    窗口因仍保留在上一代状态中而被重新收割，再幂等重放——不丢不重；
  - 窗口在检查点保存成功前不离开状态，保证任意崩溃点都有兜底。
- 去重按事件 `id` 分片哈希，随检查点持久化，恢复后仍生效。
- 并发：窗口按 `key` 分片；检查点屏障暂停生产者并等待在途清空，
  按固定顺序锁全部分片快照（`go test -race` 验证）。

## 本地验证方法

```bash
# 单元 + 集成测试：乱序、重复、迟到、崩溃重启、写出失败续写、半截残行修复、
# 指定代恢复、v1 老检查点兼容、代数/待写出上限、背压、并发一致性
go test ./internal/engine/ -v          # 日志含事件、窗口判定、续写、残行修复与恢复依据
go test -race -count=3 ./internal/engine/

# 端到端：写出故障续写（观察 SINK RETRY/RESUME 与 sink_retries/sink_replays）
go run ./cmd/streamengine -dir /tmp/f -events 600 -sink-faults 3

# 端到端：多代 + 指定历史代恢复（观察 fork prune 与 RECOVER replay）
go run ./cmd/streamengine -dir /tmp/g -events 600 -retain-checkpoints 5
go run ./cmd/streamengine -dir /tmp/g -events 600 -retain-checkpoints 5 -recover-from 2
```

### 一步步复现“写出中途被强杀 → 残行 → 重启补齐 → 重跑不重复”

确定性的端到端复现由测试直接给出（不依赖调度/时序，可反复运行）：

```bash
# 结果/迟到/双文件分别残行：重启补齐、逐窗口对齐无故障基线、再重跑不重复写
go test ./internal/engine/ -run TestRecoveryAfterPartialTornLines -v -count=10
# 真实“撕裂写”（半截字节无换行落盘后失败）+ 崩溃，再重启补齐
go test ./internal/engine/ -run TestTornWriteFaultAcrossRestart -v
# 首次运行、还没检查点就被杀并留下残行：重启从头恢复
go test ./internal/engine/ -run TestRecoveryFirstRunNoCheckpointPartialTail -v
# 完整行 CRC 对不上：必须报 ErrSinkCorrupt，不能当残行硬接
go test ./internal/engine/ -run TestRecoveryRejectsGenuinelyCorruptOutput -v
```

在真实文件上手工演练也可以：

1. 先跑一轮基线并保存 `results.jsonl`/`late.jsonl` 作为对照；
2. 让引擎跑到中段后强杀（测试里用 `context` 取消，等价于 kill -9）；
3. 用编辑器/脚本在 `results.jsonl` 末尾粘贴一段**不带换行**的半截 JSON
   （`late.jsonl` 同理，或两个文件都加），保存；
4. 重新启动引擎：日志出现 `RECOVER trimmed partial trailing write ...`，
   随后 `RECOVER replay ...` 把没写完的批次补齐；
5. 对比恢复后的文件：每个窗口的归属/`count`/`sum` 与基线**逐窗口一致**，
   迟到条数一致，没有幽灵残行记录；
6. 再启动一次（源已耗尽，等价于重放确认）：结果条数不再增长，不重复写；
7. 反例：追加一条**带换行**但把 CRC 改错的完整行，重启应直接失败并打印
   `genuinely corrupt ... refusing to treat it as a partial tail`。

关键测试：

| 测试 | 覆盖 |
|---|---|
| `TestOutOfOrderDuplicatesLate` | 乱序归属、重复去重、迟到旁路（对照独立模型） |
| `TestLateEventDoesNotModifyCommittedWindow` | 已提交窗口不被迟到事件回改 |
| `TestCrashRecovery` | 中途取消模拟崩溃，恢复后与无崩溃基线逐窗口一致 |
| `TestSinkCommitResumeAfterFault` | 写出失败后进程内续写：跳过已写、补齐缺失、幂等 |
| `TestSinkRepairPartialTail` | 进程被杀留下的半行残数据被修补后正确续写 |
| `TestRepairTailsTrimsPartialLastLine` | 无换行半截残行被裁掉；再次检查为 no-op |
| `TestRepairTailsRejectsGenuinelyCorruptCompleteLine` | 完整行坏 JSON/CRC 错/版本未知 → `ErrSinkCorrupt`，且文件原样保留 |
| `TestRepairTailsAcceptsLegacyBareLines` | 旧版裸记录行可读；其后残行仍可裁；裸坏行仍判损坏 |
| `TestTornWriteFaultLeavesRepairableHalfLine` | 撕裂写故障留下无换行半截，修复后同批补齐、再提交幂等 |
| `TestRecoveryAfterPartialTornLines` | 结果/迟到/双文件分别残行：重启补齐、对齐基线、再重跑不重复写 |
| `TestTornWriteFaultAcrossRestart` | 真实撕裂写 + 崩溃，重启从最近检查点补齐，不丢不重 |
| `TestRecoveryFirstRunNoCheckpointPartialTail` | 首次运行无检查点即被杀并留残行：重启裁掉残行从头恢复 |
| `TestRecoveryRejectsGenuinelyCorruptOutput` | 输出文件完整行 CRC 错：恢复明确失败、不当残行硬接 |
| `TestSinkFaultInRunResume` | 引擎内多次写出失败自动续写，窗口/计数/求和与基线一致 |
| `TestSinkFaultThenCrash` | 写出失败+进程被杀，重启回滚+批次重放，不丢不重 |
| `TestRecoverFromSpecificGeneration` | 从指定历史代恢复、分叉未来代清理、结果与基线一致 |
| `TestLegacyV1CheckpointCompatible` | 老版本 v1 单文件检查点可读、可恢复、可跑完 |
| `TestCheckpointRetentionLimit` | 保留代数上限生效、旧代被删并有计数 |
| `TestPendingOutputCapPolicies` | 待写出上限：reject 明确拒绝/计数、block 阻塞后放行 |
| `TestRecoverFromCorruptCheckpointFails` | 检查点 CRC 损坏时恢复明确失败、不静默继续 |
| `TestCheckpointCorrupt` / `TestCheckpointVersionMismatch` | 检查点损坏与版本不符可区分报错 |
| `TestBackpressureObservable` | 队列容量 1 时背压可观测且结果仍正确 |
| `TestConcurrentShardsConsistency` | 8 分片 8 协程下无重复输出、计数自洽（`-race`） |

## 已知限制（本地模拟范畴）

- 检查点与输出文件需同时可用；去重集合随事件数线性增长（真实系统可用布隆过滤器/TTL）。
- 单源单文件顺序读取；多源需要为每个源维护独立位点。
- 写出续写按记录身份去重，要求同一批内窗口坐标/迟到事件 ID 唯一（本模型满足）。
