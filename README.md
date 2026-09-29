# resource-reconciler-go

一个**本地可运行**的声明式资源控制器。控制面持久化（期望状态 + 控制器状态）与
"真实后端"（模拟执行器）全部在进程内完成，不依赖任何外部系统，便于复现和推理。

它演示在**多版本、并发、重启**这三类场景下，期望状态、实际状态、准入校验与
失败处理如何产生一致且可解释的收敛结果。

---

## 1. 架构总览

```
 Apply/Delete                 bounded key queue          workers
 (desired state)   ───────►  (dedup + in-flight  ─────►  Reconciler
                               exclusion + delayed        │
 admission +     ───────────  requeue, hard cap)          ├─ DecodeSpec / Validate
 defaulting                                                ├─ ordered Ensure / Remove
                                                           └─ CAS write (optimistic lock)
        ◄──────────────  DecisionSink (reason-coded log) ─┘
 MemStore (in-memory + atomic snapshot file, monotonic resourceVersion)
 FakeActuator (idempotent, DURABLE actual backend file + transient/deterministic failure injection)
```

- `pkg/apis`：版本化 wire 类型、严格解码、确定性默认值、跨对象校验、状态条件与原因码。
- `pkg/storage`：带单调 `resourceVersion` 的进程内存储，支持 CAS（Create/Update/Delete）
  与原子快照（temp + rename），模拟控制面持久化。
- `pkg/engine`：有界键控队列、时钟（真实/手动推进）、有界退避+抖动、幂等执行器、
  带乐观锁重试与失败分类的调和核心。
- `pkg/controller`：装配（worker 池、启动恢复、周期 resync、bounded 处理）与
  对期望状态的 `Apply` / `Delete`。
- `pkg/controller/recovery.go`：**启动实际侧对账**。分页扫描持久化实际后端，
  按持久化所有权事实判定保留/接管/清理，有界、可取消、可超时、幂等可重入。
- `cmd/demo`：覆盖全部语义的本地演示，逐步打印**输入、判定（含原因码）、最终状态**。

---

## 2. 收敛语义

### 2.1 期望状态声明与收敛（声明式、幂等）

- 重复施加同一期望**不产生重复副作用**：
  执行器 `Ensure` 是幂等的——实际已匹配期望时返回 `Unchanged` 且不记录任何副作用；
  控制器用规范化规格的 `SpecHash` 判断"已符合期望"，此时**不写库、不改条件、
  不更新 `resourceVersion`**（"已符合期望的对象不得被无谓改写"）。
- 实际偏离期望时被修正：执行器对比 `ActualState` 与 canonical spec，仅在漂移时
  产出一次 `Updated`，且更新保持资源身份（`ResourceID`）稳定。
- 每对象副作用数是可断言的：创建后反复调和，`SideEffectCount` 恒定为 1
  （见 `pkg/engine/actuator_test.go`、`pkg/controller/controller_test.go`）。

### 2.2 生命周期的有序推进（创建/更新/删除）

用一个 **finalizer** 表达"控制器仍持有模拟实际资源"，并把删除拆成三个幂等阶段：

1. **依赖门控**：依赖未就绪（不存在、无 finalizer、非 Ready@当前 generation、
   或正被删除）时，对象停在 `DependentsBlocked=True / DependencyPending`，
   **绝不提前创建**。
2. **先持久化 finalizer，再生效副作用**：副作用之前先 CAS 写 finalizer，
   因此失败/崩溃不会留下"无主"实际资源。
3. **删除严格逆序**：删除一个仍被存活子对象引用的对象会被拒绝并记录
   `DependencyPending`；必须先删子对象。删除时：① 幂等 `Remove` 实际资源并持久化
   `actual=nil` → ② 释放 finalizer → ③ 从存储删除。任一阶段失败都不会留下
   半完成态或孤儿资源（重启后依据持久化阶段继续，而不会重复 Remove）。

依赖就绪/被删除后通过 `DependencyEnqueuer` 触发被阻塞对象重新入队；
周期 **resync** 作为兜底自愈错过的信号。

### 2.3 乐观并发控制（OCC）

- 每次写都携带读取时的 `expectedRV`；存储在 rv 不匹配时返回 `ErrConflict`，
  写入**不会覆盖较新状态**。
- `Reconcile` 内部以**有界跳数**（`maxCASHops`）重新 Get 并重试，因此调和过程中
  规格被更新时，结果总是收敛到**最新版本**。正常的多步流程（加 finalizer、
  删除阶段切换）使用内部 `errContinue`，与真正的写冲突区分开，不互相消耗预算。
- 队列保证同一 key 同时只被一个 worker 处理（in-flight 互斥）；执行器额外统计
  每 key 并发峰值（测试中断言恒为 1）。

### 2.4 工作量与耗时上限（有界）

- 队列对**去重后不同 key 数量**设硬上界；对同一对象的高频更新会被去重，
  因此"热点对象刷屏"不会撑爆队列，也不会出现无界排队或内存无界增长。
- 队列满时 `Apply` 的期望状态仍被持久化，但立即返回可区分的
  `QueueFullError`/`QueueOverflow` 记录（`Stats().Overflowed` 可观察），
  由 resync 兜底收敛——**拒绝/降级有迹可循，而不是无限堆积**。
- 单次调和的 CAS 跳数有界；瞬时重试次数有界（`MaxAttempts`）。
- 单个 spec 大小有上界（`maxObjectSize`），防止超大对象耗尽内存。

---

## 3. 准入规则与默认值

### 3.1 严格、显式的拒绝（不静默忽略、不猜测）

使用 `json.Decoder.DisallowUnknownFields`，任何未知版本 / 未知 kind / 未知字段都被
显式拒绝，并给出**可区分的原因码**（无需解析错误文本）：

| 输入 | 原因码 |
|---|---|
| 不支持的 `apiVersion`（含空） | `UnsupportedVersion` |
| 不支持的 `kind` | `UnsupportedKind` |
| spec 里出现未知字段 | `UnknownField` |
| spec 非法 JSON / 字段类型错误 | `MalformedSpec` |
| `metadata.name` 缺失/非法 | `MissingName` / `NameInvalid` |
| `replicas` 越界（0–10） | `ReplicasOutOfRange` |
| `graceSeconds` 越界（0–3600） | `GraceOutOfRange` |
| v1 使用 `Auto` / v2 使用未知 mode | `UnsupportedMode` |
| 自依赖 | `SelfDependency` |
| 依赖不存在 | `DependencyNotFound` |
| 依赖成环 | `CircularDependency` |

准入在 **Apply 写库之前**完成（含基于全量存储的图校验）：非法对象不会进入
存储、队列或执行器，因此不会产生任何副作用，也不会被加上 finalizer。

### 3.2 版本兼容与确定性默认值

- v1 与 v2 的 wire 结构不同（v2 新增 `graceSeconds` 与 `mode: Auto`）。
  历史 v1 对象在**读取时**被确定性补齐 `graceSeconds=30`，并把"补了哪些默认值、
  补成什么"记录到 `status.defaults`，使判定依据可追溯。
- 默认值语义确定：同一输入重复解码结果相等、`SpecHash` 相同；
  显式给出的零值（如 `"replicas":0`）**不会**被当作缺省而覆盖。
- 不支持的版本/字段永远显式报错，而不是被静默忽略或靠猜测填充。

---

## 4. 失败分类与退避

| 类别 | 行为 | 可观察状态 | 是否空转 |
|---|---|---|---|
| **瞬时失败**（超时/5xx 之类） | 有界指数退避 + 比例抖动后重试，失败预算用尽后转为终态 | `Progressing=True/TransientError`（消息含第几次、多久后重试） | 否：退避后才重试，延迟有上界 |
| **确定性失败**（4xx/坏配置之类） | 不重试，立即终态 | `Ready=False/DeterministicError`，`status.fatalReason/fatalMessage` 持久化 | 否：同 generation 重复调和**不再调用后端、不再写库** |
| **重试预算耗尽** | 由瞬时升级为终态 | `Ready=False/RetryBudgetExhausted` | 否 |

- 退避：`Base` 起，按 `Factor` 指数增长，封顶 `Max`，抖动为 `[1-J,1+J)` 比例抖动；
  关闭抖动源（传 `nil`）时结果完全确定，测试可复现。
- 新的期望 generation 会**清空旧的失败/重试计数**，新输入不被旧失败绑架；
  同 generation 的重读不会被误判为"规格变更"。
- **重启续作（控制面 + 实际面双重持久化）**：attempts、终态原因、`actual`、
  finalizer、删除时间戳持久化在控制面快照；真实资源身份与内容持久化在实际侧文件。
  重启先跑**有界启动对账**（见 7.1–7.3），再把所有持久化对象入队一次，从已持久化
  阶段继续：已生效的副作用不重复（执行器 `Remove` 对"已不存在"返回 no-op，
  `Ensure` 对"已匹配"返回 `Unchanged`，对"资源存在但记账缺失"返回 `Adopted`
  而非再次创建）。

---

## 5. 状态可观测

每个对象都有一组带**原因码**的条件（判定依据可追溯）：

- `Admission`：`Accepted` 或具体拒绝原因。
- `DependentsBlocked`：因依赖未就绪/删除受阻（`DependencyPending`）。
- `Progressing`：`TransientError`（含退避信息）/ `Deleting` / 稳态 `Ready`。
- `Ready`：就绪（稳态原因稳定为 `Ready`，不因 Created/Unchanged 反复翻转），
  或 `DeterministicError` / `RetryBudgetExhausted` / `DependencyPending`。

控制器与调和器的每个判定都产出一条 `Decision`（输入 gen/hash、动作、原因码、
消息、结果 rv）。`WriterSink` 输出每行一个 JSON；`SliceSink` 供测试断言。
demo 会把**输入、判定、最终状态**全部打印出来。

> 稳态条件的 reason 与 message 都是**稳定**的；`LastTransitionTime` 仅在语义变化时
> 更新，因此重复收敛不会造成条件翻转或无谓写库。

---

## 6. 本地验证方法

需要 Go 1.25+。竞态检测依赖 cgo（本机有 gcc 时）：

```bash
# 常规测试
go test ./... -count=1

# 竞态检测（推荐；需要 CGO_ENABLED=1 与 C 编译器）
CGO_ENABLED=1 go test ./... -race -count=1

# 多轮重复，验证异常路径结果可复现、无 flaky
CGO_ENABLED=1 go test ./... -race -count=5

# 可运行演示：正常收敛、幂等、漂移修正、非法拒绝、瞬时/确定性失败、
# 有序删除、重启续作（实际身份/副作用不变），逐步打印输入/判定/最终状态
go run ./cmd/demo

# 崩溃窗口重现：在"动作已生效、记账未落"的精确点强杀，再单独恢复
DIR=$(mktemp -d)
go run ./cmd/demo -snapshot $DIR/store.json -actual $DIR/actual.json \
  -crash AfterEnsure -crash-key a          # 进程以退出码 42 停在屏障
go run ./cmd/demo -snapshot $DIR/store.json -actual $DIR/actual.json \
  -recover-only                            # 对账+接管：身份 res-N 不变、0 副作用
go run ./cmd/demo -snapshot $DIR/store.json -actual $DIR/actual.json \
  -recover-only                            # 再跑一次：Unchanged，结果幂等
# 删除窗口同理：把 -crash 换成 AfterRemove
```

`-recover-only` 会打印每个对象的 `actualID`（实际侧身份）、`recordedID`
（控制面记录身份）、是否在删除、以及本生命周期的副作用数，便于直接断言重启无感。

### 主要测试（以异常路径为主）

- `pkg/apis/admission_test.go`：非法版本/字段/越界/模式、v1 历史对象补默认、
  默认值重复读取稳定、自依赖/缺依赖/环。
- `pkg/storage/store_test.go`：CAS 冲突、32 并发下每版本唯一赢家、
  快照重启后 rv 语义保留、损坏/未知格式快照被拒绝。
- `pkg/engine/actuator_test.go`：幂等 create/update/remove、瞬时预算耗尽后成功、
  确定性失败永不创建资源、副作用审计。
- `pkg/engine/actuator_durable_test.go`：实际侧跨重启**身份不变**、接管不计副作用、
  删除跨重启不复活、实际 key 分页有序枚举。
- `pkg/engine/reconciler_test.go`：创建后幂等、漂移修正、依赖门控与删除保护、
  瞬时退避后收敛、确定性失败终态不空转、准入拒绝零副作用、换 generation 清除旧失败。
- `pkg/engine/queue_test.go`：去重/FIFO、容量上界溢出、in-flight 同 key 互斥、
  延迟入队随时钟推进就绪、cancel/close 唤醒。
- `pkg/controller/controller_test.go`：并发热点对象收敛一致且执行器不并发、
  非法 spec 写库前被拒、队列溢出被拒绝并计数、**重启后续作且不重复副作用**、
  **重启续作删除不留孤儿**。
- `pkg/controller/recovery_test.go`（本轮）：
  - `TestRestartKeepsActualIdentityAndNoDuplicateSideEffects`：连续 3 次重启，
    身份/数量/内容不变、新生命周期 0 副作用；
  - `TestCrashBetweenEnsureAndRecordIsRecovered`：创建动作已生效、记账前强杀，
    重启后以同一身份 `Adopted`、0 副作用并 Ready；
  - `TestCrashBetweenRemoveAndRecordLeavesNoOrphan`：删除动作已生效、记账前强杀，
    重启后无孤儿、对象彻底删除；
  - `TestStartupReclaimsResidualsButKeepsOwned`：无主残留被清，finalizer 持有的资源
    绝不误清、身份不变；
  - `TestDriftIsCorrectedAcrossRestartKeepingIdentity`：手动改坏实际资源，重启纠偏、
    身份保持、恰好一次更新；
  - `TestLegacySnapshotTakenOverInPlace`：上一版本快照就地接管，身份逐字保留、
    0 重建、`Migrated=1`；
  - `TestWritesDuringRecoveryAreAcceptedAndConverge`：恢复扫描期间并发写入正常受理并收敛；
  - `TestRecoveryCancellationReturnsPromptly` / `TestRecoveryTimeoutStopsAndResyncCompletes`：
    取消立即返回、超时记录可区分原因且由 resync 补齐；
  - `TestRepeatedCrashesDuringRecoveryAreIdempotent`：恢复过程中再次崩溃，结果仍幂等一致。

  以上断言都落在可观察结果上（`ResourceID`、`SideEffectCount`、资源数量、
  最终 Ready / 对象存在性、`Stats().Recovery`），并在 `-race -count=3` 下可复现。

---

## 7. 设计取舍（一致性的边界）

### 7.1 实际侧持久化与恢复语义（本轮新增）

"真实后端"现在同样是**持久化**的：`FakeActuator` 每次**已提交**的变更（创建 /
更新 / 删除）在调用返回前都以原子快照（temp + fsync + rename，`formatVersion=1`）
落到独立的实际侧文件，资源身份（`ResourceID`）与 ID 计数器一并持久化。进程重启后
`NewDurableFakeActuator(path)` **就地接管**该文件，因此：

- 无外部改动时，重启多少次都不会再执行一次已经生效过的创建/更新/删除：
  实际资源的**身份、数量、内容**保持不变，`SideEffectCount` 不因重启增长。
- 手动改坏实际资源制造漂移后，重启时正常调和按期望内容纠偏，纠偏保持原身份。
- 控制面对象里早已写下的 `status.actual` 可被**就地迁移**：若实际侧文件不存在
  （上一版本的数据），启动时按对象分页读取，把带 finalizer 的对象已记录的实际资源
  用 `Hydrate` 原样灌回实际侧，身份逐字保留、零创建副作用、无需清空重来
  （决策 `RecoveryMigrated`，`Stats().Recovery.Migrated` 可观察）。

**动作与记录之间的崩溃窗口**由"动作先落实际侧、再写控制面记录"的顺序 + 启动对账
共同兜住。调和器在两个精确屏障暴露崩溃注入点（`CrashHook`）：

| 屏障 | 崩溃时的持久化事实 | 重启后结论 |
|---|---|---|
| `AfterEnsure`（创建/更新/接管已提交，控制面尚未记录） | 实际侧有资源；控制面有 finalizer、`status.actual` 缺 | 启动判定**保留**，调和 `Ensure` 返回 `Adopted`：沿用既有身份，0 副作用 |
| `AfterRemove`（删除已生效，`status.actual=nil` 尚未记录） | 实际侧已无资源；控制面仍有 finalizer+actual、带删除时间戳 | `Remove` 幂等 no-op，继续完成 finalizer 释放与对象删除，无孤儿 |

### 7.2 对账（reconcile-actual）的归属判定规则

判定**只依据持久化的控制面事实**，确定且可追溯（每条结论都有原因码日志）：

- 实际资源的 key 在控制面**没有对象**：无主残留 → 删除（`RecoveryReclaimed`）。
- 对象存在但**没有**控制器 finalizer：执行器在 finalizer 持久化之前从不被调用，
  因此它不可能是在途创建，确定是孤儿 → 删除并入队收敛（`RecoveryReclaimed`）。
- 对象**有** finalizer：控制器持久化持有该资源，**无条件保留**——无论控制面是否
  已记录，都不会被误判成残留；记录缺失即"动作已生效、记账未落"，入队后由正常调和
  以 `Adopted` 接管（`RecoveryRetained` / `RecoveryAdopted`）。

对账**幂等可重入**：重复重启、或恢复过程中再次崩溃，结论一致；残留删除与资源接管
都是幂等操作。

### 7.3 有界与可取消

- 启动扫描与旧数据迁移都**分页**（`ActualKeys` / `MemStore.Keys` 的 cursor +
  pageSize），不会一次性把全部实际资源载入内存；页间按 `PagePause` 让出。
- 扫描与迁移完全由 context 驱动：等待依赖就绪、退避重试、恢复扫描都响应取消与
  `Timeout`。被取消/超时后尽快返回，并留下可区分原因
  （`RecoveryCancelled` / `RecoveryTimedOut`，`Stats().Recovery` 可观察）；
  未扫完的部分由周期 resync 幂等补齐。
- 恢复在 worker 池启动后于后台进行，**恢复期间新的期望状态写入照常受理并最终收敛**；
  工作队列仍是去重 + 硬容量上界，超载按既有策略返回 `QueueOverflow` 而不无界排队。

### 7.4 边界

- 存储是单进程内存 + 文件快照；它精确模拟 etcd 风格的单调 rv 与 CAS 语义，
  但不模拟网络分区。一致性论证限定在"单控制面、进程内/崩溃重启"模型内。
- 所有时间与抖动都可注入（`Clock`、`jitter func`），失败可注入（`FailureRule`），
  崩溃点可注入（`CrashHook`），因此异常路径测试是确定性的，而不是依赖真实睡眠或随机。
