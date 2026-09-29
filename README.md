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
ActualStore (in-memory, or per-file directory backend with fsync+rename)
FakeActuator (durable-identity idempotent backend + failure/crash injection)
```

- `pkg/apis`：版本化 wire 类型、严格解码、确定性默认值、跨对象校验、状态条件与原因码。
- `pkg/storage`：带单调 `resourceVersion` 的进程内存储，支持 CAS（Create/Update/Delete）
  与原子快照（temp + rename），模拟控制面持久化。
- `pkg/engine`：有界键控队列、时钟（真实/手动推进）、有界退避+抖动、
  幂等且**跨重启持久化身份**的执行器、启动/周期所有权对账（reaper）、
  带乐观锁重试与失败分类的调和核心，以及"动作↔记账"崩溃窗口注入。
- `pkg/controller`：装配（worker 池、**分页、可取消、有超时**的启动恢复与
  周期 resync 对账、bounded 处理）与对期望状态的 `Apply` / `Delete`。
- `pkg/actual`：实际侧持久化后端。`DirStore` 把每个真实资源存成一个 JSON 文件，
  写入走临时文件 + fsync + rename；强杀进程后磁盘上只有旧文件或新文件，没有撕裂记录。
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

### 2.2b 实际侧持久化与重启对账（本轮新增）

控制面与实际侧现在**都**跨进程持久化，重启对用户无感：

- **稳定身份**：真实资源的 `ResourceID` 由归属对象名确定性派生
  （`res-<sha256(name)[:24]>`），旧版本记录在案的 ID 会被就地采纳。
  因此无论重启多少次，同一个对象对应同一个真实资源；重建进程（全新内存、
  同一实际目录）后再次 `Ensure` 只会得到 `Unchanged`，**不会再创建/更新/删除一次**。
- **漂移纠正且不换身份**：实际资源被外部改坏（内容、甚至 ID 被换掉）时，
  对账把内容纠回期望，身份以实际侧现存资源为准并回写记账，不新建第二个资源。
- **崩溃窗口**：副作用与记账之间有三个窗口——`after-create` / `after-update` /
  `after-remove`（动作已落盘、结果尚未写回 status 的瞬间）。钩子在这些点强杀进程，
  重新拉起后：
  - 已生效的创建/更新不会重复，记账补齐到磁盘上已存在的资源；
  - 已生效的删除不会"复活"资源，删除阶段从 `actual=nil` 继续；
  - 对账幂等可重入，恢复途中再次崩溃、反复重启，最终结果一致。

**保留还是清理，依据确定且可追溯。** 启动与周期 resync 都对实际侧做一次所有权审计：

| 实际侧资源的归属判定 | 处置 | 决策原因码 |
|---|---|---|
| 存在同名对象 **且** 带 finalizer | 保留，不动 | `ActualKept` |
| 存在同名对象但无 finalizer（记账可能落后） | **保留**并交给正常调和，绝不先删 | `ActualKept`（defer） |
| 控制面中**不存在**同名对象（确定无主） | 删除该实际资源 | `OrphanReclaimed` |

只有"控制面无此对象"才是删除实际资源的依据；因此"资源有效、只是记账没落盘"
（对象与 finalizer 已先于副作用持久化）永远不会被误判成残留。每个判定都写一条
带原因码与解释的 `Decision`，可在日志里追溯为什么留、为什么删。

### 2.2c 对账有界、可取消、可超时

- **分页扫描**：控制面（`MemStore.ListKeysPage`）与实际侧（`Store.ListPage`）
  都按游标分页，页大小 `Config.RecoveryPageSize`（默认 64）。实际资源很多时
  不会一次性全部读进内存，也不会长时间卡住启动——审计在独立 goroutine 中进行，
  恢复期间 `Apply` 写入照常受理并最终收敛。
- **取消与超时**：每次审计通过 `context.WithTimeout(parent, RecoveryTimeout)`
  限界（默认 30s）。等依赖就绪、退避重试、恢复扫描的每条路径都检查 ctx；
  被取消/超时时尽快返回，并记录可区分的 `RecoveryCancelled` / `RecoveryTimedOut`，
  下一次 resync 从断点继续。
- **不无界排队**：审计入队同样走有界队列；满了就记录 `QueueOverflow` 并跳过该 key，
  由下一轮审计兜底，不撑爆内存。
- `Stats().Reclaimed` 给出本轮运行内回收的无主资源数，可观察、可断言。

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
- **重启续作（控制面与实际侧都持久化）**：attempts、终态原因、`actual`、
  finalizer、删除时间戳在控制面持久化；真实资源本身在 `actual.DirStore` 持久化，
  身份跨重启稳定。重启时分页重新入队所有对象并对实际侧做一次所有权审计，从已持久化
  阶段继续：已生效的副作用绝不重复（`Ensure` 对"已匹配"返回 `Unchanged`，
  `Remove` 对"已不存在"返回 no-op）；动作已生效但记账未落下的资源被**就地补齐而非
  重建**；确定无主的残留被回收。详见 §2.2b / §2.2c。

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
# 有序删除、实际侧持久化重启续作，逐步打印输入/判定/最终状态
go run ./cmd/demo
```

演示的第 8 步会把控制面快照与实际资源目录都留在磁盘上（`-snapshot`、
`-actual-dir` 可指定），用同一个 `-actual-dir` 重启即可看到资源被原样接管、
副作用计数为 0，审计日志逐条给出 `ActualKept` / `OrphanReclaimed`。

### 把崩溃点摆出来（强杀重现）

崩溃窗口通过环境变量选择，进程在副作用**已落盘、记账尚未写回**的瞬间以退出码 77 自杀：

```bash
# 可选三个点：after-create / after-update / after-remove
# RECONCILER_CRASH_KEY 可进一步限定到某个对象
go run ./cmd/demo -actual-dir ./demo-actual -snapshot ./demo-snap.json
RECONCILER_CRASH_POINT=after-create \
  go run ./cmd/demo -actual-dir ./demo-actual -snapshot ./demo-snap.json
# 再干净地拉起一次：资源身份不变、不重复副作用、记账补齐
go run ./cmd/demo -actual-dir ./demo-actual -snapshot ./demo-snap.json
```

对应的自动化重现是 `pkg/controller/crash_test.go`：父进程用 `exec` 重新启动测试
二进制，子进程在选定窗口 `os.Exit(77)`（真实强杀，不是模拟错误返回），父进程再
启动一个干净进程断言最终身份、副作用次数与最终状态。覆盖创建、更新、删除三个窗口
以及在恢复过程中反复崩溃的幂等性。

### 主要测试（以异常路径为主）

- `pkg/apis/admission_test.go`：非法版本/字段/越界/模式、v1 历史对象补默认、
  默认值重复读取稳定、自依赖/缺依赖/环。
- `pkg/storage/store_test.go`：CAS 冲突、32 并发下每版本唯一赢家、
  快照重启后 rv 语义保留、损坏/未知格式快照被拒绝。
- `pkg/engine/actuator_test.go`：幂等 create/update/remove、瞬时预算耗尽后成功、
  确定性失败永不创建资源、副作用审计。
- `pkg/engine/reconciler_test.go`：创建后幂等、漂移修正、依赖门控与删除保护、
  瞬时退避后收敛、确定性失败终态不空转、准入拒绝零副作用、换 generation 清除旧失败。
- `pkg/engine/queue_test.go`：去重/FIFO、容量上界溢出、in-flight 同 key 互斥、
  延迟入队随时钟推进就绪、cancel/close 唤醒。
- `pkg/controller/controller_test.go`：并发热点对象收敛一致且执行器不并发、
  非法 spec 写库前被拒、队列溢出被拒绝并计数、**重启后续作且不重复副作用**、
  **重启续作删除不留孤儿**。
- `pkg/actual/store_test.go`：目录后端跨实例读写身份一致、删除幂等、分页有序且有界、
  外来文件（如控制面快照）不被误认成实际资源、非法 key 拒绝、ctx 取消生效。
- `pkg/engine/actuator_durable_test.go`：持久后端重启后 `Ensure` 为 `Unchanged` 且
  身份不变、外部漂移被同身份纠正、删除跨重启幂等、崩溃钩子在落盘后触发。
- `pkg/engine/reaper_test.go`：所有权审计三分支（回收确定孤儿 / 保留 finalizer 资源 /
  无 finalizer 先保留并交给调和），且响应取消。
- `pkg/controller/recovery_test.go`：重启身份与副作用不重复、手动改坏实际资源被纠回、
  孤儿被回收而有效未记账资源不被误清、恢复期间 30 路并发写入全部收敛、
  取消立即返回与超时产生 `RecoveryCancelled`/`RecoveryTimedOut`、旧快照就地接管。
- `pkg/controller/crash_test.go`：创建/更新/删除动作与记账之间强杀后的恢复，
  以及恢复中反复崩溃的幂等性（子进程退出码 77）。

---

## 7. 设计取舍（一致性的边界）

- "真实后端"是进程内模拟，但实际资源通过 `actual.DirStore`（每资源一个 JSON 文件、
  fsync+rename 原子写）**跨重启持久化**，精确模拟"磁盘上的真实资源"；它不模拟
  远端 API 的网络分区/部分失败语义。资源身份由对象名确定性派生，跨重启稳定；
  对账只在"控制面无此对象"这一确定依据下删除实际资源，归属不确定的资源一律保留并
  交给调和——宁可多留一轮，绝不误删仍然有效的资源。
- 存储是单进程内存 + 文件快照；它精确模拟了 etcd 风格的单调 rv 与 CAS 语义，
  但不模拟网络分区。一致性论证因此限定在"单控制面、进程内/崩溃重启"模型内。
- 所有时间与抖动都可注入（`Clock`、`jitter func`），失败可注入（`FailureRule`），
  因此异常路径测试是确定性的，而不是依赖真实睡眠或随机。
