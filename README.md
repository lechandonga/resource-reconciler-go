# resource-reconciler-go

一套**本地可运行的声明式资源控制器**。控制面对象保存在进程内、并用原子写入的
JSON 文件持久化；数据面副作用（实例）同样持久化且全部幂等。系统演示了在
**多版本、并发、重启**场景下，期望状态、实际状态、准入校验与失败处理如何给出
**一致且可解释**的收敛结果。

- 无外部依赖：仅使用 Go 标准库。
- 所有状态落盘到一个临时/指定目录，重启后从已持久化状态继续。
- 全部异常路径测试可在竞态检测（`-race`）下重复运行。

---

## 1. 快速开始

```bash
# 编译
go build ./...

# 运行端到端演示：准入拒绝 / 版本升级 / 退避 / 有序依赖 / 重启 / 有序删除
go run ./cmd/demo

# 全部测试（普通模式）
go test ./... -count=1

# 竞态检测（需要 cgo；本仓库测试设计为可复现、不依赖时序碰运气）
CGO_ENABLED=1 go test ./... -race -count=3

# 只跑异常路径集成测试并查看输入/判定/最终状态日志
CGO_ENABLED=1 go test ./test/ -race -count=1 -v
```

测试日志通过注入的 `Logger` 打印三类关键信息，满足可追溯要求：

- **输入**：`event=admission input=...`、`event=reconcile_start input=Workload{...}`
- **判定**：`event=reconcile_decision reason=<原因码> requeue=<bool>`
- **最终状态**：同一行的 `after=Workload{phase, gen, observed, attempts, instances, deleting}`

---

## 2. 模块与代码地图

| 包 | 职责 |
|---|---|
| `pkg/apis/core/v1` | 当前稳定版类型、阶段（Phase）、状态条件（Condition）、稳定原因码常量 |
| `pkg/apis/core/v1beta1` | 历史版本线格式（`replicaCount/containerImage/depends`） |
| `pkg/apis/core/defaults` | 确定性、幂等的默认值填充 |
| `pkg/apis/core/validation` | 规格校验，每种非法输入对应**可区分**的原因码 |
| `pkg/apis/core/convert` | 版本解码/升级；未知版本与未知字段**显式拒绝** |
| `pkg/admission` | 准入链：解码升级 → 默认值 → 校验 |
| `pkg/storage` | 文件持久化 + 单调 `resourceVersion` + 乐观并发（OCC） |
| `pkg/dataplane` | 幂等、可注入失败的模拟数据面 |
| `pkg/queue` | 有界、按 key 合并（coalescing）的工作队列 |
| `pkg/reconciler` | 单对象、单遍、有界的收敛逻辑与失败分类 |
| `pkg/controller` | worker、key 级互斥、FIFO 提交串行化、退避重排、重启恢复 |
| `test/` | 以异常路径为主的集成测试（非法规格/版本/并发/失败/重启） |
| `cmd/demo` | 本地自包含演示 |

---

## 3. 收敛语义（Reconciliation Semantics）

### 3.1 声明式与幂等

- **偏离即修正**：实际状态（数据面实例集合）与期望（`spec.replicas/image`）做 diff，
  多退少补。
- **重复施加同一期望无重复副作用**：数据面 `EnsureInstance(id,image,generation)`
  在 `(id,image,generation)` 完全相同时是空操作；删除不存在的实例也是空操作。
- **已符合期望不被改写**：规格未变的重复 `Submit` 直接返回，**不 bump
  generation、不增加 resourceVersion、不触发数据面调用**；收敛遍若完全一致，
  判定原因为 `Noop` 且不重排队列。
- **实例寻址稳定**：实例以 `(workload, index)` 寻址，扩缩容时确定性地增删
  固定下标，避免"重建一切"。

### 3.2 有序生命周期（创建 / 更新 / 删除）

- **创建顺序**：依赖必须先 `Available`（`observedGeneration == generation`），
  被依赖方才允许创建数据面实例；否则停在 `Progressing/Degraded`，不产生副作用。
- **更新顺序**：规格变更 bump `generation`；实例按最新 generation 更新，
  依赖未就绪时不更新。
- **删除顺序（finalizer）**：
  1. 只要还有**非删除中**的对象依赖本对象，删除被阻塞
     （条件 `Finalizer=False, reason=FinalizerBlocked`）；
  2. 数据面子实例按下标**从高到低**、每遍有界地删除；
  3. 数据面清空后才在同一个 OCC 保护下物理删除控制面对象。
  因此**不存在半完成状态，也不存在孤儿数据面资源**。
- 删除一个依赖中的对象会事件式唤醒相关方（依赖方与被依赖方），不依赖轮询。

### 3.3 单遍工作量有界

- 单次收敛最多执行 `reconciler.MaxDataPlaneOps = 3` 个数据面变更，超出则记录
  `ReconcileBudgetExceeded` 并再排一遍。
- 扩容到上限（10 个副本）需要多遍完成，单遍工作量恒定，不随规格规模增长。

---

## 4. 准入规则与默认值（Admission）

准入链为：**解码/版本升级 → 默认值填充 → 校验**。

### 4.1 默认值（确定且稳定）

| 字段 | 缺省值 |
|---|---|
| `spec.replicas` | `1`（合法区间 1..10） |
| `spec.image` | `busybox:latest` |
| `spec.dependsOn` | `[]` |
| `spec.config` | `{}` |

- 默认值在准入时填充并打上 `core.example.com/defaults-version` 注解；
- `Apply` 是**幂等**的：同一输入任意次读取结论一致；历史对象每次读取都会被
  确定性地补齐默认语义；
- 显式提供的值永不被默认值覆盖。

### 4.2 校验原因码（可区分的拒绝原因）

| 原因码 | 触发条件 |
|---|---|
| `InvalidSpec` | 非 JSON、字段类型错误 |
| `UnsupportedVersion` | `apiVersion` 不受支持（含未来版本 `v2`、空版本、其它 group） |
| `InvalidKind` | `kind` 缺失或不是 `Workload` |
| `MissingName` / `InvalidName` | 名称缺失 / 非小写 DNS 标签 |
| `ReplicasOutOfRange` | 副本数不在 1..10 |
| `InvalidImage` | 镜像为空、含空白、tag 非法等 |
| `DuplicateDependency` / `EmptyDependency` | 依赖重复 / 含空项 |
| `InvalidConfigKey` | config 键非法 |
| `UnknownField` | 顶层 / metadata / spec 出现该版本未声明的字段 |

非法对象**不会**进入存储，也不会触发任何数据面副作用。

### 4.3 版本演进兼容

- 支持 `core.example.com/v1`（当前）与 `core.example.com/v1beta1`（历史）。
- v1beta1 字段 `replicaCount/containerImage/depends` 在准入时**升级**为 v1，
  原始受理版本记录在 `core.example.com/stored-api-version`。
- **不静默忽略、不猜测**：
  - 未来/未知版本 → `UnsupportedVersion`；
  - 在 v1beta1 清单里写 v1 字段（或反之）→ `UnknownField`；
  - 磁盘上带未知版本/字段的对象在加载边界 `convert.DecodeStored` 处显式报错。

---

## 5. 并发与乐观并发控制

- **同对象不被并发处理**：每个 key 有独立互斥锁；`-race` 测试通过数据面屏障
  断言同一 workload 任意时刻最多一个操作在执行。
- **乐观并发（OCC）**：存储为每次写入分配单调递增的 `resourceVersion`；
  `Update/Delete` 必须带期望值，过期写回返回 `storage.ErrConflict`，
  **绝不覆盖较新状态**。收敛写回冲突后立即基于最新状态重读重试，最终收敛到
  最新 generation。
- **提交串行化（FIFO 票据锁）**：同一 key 的客户端 `Submit/Delete` 按调用顺序
  串行完成读-改-写。因此：
  - 一个成功返回的 `Submit`，其规格必然已持久化；
  - **按确定顺序**发出的多次更新，最后一次返回时落盘的就是它的规格
    （见 `TestLatestOrderedSubmitWins`）；
  - **同时到达**的并发写本身没有外部可定义顺序，系统保证的是：不丢不重、
    generation 连续无重复、数据面精确等于"最后被串行化的那个规格"而不是
    混合态（见 `TestConcurrentWritesPreserveIntegrity`）。
- 与收敛并发更新规格时，正在跑的旧收敛若写回过期 RV 会被拒绝并重试，
  结果收敛到最新版本。

---

## 6. 失败分类、退避与重启

### 6.1 失败分类

| 类别 | 例子 | 行为 |
|---|---|---|
| 瞬时 `ClassTransient` | 模拟抖动 `FlakyAPI/DownAPI` | 有界指数退避 + 抖动重试 |
| 确定性 `ClassDeterministic` | 镜像 `*/bad`、`*:broken`（`ImageImmutableError`） | 立即置 `Failed`，**不再重试、不空转**，作为可观察状态暴露 |

- 退避：`base=10ms`，指数增长，上限 `max=2s`，±20% 抖动；
- 每 generation 最多 `MaxAttempts=5` 次瞬时重试，超限置
  `Failed / BackoffLimitExceeded`（可观察、可区分，不再无限重试）；
- **规格变更（新 generation）会重置重试预算**，新规格不被旧失败计数卡死；
- **依赖缺失**属于确定性条件，采用事件驱动等待，**不做定时轮询**，避免空转；
  依赖提交、可用性变化、删除或进程重启都会唤醒等待方。

### 6.2 可观测状态

每个对象暴露：

- `status.phase`：`Pending / Progressing / Available / Degraded / Failed / Deleting`；
- `status.conditions[]`：`Admitted / Reconciled / DependenciesReady / Finalizer`，
  带 `status/reason/message/observedGeneration/lastTransition`，状态未变时
  `lastTransition` 保持稳定，判定依据可追溯；
- `status.attempts`、`status.lastError{reason,message,transient,time}`。

### 6.3 重启恢复

- 控制面与数据面都通过"临时文件 + fsync + 原子 rename"持久化，重启看到的
  恰好是最后一次被确认的状态。
- 启动时列出所有未终结对象并入队继续；数据面操作幂等，因此
  **已生效的副作用不会重复执行**（重启测试断言总 `ensure` 次数不增加、
  已收敛对象的 RV 不变化）。
- 创建中途、删除中途（被依赖阻塞）崩溃后，重启能继续完成且不留孤儿。
- 崩溃前已耗尽瞬时重试预算的对象，重启后获得新预算，数据面恢复时即可收敛。

---

## 7. 工作量 / 耗时 / 内存上限

- **有界队列**：每个 key 至多一个排队项（coalescing），处理中到达的变更折叠为
  一个 dirty 标记、`Done` 后重排一次；规格风暴不会造成无界排队或重试风暴。
- **容量拒绝**：队列满时 `Add` 返回可区分的 `queue.ErrDropped`，控制面记录
  `QueueShed` 而不是无限阻塞或内存增长。
- **有界单遍**：每遍数据面操作数恒定（`MaxDataPlaneOps`）。
- **有界提交重试**：提交冲突的读-改-写重试有上限（25 次），超限以 `Conflict`
  显式拒绝。
- **有界退避与重试**：退避有上限、重试次数有上限，确定性失败零重排。

---

## 8. 测试矩阵（以异常路径为主）

| 测试 | 覆盖点 |
|---|---|
| `TestIllegalSpecsAreRejected` | 14 类非法规格，逐一断言具体原因码 |
| `TestDefaultsAreDeterministicAndStable` | 默认值稳定、重复读取一致、重复提交不改写 |
| `TestV1Beta1*` / `TestUnsupportedVersion*` / `TestPersistedObject*` | 历史版本升级、默认回填、未知版本/字段拒绝 |
| `TestDeclarativeConvergence` | 漂移修正、幂等、无谓不改写、按 generation 更新 |
| `TestOrderedLifecycleAndDeletion` | 先依赖后依赖方、删除阻塞、有序删除、无孤儿 |
| `TestDependencyCycleRejectedAtRuntime` | 运行期成环的确定性失败 |
| `TestReconcileBudgetIsBounded` / `TestLargeScaleOutUsesBoundedBudget` | 单遍有界 |
| `TestConcurrentWritesPreserveIntegrity` / `TestLatestOrderedSubmitWins` | 并发完整性、最新版本收敛 |
| `TestSameObjectNotProcessedConcurrently` | key 级互斥（屏障检测） |
| `TestOptimisticConcurrencyAtStoreBoundary` | 过期写回被拒、不覆盖新状态 |
| `TestTransient*` / `TestDeterministic*` / `TestSpecChangeResetsRetryBudget` | 退避、确定性失败不空转、预算重置 |
| `TestRestart*`（4 个） | 不重复副作用、中途继续、删除继续、预算重置 |
| `TestQueue*` | 有界拒绝、合并、dirty 重排、并发安全 |
| 包内单测 | 默认值、校验、存储 OCC/重载、数据面幂等与失败不留痕 |

复现命令：

```bash
CGO_ENABLED=1 go test ./... -race -count=10
```

---

## 9. 设计取舍说明

- 模拟数据面把"镜像不可变错误"编码在镜像名约定里（`*/bad`、`*:broken`），
  用于确定性地演示失败分类；瞬时失败通过可注入 `Hook` 模拟。
- 持久化采用单文件整库原子写以简化"确认即落盘"语义；对象规模增大时可替换为
  WAL/分键存储，`storage.Store` 接口保持不变。
- "同时并发写无全局顺序、有序提交最新者胜"是刻意与真实分布式系统对齐的语义，
  测试分别断言这两条而非要求对同时到达的写入强加人为先后。
