# 内存事件（OOM）的顺序语义与实现

本文说明 embedshim 如何把 cgroup 内存计数器变成有序的任务事件，以及为什么
顺序不依赖两个事件源的到达先后。

## 发布的事件

一个容器对外发布的事件按以下因果关系排列：

```
OOM（0 次或多次） -> Exit（最多 1 次） -> Delete（恰好 1 次，最后）
```

- **OOM**：cgroup 内存计数器的增长。每个 OOM 事件携带 `Deltas`（本次报告
  的 `oom`/`oom_kill` 增量）、`Merged`（合并了几次读取）以及 `OOMFields`
  （内核实际暴露的字段）。
- **Exit**：初始化进程退出。`Reason` 说明退出时的同步读取是否成功；退出
  读取失败（cgroup 已清理、已降级）时仍发布退出，只是原因为非空。
- **Delete**：任务删除，最后一个事件。发布后出口关闭，其后到达的内存通知
  只记日志，不再发布。

出口（`memevents.Outlet`）用一把互斥锁串行化所有入口，因此发布顺序等于
临界区取得锁的顺序。**到达顺序从不直接决定发布顺序。**

## 两个事件源与同步读取

内存通知来自对计数器文件的监听（异步），退出通知来自 pidfd /
`sched_process_exit` 跟踪点（异步）。两者谁先到不确定，因此：

- **发布退出之前，主动同步读一次计数器。** 如果相对基线有增长，先发布
  OOM，再发布退出。这样“退出先到、计数已增长”与“内存事件先到”得到相同
  的对外序列。
- **发布删除之前同样同步读一次。** 删除是最后一次能读到计数器的机会。
  这一步拆成 `PrepareDelete`（读取并发布内存事件）与 `OnDelete`（发布删除
  并关闭出口）：前者在 `runc delete` 之前执行，后者在运行时的删除成功之后
  执行，避免为最终没有删除成功的任务发布删除事件。

```go
// 退出路径（init_process.go setExited -> shim.publishExit -> Outlet.OnExit）
status := sample()                 // 一次 open+read
if delta := status - baseline; delta > 0 {
    publish(OOM, delta)
}
publish(Exit)
```

### 代价与延迟

同步读取是一次小文件的 `open`+`read`（`memory.events` / `memory.oom_control`
通常只有几十字节）：页缓存命中时为个位数微秒，冷缓存时可达数十微秒。只有
在确有增长时才额外写一次基线（见下），因此稳态下退出路径的附加成本基本只有
这一次读。

它运行在观察到退出的那个 goroutine 上，位于退出事件发布**之前**、任何
cgroup 清理**之前**：

- **在退出事件之前**：顺序要求如此，否则同步读到的增长无法排在退出前。
- **在清理之前**：进程退出后 cgroup 可能被立即清理，晚于清理的读取会失败。
  因此 `OnExit` 必须先于 `runc delete` 及 `bundle.Delete`。`shim.Delete`
  也先发布删除事件、再做 `init.Delete`（其中会调用 `runc delete`）。

同步读取阻塞了退出发布，但它只在退出时发生一次，不影响正常运行路径。异步
监听仍然负责长时间运行中、退出之前的 OOM 上报。

## 计数器、基线与边界

计数器的语义：

| 来源 | 触发次数 | 击杀次数 |
| --- | --- | --- |
| cgroup v2 `memory.events` | `oom` | `oom_kill` |
| cgroup v1 `memory.oom_control` | 不提供（`under_oom` 是 0/1 状态） | `oom_kill` |

只有计数**增长**才产生事件。相对基线读取：

1. **第一次监听之前已累计的值不当作新事件。** 没有持久化基线时，第一次
   读取直接成为基线。
2. **重启恢复时，以任务持久化时记录的基线为准。** 持久化基线与本次第一次
   读取之间的增长**不补报**：这些增长发生在无人观察期间，它们与退出事件的
   先后已经无法确定（退出甚至可能已经发生）。基线推进到当前读数。文件
   `memory.events.baseline.json` 位于 bundle 内，与其余任务状态一起持久化。
3. **计数器倒退**（当前 < 上次）表示来源被重建，通常是宿主重启。此时重置
   基线、记一条告警，**不产生事件**；倒退绝不会被换算成负数增量。

其他边界：

- **cgroup 被删除的特殊通知不是内存事件。** inotify 的 `IN_DELETE_SELF`、
  `IN_MOVE_SELF`、`IN_IGNORED` 被映射为 `Gone`；收到后只做标记、停止读取，
  绝不会把消失的计数器当成“降到零”（那会在重置基线后看起来像增长）。
- **字段缺失**：内核未暴露的字段按“不支持”处理——读作零，且**永不产生
  事件**（`FieldSet` 区分“字段存在且为 0”与“字段不存在”）。v1 没有触发
  计数，因此 `OOMFields.OOM` 恒为 false。若运行中字段集合发生变化，静默
  重置基线。
- **基线写入失败、基线文件损坏**：记日志并继续。损坏的基线按“不存在”处理；
  最坏情况是漏报一次增长，而不是伪造事件或让任务失败。

## 降级（建立监听失败）

建立监听失败**不会让容器启动失败**，而是降级并在任务上留下标记
（`memory-events.degraded.json` 与 `Degraded.Marker()`）：

| 情况 | 模式 | 行为 |
| --- | --- | --- |
| 文件不存在 / cgroup 已删 | `disabled` | 不产生内存事件；退出/删除照常 |
| 权限不足 | `disabled` | 同上 |
| 文件无可识别字段 | `disabled` | 同上 |
| 计数可读但监听失败 | `exit-only` | 无异步事件；退出/删除的同步读仍然有效 |

降级不改变退出与删除事件的发布，因此因果顺序仍然成立，只是缺少 OOM。

## 重启恢复与去重

重启恢复时先按 `reload.go` 重建任务：

- 仍为 `running`/`paused` 的任务：重新建立出口，基线以持久化值为准（见上
  文第 2 条）。
- 已为 `stopped` 的任务：它的退出在上一轮运行中已经上报过（或由退出跟踪
  点在重启前上报），出口以“退出已发布”状态创建（`ExitReported`）。之后
  `Delete` 仍会正常发布，但不会出现重复退出。
- 硬重启（宿主重启）导致计数归零时，按“计数器倒退”处理，不补报。

## 合并（coalescing）

短时间内计数增长多次时有两种策略：

- **逐个发布**（`MergeWindow = 0`）：每次增长一个事件，保留时间精度。
- **合并发布**（`MergeWindow > 0`）：窗口内累计增量，发布一个带 `Merged`
  次数的事件。

取舍：合并减少事件数量、对使用者更友好，但丢失每次增长的时刻，也无法从
事件本身恢复增长节奏；逐个发布保留时间精度，却可能在内存抖动时产生事件
风暴。

无论选择哪种，**退出与删除路径都会先同步冲刷窗口内待发布的事件**，所以合
并窗口不会把 OOM 推迟到退出之后。窗口只在异步路径上生效。

## 与 cgroup 两代的接口

两代 cgroup 的差异完全封装在 `Source` 之后：

```go
type Source interface {
    Sample() (Sample, error)   // 立即读取计数器
    Watch() (Watch, error)     // 建立异步监听
    Generation() Generation
}
```

- **v2**：监听 `memory.events` 的 inotify `IN_MODIFY`，字段 `oom`/`oom_kill`。
- **v1**：监听 `memory.oom_control`。cgroup v1 原生的 OOM 通知是
  `cgroup.event_control` 注册的 eventfd，而不是该文件本身的修改，因此 inotify
  之外**附加周期重采样**（`DefaultRescanInterval`）作为兜底；字段只有
  `oom_kill`。

统一接口的映射规则：缺失字段 → 零值 + 不支持；cgroup 删除通知 → `Gone`；
读失败 → 分类后降级。

## 测试

`pkg/memevents` 的测试用替身覆盖两个事件源：

- `TestScenarioMemoryFirst`：内存事件先到 → `OOM, Exit, Delete`。
- `TestScenarioExitFirstButCountersGrew`：退出先到、同步读发现增长 →
  `OOM, Exit, Delete`，迟到的通知不再发布。
- `TestScenarioExitAfterCgroupRemoved` / `TestScenarioExitAfterWatchReportedGone`：
  cgroup 已清理 → 只有 `Exit, Delete`，`Reason` 说明原因。
- `TestScenarioMemoryAfterDelete`：删除之后的通知只记日志。
- `TestScenarioThreeIncreasesIndividual` / `TestScenarioThreeIncreasesMerged`：
  短时间增长三次，分别断言逐个发布与合并（`Merged == 3`）。
- `TestRandomArrivalOrder`：300 轮随机到达顺序，每轮都断言 OOM 不晚于
  退出、退出唯一且早于删除、删除唯一且最后、无事件泄漏到删除之后，并核对
  上报的增量落在合法区间内。
- `TestFileSourceWatch`：真实文件上的 inotify 修改与删除通知。

全部测试可离线运行，不需要内核跟踪点或真实 cgroup。
