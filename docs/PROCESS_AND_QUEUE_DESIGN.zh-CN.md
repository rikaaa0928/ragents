# 双消息队列与宿主命令进程管理系统设计文档

- **文档状态**：Implemented（已实现）
- **关联模块**：`ragents-protocol`、`ragents-apiserver`、`ragents-daemon`、`ragents-core`、`extensions/shell`

---

## 1. 背景与设计目标

在真实工程场景中，Agent 经常需要执行“先在后台拉起一个或多个后端服务（如数据库、API Server、Mock 服务），再运行客户端测试或发起请求，最后查看服务日志并清理进程”的复合工作流。原有的同步阻塞接口 `execute-command-with-timeout` 无法支撑后台长驻进程管理。

同时，当后台进程在 Agent 执行期间自行退出（正常完成或异常崩溃），或者用户在任务运行中途发来实时引导消息时，系统需要一套明确的**双消息队列机制（等待消息队列 + 实时消息队列）**，将异步事件无缝注入 Agent 的 ReAct 循环。

本设计遵循以下核心原则：
1. **最小宿主复杂度**：进程托管统一收口在节点常驻的 `agent-daemon` 中，通过现有的单 Daemon 共享 UDS（`SharedIpcHub`）向 Worker 子进程暴露能力。
2. **安全的默认行为**：`wait` 默认为 `true`（同步阻塞等待完成），`lifecycle` 默认为 `"activation"`（随单次 Activation 结束自动回收）。
3. **整棵进程树干净回收**：使用独立进程组（Process Group / `pgid`）管理后台命令，停止或超时清理时按进程组清理，杜绝 `sh -c` 派生子进程残留为孤儿进程。
4. **与 OS 内核沙箱无缝协同**：后台命令（`wait = false`）同样运行在 macOS `sandbox-exec` / Linux `bwrap` 内核沙箱中，若启动瞬间因沙箱越界报错，同样支持自动捕获受阻路径并发起权限审批。

---

## 2. 统一双消息队列架构设计（`Realtime` 实时队列 vs `Pending` 逐条等待队列）

### 2.1 核心理念与边界收口

`ragent-base`（`ragents-apiserver` + `ragents-core`）对上层业务抽象为 **Session 级双消息队列**：**只要消息进入队列，控制面与执行器即保证自动调度消费直至两队为空**，上层无需关心或手动拼接 `Continuation`/`Normal` 等激活类型（另保留 `POST /api/v1/sessions/{id}/resume` 专用于中断/失败后不追加消息的断点续跑；`Activation` 在内部继续作为执行单元、租约边界、事件批次与观测对象保留）。

两个队列的读写全部持久化在 `api-server` SQLite 的统一表 `session_queue_messages`（`queue_kind IN ('realtime', 'pending')`）中，且每条入队记录均可附带声明式的单次工具开关配置 **`QueueToolPolicy { enabled_tools, disabled_tools }`**（以及展示用 `metadata`）：

```text
  [上层业务 Server / CLI / SDK]                           [agent-daemon 进程监控协程]
         │                                                             │
         │ HTTP REST:                                                  │ WebSocket 隧道:
         │ 1. POST /sessions/{id}/queues/pending   (入等待队列)         │ 后台进程自然退出
         │ 2. POST /sessions/{id}/queues/realtime  (入实时队列)         │ 入队 Realtime
         ▼                                                             ▼
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                        api-server (SQLite: session_queue_messages)                   │
│                                                                                      │
│  ┌───────────────────────────────────────┐  ┌──────────────────────────────────────┐ │
│  │ 1. 实时队列 (queue_kind = 'realtime') │  │ 2. 等待队列 (queue_kind = 'pending') │ │
│  │  - 携带 content + tool_policy         │  │  - 携带 content + tool_policy        │ │
│  │  - 空闲时批量消费建新 Activation      │  │  - 空闲时立刻逐条出队建 Activation   │ │
│  │  - 忙时在 Turn 间隙批量 Drain 注入     │  │  - 忙时等当前 Activation 完成后       │ │
│  │    当前 Activation 并动态刷新工具集    │  │    FIFO 逐条出队开启独立 Activation  │ │
│  └───────────────────┬───────────────────┘  └──────────────────┬───────────────────┘ │
└──────────────────────┼─────────────────────────────────────────┼─────────────────────┘
                       │                                         │
                       │ Turn 间隙 / 退出前 Drain                │ Activation 完成且 Realtime 空时
                       │ 实时覆盖当前 Activation 工具集           │ 严格 POP 队头 1 条（不跨 Activation 继承）
                       ▼                                         ▼
              ┌─────────────────────────────────────┐   ┌──────────────────┐
              │     当前正在运行的 Activation #1     │   │  Activation #2   │
              │  Turn 1 ──► [Drain] ──► Turn 2 ...  │   │  (独立新使命/配置)│
              └─────────────────────────────────────┘   └──────────────────┘
```

### 2.2 双队列调度、工具动态刷新与跨 `Activation` 隔离规则

| 维度 | 1. `Realtime` 实时队列 | 2. `Pending` 等待队列 |
| :--- | :--- | :--- |
| **入队与出队粒度** | 每次消费（空闲启动或运行中 `drain`）**全量批量取出**当前所有实时消息 | 调度器在 `active_activation_id IS NULL`（且 `Realtime` 为空）时**严格逐条出队**（`ORDER BY id ASC LIMIT 1`），1 条消息 = 1 个独立 `Activation` |
| **优先级** | **最高优先**：正在运行中在 `Turn` 间隙/准备结束前立刻吸收；旧 `Activation` 结束瞬间若仍有残留实时消息，优先于 `Pending` 启动消费 | 等当前 `Activation`（含其内部中途吸入的全部 `Realtime` 消息）彻底结束后，再按 FIFO 依次取出下一条独立执行 |
| **工具开关 (`QueueToolPolicy`) 作用域** | 每次 `begin_turn()` / `commit_model_output_with_drain()` `drain` 出实时消息后，按入队 `id ASC` 顺序将其 `disabled_tools` / `enabled_tools` 叠加至当前 `Activation` 的生效禁用集，**立即重新构建 `active_tools` 与 `request_tools`**（本 `Activation` 后续所有 `Turn` 沿用最新工具集） | 新建 `Activation` 仅从 Session 基础工具全集 + **本次出队那 1 条消息自身的 `tool_policy`** 初始化，**绝不继承上一个 `Activation` 的任何禁用/启用状态** |
| **物理安全性** | `ragents-core::runner::prepare_tool_call` 严格基于实时刷新后的 `active_tools` 做白名单执行校验，被禁工具即使模型幻觉输出调用也会被内核拒绝 | 每次入队 / 出队 / 撤回均触发 `SessionEvent::SessionQueueUpdated`，前端/业务侧通过 SSE 即可实时同步队列快照与原子撤回 |

> **典型协同示例（禁言窥屏中途实时解禁）**：上层业务将某条只读窥屏消息以 `disabled_tools: ["chatroom_speak"]` 投入 `Pending`，开启 `Activation #1`；在其运行期间突然收到明确点名消息，业务以 `enabled_tools: ["chatroom_speak"]` 投入 `Realtime` 队列；`runner.rs` 在下一个 `Turn`（或准备退出前）原子 `drain` 该实时消息并实时刷新 `request_tools`，`Activation #1` **无需等待结束**即可在下一轮模型请求中直接使用 `chatroom_speak` 回复；而 `Activation #1` 结束后，下一个独立 `Pending` 任务开启 `Activation #2` 时天然不受先前任何状态影响。

---

## 3. 命令进程管理系统架构设计

### 3.1 为什么进程托管层应该放在 `agent-daemon`？

在 `ragents` 默认的 `SubprocessExecutor` 模式下，每次 `Activation` 运行在一个独立的短生命周期 Worker 子进程（`ragents internal-core-worker`）中，`Activation` 一结束该 Worker 子进程就会退出。

因此，**将命令进程管理器（`ProcessManager`）放在节点常驻的 `agent-daemon` 中（通过 `CoreHost` Trait 经由共享 UDS 暴露给 Worker 子进程）是最简单、最优雅的方案**：
1. **`lifecycle = "session"` 进程天然跨 `Activation` 存活**：即使 `Activation #1` 的 Worker 子进程退出了，后台进程的句柄和 `stdout`/`stderr` 异步日志收集器依然在 `agent-daemon` 中稳健运行；`Activation #2` 启动后可直接查询 `Activation #1` 留下的后台进程。
2. **生命周期清理 100% 可靠**：
   - 当 `Activation` 结束/取消时，`agent-daemon` 自动清理该 `Activation` 下所有 `lifecycle == Activation` 的进程；
   - 当 `Session` 关闭或 `agent-daemon` 退出时，`agent-daemon` 自动清理该 `Session` 下所有 `lifecycle == Session` 的进程。
3. **进程退出通知由 `agent-daemon` 上报至 `api-server`**：`agent-daemon` 的进程监控协程检测到后台进程退出时，通过与 `api-server` 之间的 WebSocket 控制隧道发送 `DaemonToServerMessage::PushRealtimeMessage`，将通知统一写入 `api-server` 的实时消息队列。

---

### 3.2 核心数据结构设计

```rust
/// 后台命令进程生命周期范围
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessLifecycle {
    /// 默认：随当前 Activation 结束（Completed / Failed / Cancelled）自动终止清理
    #[default]
    Activation,
    /// 随当前 Session 存续，不随单次 Activation 结束而停止；仅当 Session 关闭或被主动 kill 时清理
    Session,
}

/// 进程运行状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManagedProcessPhase {
    Running,
    Exited,
    Killed,
    TimedOut,
}

/// 进程输出日志流选择
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessLogStream {
    Stdout,
    Stderr,
    #[default]
    Combined,
}

/// 托管进程状态快照（返回给 Agent）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagedProcessInfo {
    pub process_id: String,          // 如 "proc_01..."
    pub pid: Option<u32>,            // 宿主机真实 OS PID
    pub session_id: SessionId,
    pub activation_id: ActivationId,
    pub command: String,
    pub lifecycle: ProcessLifecycle,
    pub phase: ManagedProcessPhase,
    pub exit_code: Option<i32>,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub duration_ms: u64,
}
```

---

### 3.3 日志缓冲与进程组（Process Group）管理

1. **三路日志采集（`stdout` / `stderr` / `combined`）**：
   - 每个由 `agent-daemon` 托管的进程在启动时，将 `stdout` 和 `stderr` 设为 `Stdio::piped()`。
   - 后台读取协程按行/分块实时读取：
     - 维护独立的 `stdout_buf`、`stderr_buf`，以及按时间戳严格交织的 `combined_buf`（上限可配，如每个进程保留最新 `4MB` 环形缓冲，避免无限打日志撑爆内存）。
   - Agent 查询日志时可指定：
     - `stream`: `"stdout"` | `"stderr"` | `"combined"`（默认 `"combined"`）
     - `tail_lines`: 仅看最新 N 行（如 `100` 行）
     - `offset` / `limit`: 按字节或行偏移增量拉取最新日志。
2. **进程组（Process Group）干净回收**：
   - 在 Unix（macOS / Linux）下，启动子进程时设置 `process_group(0)`（即 `setpgid(0, 0)`），使该命令及其派生的所有子进程处于同一个进程组 `pgid = child_pid`。
   - 当需要停止进程（Agent 主动调用 `stop`、超时、或 `Activation`/`Session` 结束自动回收）时，向整个进程组 `-pgid` 发送 `SIGTERM`，等待短暂宽限期（如 `500ms`）后若仍存活则发送 `SIGKILL`，确保不会留下任何孤儿后台服务。
3. **后台进程的沙箱与权限审批兼容**：
   - 当 `wait = false` 启动后台进程时，`agent-daemon` 会等待一个极短的启动观察窗口（如 `150ms`）：
     - 若命令在 `150ms` 内因 OS 沙箱拦截（`Operation not permitted`）立即退出，则当场走既存の多目录自动审批与重跑闭环；
     - 若命令在 `150ms` 后仍在正常运行（例如后端 Server 已成功监听端口），则立即向 Agent 返回 `{ status: "running", process_id: "proc_...", pid: 12345 }`！

---

### 3.4 后台进程退出时的实时消息通知格式

当一个 `wait = false` 的后台进程在 `Activation` 运行期间自行退出（且不是因为当前 `Activation` 结束被系统自动清理，也不是 Agent 刚刚同步调用了 `process_stop` 主动杀掉的），`agent-daemon` 会自动向该 `Session` 的 **实时消息队列（`RealtimeQueue`）** 插入一条结构化通知：

```text
[System Notification: Background Process Exited]
- process_id: proc_01a13f...
- pid: 54321
- command: uv run python server.py
- status: exited
- exit_code: 1
- duration_ms: 3420
Hint: You can inspect its output using the `process_logs` tool with `process_id: "proc_01a13f..."` and `stream: "combined"` (or `"stdout"` / `"stderr"`).
```

在下一轮 `Turn` 开始前（或者在当前 `Activation` 准备结束前），`AgentCore` 从 `RealtimeQueue` 取出该通知并注入上下文，同时向 `api-server` 发出 `SessionEvent::BackgroundProcessExited` 事件供 CLI 实时展示。

---

### 3.5 WIT 接口与暴露给 Agent 的工具设计

#### 1. WIT `interface host`（保持极简，仅 1 个入口）
将 [ragents/wit/ragent-extension.wit](ragents/wit/ragent-extension.wit) 中的 `execute-command-with-timeout` 替换为统一的宿主进程管理接口：

```wit
package ragent:extension@1.0.0;

interface host {
    /// Unified host command process management entrypoint.
    /// Supports actions: "start", "status", "logs", "stop", "list".
    manage-process: func(request-json: string) -> string;
}
```

#### 2. `extensions/shell` 向 Agent 暴露的工具集

在 `ragent_shell_extension.wasm` 中提供直观、好用的工具供大模型调用：

1. **`shell`（启动/执行命令，支持阻塞与后台异步模式）**：
   - `command` (`string`, 必填)：要执行的终端命令。
   - `wait` (`boolean`, 默认 `true`)：
     - `true`：阻塞等待命令完成，直接返回 `exit_code`、`stdout`、`stderr`（单次输出超过 **2000 行或 50KB** 时由 `extensions/shell` 自动保留尾部并将完整输出落盘至 `.ragents-tmp/`）；
     - `false`：后台异步启动进程，立即返回 `process_id`、真实 `pid` 与当前状态。
   - `lifecycle` (`"activation" | "session"`, 默认 `"activation"`)：
     - `"activation"`：当前 Activation 结束时自动停止并清理；
     - `"session"`：跨 Activation 保持后台运行，直至被主动停止或 Session 关闭。
   - `timeout` / `timeout_seconds` (`number`, 可选，默认 `0` 不限时)：最大运行超时秒数（支持整数或小数秒，如 `0.5`、`10`）。阻塞模式下若触发超时，宿主会按进程组（`-pgid`）清理子进程树，并**完整保留超时前已产生的 `stdout` 与 `stderr`** 返回给模型。
2. **`process_status`（查询单个或全部后台进程状态与返回码）**：
   - `process_id` (`string`, 可选)：指定进程 ID（或 PID）；若省略则列出当前 Session 下所有托管进程的状态、PID、生命周期与 `exit_code`。
3. **`process_logs`（查询后台进程日志）**：
   - `process_id` (`string`, 必填)：目标进程 ID（或 PID）。
   - `stream` (`"stdout" | "stderr" | "combined"`, 默认 `"combined"`)：选择只看标准输出、只看标准错误、或合并输出。
   - `tail_lines` (`integer`, 默认 `200`)：读取最新 N 行日志。
4. **`process_stop`（主动停止后台进程）**：
   - `process_id` (`string`, 必填)：目标进程 ID（或 PID）。
   - 返回停止后的最终状态与 `exit_code`。
