# 双消息队列与宿主命令进程管理系统设计文档

- **文档状态**：Draft（待评审）
- **关联模块**：`ragents-protocol`、`ragents-apiserver`、`ragents-daemon`、`ragents-core`、`extensions/shell`

---

## 1. 背景与设计目标

在真实工程场景中，Agent 经常需要执行“先在后台拉起一个或多个后端服务（如数据库、API Server、Mock 服务），再运行客户端测试或发起请求，最后查看服务日志并清理进程”的复合工作流。原有的同步阻塞接口 `execute-command-with-timeout` 无法支撑后台长驻进程管理。

同时，当后台进程在 Agent 执行期间自行退出（正常完成或异常崩溃），或者用户在任务运行中途发来实时引导消息时，系统需要一套明确的**双消息队列机制（等待消息队列 + 实时消息队列）**，将异步事件无缝注入 Agent 的 ReAct 循环。

本设计遵循以下核心原则：
1. **最小宿主复杂度**：进程托管统一收口在节点常驻的 `agent-daemon` 中，通过现有的单 Daemon 共享 UDS（`SharedIpcHub`）向 Worker 子进程暴露能力。
2. **向后兼容的默认行为**：`wait` 默认为 `true`（同步阻塞等待完成），`lifecycle` 默认为 `"activation"`（随单次 Activation 结束自动回收）。
3. **整棵进程树干净回收**：使用独立进程组（Process Group / `pgid`）管理后台命令，停止或超时清理时按进程组清理，杜绝 `sh -c` 派生子进程残留为孤儿进程。
4. **与 OS 内核沙箱无缝协同**：后台命令（`wait = false`）同样运行在 macOS `sandbox-exec` / Linux `bwrap` 内核沙箱中，若启动瞬间因沙箱越界报错，同样支持自动捕获受阻路径并发起权限审批。

---

## 2. 双消息队列架构设计（等待队列 vs 实时队列）

### 2.1 当前版本现状说明

在当前版本的代码中，消息队列的实现状态如下：
1. **等待消息队列（`FollowUp / Activation Queue`）—— 已在 `api-server` 实现**：
   - 当一个 `Session` 正在运行 `Activation #1` 时，新提交给该 `Session` 的任务会在 SQLite `activations` 表中以 `phase = Queued` 入队（`SessionStatus.queued_activation_count` 递增）。
   - 当 `Activation #1` 结束（`CompleteActivation`）时，调度器在同一个事务内自动按 FIFO 顺序取出下一个 `Queued` 的 `Activation #2` 投入运行，相当于“等当前任务完成后自动续上下一个 Activation”。
2. **实时消息队列（`Realtime / In-Loop Queue`）—— 协议已预留 `SubmissionKind::Steer`，但 ReAct 循环内尚未打通每轮读取**：
   - 当前 `runner.rs` 的 ReAct 循环在每轮 `Turn` 开始前仅读取已提交的历史上下文，尚未在每轮 `Turn` 边界从实时队列拉取（Drain）新到达的异步通知（如后台进程退出通知、用户运行中实时插话）。

### 2.2 补齐后的双队列完整工作流（统一由 `api-server` 持久化托管）

**是的，两个队列的写入与出队全部统一收口在控制面 `api-server` 的 SQLite 数据库中**：
- **客户端（CLI / SDK）**：通过 HTTP REST 接口调用 `api-server`，向等待队列或实时队列写入消息；
- **`agent-daemon`（进程托管方）**：当它监控到后台命令进程（`wait = false`）自行结束时，**由 `agent-daemon` 通过现有的 WebSocket 控制隧道（`DaemonToServerMessage`）主动调用 `api-server`**，将进程退出通知写入 `api-server` 的实时消息队列；
- **`agent-core`（执行内核）**：在每轮 ReAct Loop（每个 `Turn` 开始前及循环结束前）通过 `CoreHost::drain_realtime_messages` 请求 `api-server` 原子取出（Drain）实时队列中的消息并落盘为上下文 `ItemBatch`。

```text
  [CLI / SDK 客户端]                                       [agent-daemon 进程监控协程]
         │                                                             │
         │ HTTP REST:                                                  │ WebSocket 隧道:
         │ 1. 提交等待队列 (FollowUp)                                   │ 检测到后台进程 (wait=false) 退出
         │ 2. 提交实时队列 (Steer / Realtime)                           │ 主动上报 PushRealtimeMessage
         ▼                                                             ▼
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                              api-server (SQLite 单一事实源)                            │
│                                                                                      │
│  ┌───────────────────────────────────────┐  ┌──────────────────────────────────────┐ │
│  │ 1. 实时消息队列 (realtime_messages 表) │  │ 2. 等待消息队列 (activations 表)      │ │
│  │  - 后台进程退出通知 (由 daemon 上报)   │  │  - phase = Queued 的后续任务         │ │
│  │  - 用户实时插话 (由 client 提交)       │  │  - 等当前 Activation 完成后自动调度   │ │
│  └───────────────────┬───────────────────┘  └──────────────────┬───────────────────┘ │
└──────────────────────┼─────────────────────────────────────────┼─────────────────────┘
                       │                                         │
                       │ 每次 ReAct Loop (Turn) 开始前            │ 等当前 Activation #1
                       │ 通过 DrainRealtimeMessages 原子消费      │ 彻底完成后自动续接
                       ▼                                         ▼
              ┌─────────────────────────────────────┐   ┌──────────────────┐
              │     当前正在运行的 Activation #1     │   │  Activation #2   │
              │  Turn 1 ──► [Drain] ──► Turn 2 ...  │   │  (自动接力启动)   │
              └─────────────────────────────────────┘   └──────────────────┘
```

#### 两个队列的职责边界与调用链路对比

| 维度 | 1. 实时消息队列（`Realtime Queue`） | 2. 等待消息队列（`FollowUp Queue`） |
| :--- | :--- | :--- |
| **存储位置** | `api-server` SQLite (`realtime_messages` 表) | `api-server` SQLite (`activations` 表，`phase = Queued`) |
| **谁调用 `api-server` 写入？** | ① **`agent-daemon`**：监控到 `wait=false` 后台进程退出时，通过 WS 隧道发送 `PushRealtimeMessage` 给 `api-server`<br>② **客户端 (CLI/SDK)**：通过 HTTP `POST /api/v1/sessions/{id}/realtime`（或 `SubmissionKind::Steer`）发送给 `api-server` | **客户端 (CLI/SDK)**：通过 HTTP `POST /api/v1/sessions/{id}/activations`（`SubmissionKind::Normal / FollowUp`）发送给 `api-server` |
| **谁调用 `api-server` 读取？** | **`agent-core` -> `agent-daemon`**：在每次 ReAct Loop（每个 `Turn` 调用大模型前）发送 `DrainRealtimeMessages` 给 `api-server` | **`api-server` 内部调度器**：在当前 `Activation` 完成（`CompleteActivation`）的同一个事务内自动取出下一个 `Queued` Activation 派发给 `agent-daemon` |
| **边界情况：当最后一次 Tool 执行完、模型正准备结束 Activation 时收到实时消息** | 在 ReAct 循环准备 `break` 退出前，会再做一次 `drain_realtime_messages()` 检查：若 `api-server` 实时队列非空（例如后台进程刚好在最后一刻退出），**自动继续下一轮 `Turn`** 让模型处理该通知，绝不漏消息！ | 若 `Session` 当前处于空闲期（无运行中 `Activation`）时某 `lifecycle=session` 的后台进程退出，`agent-daemon` 同样上报给 `api-server` 存入 `realtime_messages` 表，在下一次 `Activation` 启动的第 1 个 `Turn` 自动被读取。 |

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

1. **`shell`（启动/执行命令，兼容原有习惯并扩展异步能力）**：
   - `command` (`string`, 必填)：要执行的终端命令。
   - `wait` (`boolean`, 默认 `true`)：
     - `true`：阻塞等待命令完成，直接返回 `exit_code`、`stdout`、`stderr`；
     - `false`：后台异步启动进程，立即返回 `process_id`、真实 `pid` 与当前状态。
   - `lifecycle` (`"activation" | "session"`, 默认 `"activation"`)：
     - `"activation"`：当前 Activation 结束时自动停止并清理；
     - `"session"`：跨 Activation 保持后台运行，直至被主动停止或 Session 关闭。
   - `timeout_seconds` (`integer`, 默认 `1800`)：最大运行超时秒数（`0` 表示不限时）。
2. **`process_status`（查询单个或全部后台进程状态与返回码）**：
   - `process_id` (`string`, 可选)：指定进程 ID（或 PID）；若省略则列出当前 Session 下所有托管进程的状态、PID、生命周期与 `exit_code`。
3. **`process_logs`（查询后台进程日志）**：
   - `process_id` (`string`, 必填)：目标进程 ID（或 PID）。
   - `stream` (`"stdout" | "stderr" | "combined"`, 默认 `"combined"`)：选择只看标准输出、只看标准错误、或合并输出。
   - `tail_lines` (`integer`, 默认 `200`)：读取最新 N 行日志。
4. **`process_stop`（主动停止后台进程）**：
   - `process_id` (`string`, 必填)：目标进程 ID（或 PID）。
   - 返回停止后的最终状态与 `exit_code`。
