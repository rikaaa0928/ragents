# AGENTS.md — 开发规范与架构约束

## 1. 严禁向前兼容代码（No Backward Compatibility）
- **当前项目处于快速迭代开发阶段，所有部署与运行环境均为测试环境。**
- **开发过程中严禁编写任何向前兼容（Backward Compatibility）或历史遗留兜底逻辑**，包括但不限于：
  - 数据库初始化或运行时的 `ALTER TABLE ... ADD COLUMN`、schema 版本对比与增量迁移脚本、历史字段回填（backfill）SQL；
  - 协议/配置结构体中的旧字段别名（如 `#[serde(alias = "...")]`）或废弃字段兼容解析；
  - 业务逻辑或前端 UI 中针对旧版缺失字段的回退推断（fallback inference）。
- **可接受数据丢失**：当数据表结构、协议字段或存储格式发生变更时，直接修改主建表 DDL / 数据结构定义；若测试环境中的历史 SQLite 数据或旧会话结构不匹配，直接清理或重建数据库即可，无需保留历史脏数据兼容路径。

## 2. `api-server` 唯一事实源（SSOT）与 `NodeSpec` 基础/增量分层
- **`api-server` 是集群唯一的配置与权限事实源（Single Source of Truth）**：
  - 集群级提示词（`store_meta.cluster_system_prompt`）、节点级提示词与权限（`NodeSpec` / `NodeStatus`）、会话级提示词与权限（`SessionSpec.system_prompt`、`SessionSpec.permissions`）以及激活级工具开关（`ActivationSpec.disabled_tools`）全部持久化在 `api-server` 中。
  - 权限审批（`AllowSession`、`AllowWorkspace`、`AllowNode`）在 `api-server` 处理 `patch_activation` 时直接落盘至 `SessionSpec` 与 `NodeSpec`，节点重启后权限不丢失。
- **`NodeSpec` 基础配置与控制面增量分离 + `NodeStatus` 最终生效视图**：
  - 每次 `agent-daemon` 启动注册（`RegisterNode`）时，用本地 `daemon.toml` 的最新配置刷新 `NodeSpec.node_prompt` 与 `NodeSpec.base_permissions`（以及 `NodeStatus` 中的模型列表、扩展列表、允许工作区目录）。
  - 运行时通过控制面追加的 `NodeSpec.granted_permissions`（`AllowNode`）、`NodeSpec.workspace_permissions`（`AllowWorkspace`）与可选 `NodeSpec.node_prompt_override` 跨 Daemon 重启持久保留。
  - `api-server` 自动计算合并结果并维护在 `NodeStatus.effective_node_prompt` 与 `NodeStatus.effective_permissions` 中供全局统一消费。
  - `agent-daemon` 本地仅独享保管不落控制面的敏感配置：模型 `base_url`、`api_key` 及本地 `.wasm` 插件二进制路径。

## 3. 四段式 System Prompt (`PromptParts`) 与三层扩展边界
- **四段式结构化 System Prompt (`PromptParts`)**：
  1. `system_prompt`：集群全局级系统提示词（`api-server` 托管）；
  2. `node_prompt`：节点环境级提示词（`NodeSpec.node_prompt`）；
  3. `client_prompt`：会话/角色人设提示词（`SessionSpec.system_prompt`，支持随时声明式修改，下一轮 Activation 立即生效）；
  4. `extension_prompts`：`agent-core` WASM 扩展在 `agent.prepare` 阶段追加的协议/工作区指令（固定 `agents_md` 排在首位，其余按扩展顺序排列）。
  - 在 `agent.prepare` 完成后，`agent-core` 必须通过 `ReportPreparedConfig` 将组装好的 `PromptParts`、`effective_system_prompt` 与过滤 `disabled_tools` 后的 `effective_tools` 实时回写至 `api-server`（更新 `ConfigRevision` 与 `SessionStatus`），保证控制面、UI 透视面板与发往大模型的 `instructions` / `tools` 100% 一致。
- **收窄 `agent-core` WASM 扩展钩子**：
  - `agent.prepare` 仅允许返回只读追加的 `ExtensionPrepareOutput { append_system_prompt, tools }`，严禁读取或覆写上层 `system_prompt` 与 `model`。
  - 严禁在 `agent-core` 中引入中途篡改用户 `input`、历史消息或模型请求体的 Transform 钩子；单次激活的工具禁用统一通过控制面 `ActivationSpec.disabled_tools` / `PushRealtimeMessageRequest.disabled_tools` 声明。
