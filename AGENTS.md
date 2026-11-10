# AGENTS.md — 核心设计理念与架构约束

## 1. 严禁向前兼容代码（No Backward Compatibility）
- 所有环境均为快速迭代测试环境，**严禁编写任何向前兼容或历史兜底逻辑**（禁止 `ALTER TABLE`、增量迁移脚本、数据回填 SQL、`#[serde(alias = "...")]` 或前后端旧字段回退推断）。
- 数据表或协议结构变更时直接修改主 DDL 与结构体定义，测试库不兼容时直接清理重建。

## 2. 极简控制面与插件业务自治（Lean Control Plane & Plugin Autonomy）
- **收敛闭合原语，放弃 CRD 扩展**：`ragents-apiserver` 仅对固定闭合的核心执行原语（`Node`、`Session`、`Activation`、`ItemBatch`、`SessionEvent`、`RealtimeQueue`）做 SSOT，不引入 CRD/Admission 等重型机制；上层业务自建独立的领域 SSOT Server（如 `ragents-chat`）。
- **`ragents-apiserver` 保持纯粹精简**：仅负责状态协调、双模调度、租约巡检与 SSE 广播；不依赖 `ragents-core`/`wasmtime`/`reqwest`，不承载或转发任何高频业务状态或文件二进制/Base64 数据。
- **`ragents` 核心零业务耦合**：严禁在 `protocol`/`apiserver`/`daemon`/`core` 中硬编码聊天室、文件管理（`NodeFs*`）或特定内置工具名（如 `"web_search"`）等上层业务概念。
- **插件直连上层服务**：上层业务（如 `ragents-chat` 的发言回调、节点目录浏览、文件上传/下载）统一由 `.wasm` 插件（通过 `tools.call` 或通用 `POST /api/v1/nodes/{node_id}/extensions/call` $\rightarrow$ `node.call`）闭环实现，插件内通过 `wasi:http` 直连上层服务传输数据，完全绕过 `api-server`。

## 3. `api-server` 唯一事实源（SSOT）与 `NodeSpec` 基础/增量分层
- 集群/节点/会话级提示词、权限策略与激活级 `disabled_tools` 全部以 `api-server` 为唯一事实源持久化。
- `NodeSpec` 分离 Daemon 启动上报的基础配置（`node_prompt`、`base_permissions`）与控制面动态增量（`granted_permissions`、`workspace_permissions`、`node_prompt_override`），由 `api-server` 合并至 `NodeStatus`；`agent-daemon` 本地仅独享保管模型 `base_url`、`api_key` 与本地 `.wasm` 路径。

## 4. 四段式提示词、三层目录沙箱与收窄的扩展边界
- **四段式 `PromptParts`**：按 `1. system_prompt`（集群）$\rightarrow$ `2. node_prompt`（节点）$\rightarrow$ `3. client_prompt`（会话）$\rightarrow$ `4. extension_prompts`（扩展只读追加，`agents_md` 居首）组装，并在 `agent.prepare` 后通过 `ReportPreparedConfig` 实时回写 `api-server`。
- **三层受控目录**：默认路径白名单仅放行 `workspace_root`（`.`）、`extra_tmp_dirs`（房间共享临时目录）与 `session_tmp_dir`（`$TMPDIR`），不默认放行全局 `/tmp`。
- **收窄 WASM 钩子**：`agent.prepare` 仅允许返回只读追加的 `ExtensionPrepareOutput { append_system_prompt, tools }`，严禁覆写上层提示词/模型或篡改历史消息。
