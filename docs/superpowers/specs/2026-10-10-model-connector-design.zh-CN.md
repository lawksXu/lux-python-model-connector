# Lux Model Connector 设计

[英文原文](2026-10-10-model-connector-design.md)

Model connector 为外部图片和视频生成服务提供一套具有明确类型的内部协议。它持久化接收生成请求，恢复中断的执行，并将执行结果可靠地递交给 Java。调用方选择模型；connector 校验请求契约并负责供应商集成。

服务职责边界和可靠性方向已于 2026-10-10 确认。2026-10-11，执行生命周期简化为四种状态，暂定由 connector 管理执行持久化，并选定初版由每个 worker 独立轮询。2026-10-12，本设计与已实现的本地 Java/Python Session 执行契约 v2 对齐，包括生成消息信封、Task 终态输入，以及 Java 导入确认和失败边界。Connector 已实现具有明确类型的 v2 输入处理、MySQL 持久化、Kafka 接收与 Outbox 交付、可选的 BytePlus Seedream/Seedance 执行、独立轮询，以及 S3 原始输出保留。供应商执行要求显式启用并完整配置凭证和存储；真实验收仍须单独进行。

## 当前跨服务基线

Java 后端和 Python agent 已有本地 v2 实现。Connector 集成边界以当前契约和源文件为依据，不再以历史 v1 计划为依据：

| 参考资料 | 相关基线 |
| --- | --- |
| Java [Session 执行契约 v2](../../../../lux-java-backend/docs/api-contract-v2.md) | 当前 Task HTTP 路由、扁平生成命令、四状态回调，以及导入确认。 |
| Java [生成命令](../../../../lux-java-backend/src/main/java/com/lux/backend/infrastructure/CommandPayload.java)和[生成事件解码器](../../../../lux-java-backend/src/main/java/com/lux/backend/infrastructure/EventProtocol.java) | 已实现的 v2 字段名，以及独立的遗留回调适配。 |
| Python agent [README](../../../../lux-python-agent/README.md)和[Task HTTP 边界](../../../../lux-python-agent/src/lux_agent/backend.py) | Session 执行、原操作核实、不可变工具结果，以及显式 Task 查询。 |
| Java [Session 存储设计](../../../../lux-java-backend/docs/superpowers/specs/2026-10-09-session-run-turn-item-storage-design.md) | 业务归属、独立输入 Item、执行投影，以及新的 v2 存储要求。 |

本地实现基线包含 Java/Python v2 契约，以及 connector 的持久化接收、交付和供应商执行路径。有效消息信封将原请求归档为具名 `ArchivedGenerationRequest`，记录 `ACCEPTED` 和空结果，并在确认 Kafka 消费前持久化接收观察。`LUX_CONNECTOR_PROVIDER_EXECUTION_ENABLED=false`（默认值）时，处理停在这里。启用后，worker 将归档解码为 `SeedreamRequest` 或 `SeedanceRequest`，持久化任何校验失败，并执行准备好的原始请求。`ConnectorStore.transition(...)` 和 `replay_terminal(...)` 是内部存储操作，不是 HTTP 生成接口。仅配置凭证不会启用生成。

Connector 使用单独预先创建的 MySQL Schema 和通过配置启用的 Kafka worker，不创建数据库。现有数据、遗留消费者和待发布消息必须遵循已记录的协同 v2 切换方案。本地协议、存储和传输检查，与真实 Java/Agent/供应商验收，仍是不同的验证边界。

## 服务职责

| 组件 | 职责 |
| --- | --- |
| Agent | 决定生成什么，并构造生成请求。 |
| Java 后端 | 管理业务 Task、所属用户和 Session、输入资源授权、Asset 导入，以及用户可见的状态。 |
| Model connector | 校验生成契约，将请求适配到所选供应商，跟踪执行、保留结果，并递交执行结果。 |

Connector 通过业务 Task 和提交操作关联执行工作。它的执行生命周期不依赖 Run、Turn、Item 或工具调用的生命周期。Run 结束不会终止后台生成。

### Task 提交与结果消费

1. Python 调用 Java `POST /internal/v2/users/{user_id}/tasks`，提交 `operation_id`、`session_id`、`kind` 和 `request`。当前每次工具调用对应一个 Task 的契约中，`operation_id` 是原始 `TOOL_CALL` Item ID，不是模型供应商的 `call_id`。
2. Java 原子地创建归属于 Session 的 Task 及其 v2 `SUBMIT` Outbox 命令，然后返回不可变的 `TaskSubmissionReceipt`：`type=task_submission_receipt`、`task_id` 和 `accepted=true`。这仅确认 Java 已接收；connector 接收和供应商受理仍可能尚未完成。
3. Connector 接收同一操作，独立于原 Run 执行，并向 Java 报告进度或最终结果。操作 UUID 是幂等标识，不是 connector 指向 Agent Item 的外键。
4. Java 将明确的生成失败或已完成的 Asset 导入转换为一个 Session 输入 Item，其 `source=SYSTEM`、`kind=TASK_EVENT`，内容仅包含 `task_id`。它的 `INPUT_ACCEPTED.event_id` 等于该输入 Item 的 ID。进度变化产生 Task SSE 失效通知，不创建终态输入。
5. Python 持久化接收输入，并在后续冻结的 Turn 边界纳入上下文。空闲时，稍后接收的 Task 通知可以依据 Session 停止和唤醒规则唤醒一个新 Run。它不会改写原提交 ToolResult，也不会人为使旧 Run 完成。
6. 要查看结果，模型发起新的 `get_task` 调用。Java `GET /internal/v2/users/{user_id}/sessions/{session_id}/tasks/{task_id}` 返回 `TaskQuerySnapshot`：包含 `type=task_query_snapshot`、`task_id`、`version`、`status`，有序的 `assets`（包含 `asset_id` 和 `source_index`），以及可为空的 `failure`（包含 `stage`、`code` 和 `message`）。失败阶段是 `GENERATION` 或 `IMPORT`；供应商 URL 不是 Agent 的结果接口。

Python 提交结果不确定时，通过 Java `GET /internal/v2/users/{user_id}/tasks/by-operation/{operation_id}` 核实，不重复 POST。缺少接收回执，不能证明 Task 或外部工作不存在。已确认的 `get_task` 结果对原调用保持不可变；另一次读取属于新观察。Python 可以在执行回执中保留 Run/Turn/工具调用绑定，用于恢复该调用，但这些是 Python 执行来源信息，不是业务 Task 依赖或 connector 字段。`STOP_SESSION` 影响 Agent Run 执行，不影响后台生成 Task。

提示词规划、计费、Session 调度、自动模型选择、供应商故障切换、用户主动发起的 Task 重试，以及取消操作，都不在本次范围内。消息重投、状态核对和交付恢复始终保留原操作身份；这些行为不授权再次发起付费生成。

## 具有明确类型的生成协议

图片和视频生成共享身份标识与交付约定，同时保留各自独立的请求类型。以下是 connector 管理的逻辑类型，由当前 v2 消息信封承载，或从这些信封持久化得到。具体模型请求和参数 Schema 仍归所选 connector 适配器管理。

| 类型 | 定义的职责和字段 |
| --- | --- |
| `GenerationIdentity` | 包含 `user_id`、`session_id`、`task_id` 和 `operation_id`；前两个字段用于保留归属和关联信息，不引入对 Run 的依赖。 |
| `SeedreamRequest` | 必需的上游 `model_id`、提示词或图层分解输入、图片引用，以及全部已记录的扁平图片参数；嵌套参数使用具名类型。 |
| `SeedanceRequest` | 必需的上游 `model_id`、有序且具名的文本/图片/视频/音频/草稿内容类型，以及全部已记录的扁平视频参数。 |
| `InputGenerationPayload` | 收到的生成请求体：`identity`、`kind`，以及相应的显式类型图片或视频 `request`。这是持久化快照，不是新的消息信封。 |
| `ProviderSubmissionPayload` | 准备好的供应商调用：`provider_id=byteplus`、不含秘密的 `account_ref`、`kind`，以及确切的显式类型 `request`。在调用供应商前固定，不包含 API key。 |
| `ProviderMediaUrl` | 供应商支持的 URL、data URI 或 BytePlus 数字角色 `asset://` URI。Java Asset-ID 解析仍是独立且尚未实现的输入访问边界。 |
| `ResultSource` | 包含下载地址 `url`、`filename` 和 `media_type`，沿用现有 Java 结果导入边界。 |
| `GenerationError` | 包含 `code`、`stage`、`message` 和可选的具名 `provider_diagnostic`；具体定义见下文。 |
| `GenerationOutcome` | `GenerationSuccessOutcome` 和 `GenerationFailureOutcome` 的判别联合；具体定义见下文。 |

BytePlus 请求类型对应用户提供的官方 API 请求体，只有上游 `model_id` 精确映射为供应商的 `model`。允许任何非空白 Model ID 或 Endpoint ID，不设置白名单、前缀规则、默认模型、别名或自动故障切换。本地校验已知结构和通用边界；模型开通、权限及具体版本的能力组合由供应商判断。省略参数时保留供应商默认值，显式 false 保持原样；显式 null 或未知字段被拒绝，不能静默丢弃。

已知业务结构使用具名 Pydantic 模型、dataclass 或其他等效的显式类型。不能用通用字典代替已知的请求、参数对象、结果或错误。只有在 connector 不解释供应商原始数据中的字段、仅归档或转发这些数据时，才可以将它们保留为不透明载荷。

Java 校验保留的输入 Asset 归属。Connector 在提交前校验已知请求结构，并将所选 Model ID 原样发送给 BytePlus。当前输入边界接受供应商支持的媒体引用，不解析 Java Asset ID，也不新增虚构的 Java 内容访问接口。

Java 和 Python 通过 `ConnectorRequest` 封装 connector 管理的参数，不负责具体供应商参数。Python 当前解释顶层 `asset_ids`；Java 校验整个请求中保留字段 `asset_id`/`asset_ids` 的引用。这些约定本身不向 connector 提供已授权的文件访问。未解析的 Java Asset-ID 字段被具名 BytePlus 适配器拒绝；上游当前须提供可访问的媒体引用。`input_generation_payload` 是从扁平提交中生成的存储快照，不是嵌套的消息成员，也不替代 `request`。

### BytePlus 适配器边界

`kind=IMAGE` 选择 `POST /images/generations`；`kind=VIDEO` 选择 `POST /contents/generations/tasks`，通过 `GET /contents/generations/tasks/{id}` 读取状态。所有请求使用固定官方地址 `https://ark.ap-southeast.bytepluses.com/api/v3`。用户提供的[图片接口文档](https://docs.byteplus.com/en/docs/modelark/image-generation-api)和[视频创建文档](https://docs.byteplus.com/en/docs/modelark/create-video-generation-task-api)定义请求结构；[视频查询文档](https://docs.byteplus.com/en/docs/modelark/get-video-generation-task-api)定义状态回执。具名响应模型保留需要解释的输出、图层信息、用量和供应商错误；未来未知响应元数据被忽略。

图片执行仅接受非流式响应：省略 `stream` 或显式 `false` 可以使用，显式 `true` 在本地校验失败。该传输边界独立于允许全部 Model ID 的决定。视频输入支持文本、图片、视频、音频和草稿任务类型，不在本地重新维护各模型版本的能力组合表。`callback_url` 可作为官方请求参数转发，但 connector 本身未实现 BytePlus webhook 接收器；恢复路径轮询原任务。

确切提交及可能发送的标记先提交，再发起一次 POST。同步图片输出保存为 `SeedreamReceipt`；视频创建先保存外部 ID，完成信息保存为 `SeedanceReceipt`。不假定 BytePlus 已提供可靠的幂等提交或按原请求查找任务的保证。HTTP 408/5xx、超时、连接中断、成功数据格式错误或回执持久化丢失，都使执行保持 `RUNNING`；外部 ID 为空或认领过期均不允许自动重发。无法核实的原提交需要运维核对，本次不新增手动核对 HTTP 接口。有效的 4xx 拒绝、部分图片失败，以及视频失败、取消或过期，可以提供明确失败证据；查询传输错误或未知状态不能。

只有 `LUX_CONNECTOR_PROVIDER_EXECUTION_ENABLED=true`，且 MySQL、API 凭证、S3 配置和稳定的 connector 结果基础 URL 完整时，才启用执行。请求示例和确切环境变量维护在 [README](../../../README.md) 中。HTTPX 提供 REST 边界，boto3 提供带签名的 S3 操作。离线供应商 fixture 不证明真实账号或模型可用。

### 终态结果结构

`outcome` 列可以存储 JSON 对象，但它表示已定义的业务类型，而不是任意字典。Python 处理时必须使用对应的具名模型。非终态执行不设置该结果字段。终态结果的 `status` 判别字段必须与执行状态一致，快照在终态事务提交后不可变。

| 类型 | 确切字段和约束 |
| --- | --- |
| `GenerationSuccessOutcome` | `status`：字面值 `SUCCEEDED`；`sources`：含 1 至 64 个 `ResultSource` 的有序列表，包含全部必需的结果清单。没有错误字段。 |
| `GenerationFailureOutcome` | `status`：字面值 `FAILED`；`error`：必需的 `GenerationError`。没有结果来源字段。 |
| `ResultSource` | 必需的字符串字段 `url`、`filename` 和 `media_type`。URL 必须是完整 HTTP(S) 地址，具有主机且不含用户信息部分；文件名非空白，最多 255 个字符，不含路径分隔符或控制字符；媒体类型非空白，最多 128 个字符，使用具体类型而非通配符。可用性遵循下文的结果保留契约。 |
| `GenerationError` | 必需且非空白的字符串 `code` 和 `message`；错误码最多 64 个字符，消息最多 65,535 个 UTF-8 字节。`stage`：`VALIDATION`、`PROVIDER` 或 `RESULT_PRESERVATION` 之一；可选的 `provider_diagnostic`：`ProviderErrorDiagnostic`。这些是错误类别，不是执行阶段或额外的任务状态。 |
| `ProviderErrorDiagnostic` | 必需的 `provider_id`；可选字符串 `provider_code` 和 `provider_message`。不包含凭证或未定义的业务字段。 |

GenerationError.code 是 connector 管理的分类码，必须符合当前 Java Task.error_code VARCHAR(64) 的容量。校验错误码和消息长度边界，不能静默截断。供应商原始证据保留在 connector 回执和具名供应商诊断信息中；不能将供应商原始错误码缩短后充当 connector 分类码。

持久化成功结果示例：

```json
{
  "status": "SUCCEEDED",
  "sources": [
    {
      "url": "https://media.example.com/result.png",
      "filename": "result.png",
      "media_type": "image/png"
    }
  ]
}
```

持久化失败结果示例：

```json
{
  "status": "FAILED",
  "error": {
    "code": "PROVIDER_REJECTED",
    "stage": "PROVIDER",
    "message": "The provider rejected this request."
  }
}
```

这些对象既定义 connector 存储的结果，也定义 Java v2 接受的终态 `GenerationEvent.outcome`。遗留 v1 适配保持独立，connector 成功仍早于 Java Asset 导入成功。发布前校验结果数量、元数据和错误限制；付费提交前，模型允许的输出数量必须符合接收方契约。

## 内部消息交付

当前 Java 生成路径使用 Schema 版本 2 和以下主题：

- `lux.generation.commands.v2`：用于提交请求和状态查询。
- `lux.generation.events.v2`：用于 connector 观察和执行结果。

Java 还通过独立的遗留适配器接收 `lux.generation.events.v1`。新的 connector 命令和事件使用 v2；遗留接收路径不是新的 v1 提交路径。主题和 Schema 版本必须一致。启用消费者时，必须隔离遗留 worker、offset 和待发布消息，避免将旧积压消息解释为 v2 输入。

生成消息的分区键仍为 `user_id:task_id`。事件保留稳定的事件 ID，以及每个 Task 内单调递增的序号。发布重试复用原消息信封、ID、序号和载荷，发布器维持同一个 Task 内的事件顺序。完全相同的重复事件可以安全处理；如果使用已有标识携带不同内容，则记录为冲突。Java Task 的版本号是独立的业务计数器，不是 connector 的事件序号。

内部回调是通过 Kafka 交付的生成事件。供应商 webhook 是另一条独立的输入边界，由相应的供应商适配器负责。初始交付路径不增加 HTTP 提交接口，也不增加面向不同调用方的 HTTP 回调机制。

### V2 生成消息信封

全部生成消息信封都是扁平对象，包含 `schema_version=2`、稳定的 `event_id`、`event_type`、原始 `occurred_at`、`user_id` 和 `session_id`。ID 是规范的小写 UUID，时间戳使用微秒精度的 UTC 时间。命令或观察在顶层增加以下成员：

| 具名消息信封 | 额外成员和含义 |
| --- | --- |
| `GenerationSubmit`、`event_type=SUBMIT` | 包含 `task_id`、`operation_id`、`kind`（`IMAGE` 或 `VIDEO`），以及 connector 管理的 `request`。不包含 Run 或工具调用引用。 |
| `TaskStatusQuery`、`event_type=TASK_STATUS_QUERY` | 包含 `task_id` 和非负有符号 64 位的 `last_observed_sequence_no`，后者是 Java 当前已观察到的 connector 序号水位。查询不要求操作 ID 或供应商 ID。 |
| `GenerationEvent`、`event_type=GENERATION_EVENT` | 包含 `task_id`、正的有符号 64 位 `sequence_no`、`connector_status: ConnectorExecutionStatus` 和 `outcome: GenerationOutcome | null`。进度、终态结果和显式状态响应都使用这种共用观察形式。 |

Java v2 解码器接受具名状态和结果结构，不按历史 v1 的五种事件名称分派。`ConnectorExecutionStatus` 仅有 `ACCEPTED`、`RUNNING`、`SUCCEEDED` 和 `FAILED`。`ACCEPTED`/`RUNNING` 显式携带 null 结果；`SUCCEEDED`/`FAILED` 必须包含匹配的结果变体。供应商外部 ID、原生回执、`run_id`、`turn_id` 和 `tool_call_id` 不加入 v2 生成消息信封。Connector 根据用户归属、Session 和 Task 身份，从自身持久化执行中解析对应的供应商操作。

| Connector 观察 | Java 业务行为 |
| --- | --- |
| `ACCEPTED` | Task 从 `CREATED` 进入 `SUBMITTED`，表示 connector 已持久化接收。 |
| `RUNNING` | Task 进入 `RUNNING`；connector 处理包括提交、状态核对和结果保留。 |
| `SUCCEEDED`，且包含完整清单 | Task 进入 `IMPORTING`；只有已持久化且校验通过的 Asset 才允许 Java 进入 `SUCCEEDED`。 |
| `FAILED`，且包含已定义错误 | Task 进入 `FAILED`，`error_stage=GENERATION`，并保留原始错误码和消息。 |

每个 Task 内的 `sequence_no` 映射为 Java Task 的 `provider_event_seq`；Java Task `version` 仍是独立的业务快照版本。Session 输入 `input_sequence_no`、Python 执行事实 `event_seq` 和 `source_revision`，以及 Java SSE 序号，都是不同的计数器。Connector 观察不是 Agent 执行快照，不携带 Python checkpoint 或 Run/Turn 修订字段。更细的生成错误阶段和可选供应商诊断信息保留在 Java 原始 Inbox 消息信封中，不增加 Task 字段。

显式状态查询按命令事件 ID 去重，观察已有执行。新的响应使用 connector 自己的下一个序号，并在已有终态时使用相同的不可变终态结果；重放原终态事件时，保留原 ID、序号和内容。调用方水位不允许重建执行或重置计数器。查询失败、尚不能定论的执行缺失结果，或提交结果不确定，都不能编造 `FAILED`、`ACCEPTED` 或成功结果数据。Java 的观察扫描器只为处于生成阶段的 Task 请求 connector 状态，不查询供应商，也不下载结果。它与 connector 每个 worker 的供应商轮询循环保持独立。

供应商发往 connector 的 webhook 仍是独立输入方向，按供应商选择。同步 API 响应和轮询响应属于观察，不是 webhook 回调。Java Task SSE 失效通知和向 Agent 投递终态 `INPUT_ACCEPTED`，属于独立的业务通知。

### 传输与确认

已实现的 Java/agent v2 基线允许最大 31 MiB 的完整 UTF-8 JSON 消息信封，Kafka 请求、记录和分区拉取容量为 32 MiB，总拉取容量为 64 MiB。Connector 必须使用兼容的 broker 和客户端限制，并测量完整序列化消息信封。Java 提交超出大小限制时，在 Task/Outbox 接收事务提交前拒绝。已经执行的供应商结果若发布超出预算，必须保留完整结果和交付证据，不能截断或重复生成。修复传输与 Schema 不匹配后，再继续交付。

仅接收模式要求 MySQL `max_allowed_packet` 至少为 64 MiB。启用供应商时，启动过程另行校验回执容量：至少为供应商响应字节预算的两倍加 1 MiB，默认 128 MiB 响应预算对应 257 MiB；建议配置 512 MiB，容纳 SQL 转义开销。Connector 在启动付费 worker 前拒绝不足的容量，不修改服务器设置。Kafka 结果携带稳定的保留结果 URL，不包含 base64 图片字节。

Kafka 发布确认只证明 broker 接收，不证明 Java 应用或 Asset 导入。Java 保存原始生成 Inbox 证据，但不确认结果回调，在数据库事务外下载，然后提交 Task 终态结果、唯一 Session 输入、SSE、Outbox 和 Inbox `APPLIED`，之后才确认分区已完成的连续前缀。技术中断通过 Kafka 重投恢复同一事件和已接收清单，不使用回调 Inbox 扫描器或 MySQL 命名锁。当前 Java 处理在这个确认边界上，每次只处理一条轮询记录。Agent/Python 的先持久化接收再确认，以及 checkpoint 恢复，仍是独立机制。

## 持久化接收与执行

Connector 持久化记录命令接收、执行和发布。对于有效提交，执行记录也可以兼作接收和去重记录，不强制要求单独的 Inbox 表。如果使用独立接收记录，它必须与执行记录的创建在同一个本地事务中提交，之后才能确认 Kafka offset。无效或冲突的命令在记录后应保持可诊断，同时避免反复阻塞无关工作。如果接收信息无法持久化，就不能确认该命令。

确认 Kafka 消费之前，接收路径持久化消息信封、身份、不可变请求归档和 `ACCEPTED` 执行。启用的 worker 随后异步解码并校验具名供应商请求。已知结构无效时，在同一事务中保存失败执行及终态失败 Outbox 事件，再将这项工作视为完成，不调用供应商。即使命令已确认消费，持久化接收记录仍使校验过程可以恢复。模型可用性或具体版本能力的拒绝则由供应商响应确认。身份格式错误、未授权提交和冲突的重复提交作为诊断性拒绝记录，不能替换或失败原来已经接收的操作。

提交身份以 `user_id` 和 `operation_id` 为范围，与 Java 不可变的 Task 提交一致。一个操作被接收后，其 Task 标识和请求载荷就固定下来。相同身份和内容的重复请求返回或查询已有执行。直接比较规范化的请求内容，不要求另外持久化请求哈希。JSON 对象键顺序、传输事件 ID 或时间戳不会改变请求身份。规范化必须保留有序输入列表，以及协议对字段省略、null 和默认值的语义。如果重复请求携带不同的 Task、Session、模型或请求内容，则视为冲突。传输层的重复事件不会创建重复执行。不能根据当前 Agent checkpoint 重建请求，也不能要求原 Run 仍处于活动状态。

供应商提交和结果保留各自保留执行归属或幂等保护。使用数据库租约时，认领或修订号校验阻止旧执行者对该认领工作的过期写入；仅靠租约，无法保证供应商提交的幂等性。初版中，每个 worker 独立轮询可查询的执行，不认领执行租约。轮询观察通过条件数据库更新和下文的终态提交规则应用。

调用供应商前，先持久化提交载荷、提交开始标记，以及将要使用的供应商关联标识或幂等键。这份载荷是 connector 计划发送内容的快照，固定供应商、账号引用和确切的显式类型供应商请求，避免恢复时根据已经变化的模型配置重新解释旧执行。它在调用前创建；供应商外部 ID 或响应则在调用后取得。账号引用不包含凭证。一旦取得供应商已受理的外部 ID，就立即将其持久化。只有恢复所需信息超出该 ID 时，才保留额外的响应证据。重启恢复时，利用这些记录查询原有供应商执行，或恢复对它的状态观察。

### 执行状态

以下是 `generation_execution.status` 的全部取值，它表示 connector 的任务执行状态。Java 的业务 Task 及其 Asset 导入生命周期保持独立。供应商提交、状态核对和结果准备属于处理细节，以持久化事实表示，不增加另一套执行阶段枚举。

| 状态 | 含义 |
| --- | --- |
| `ACCEPTED` | 请求已持久化，connector 尚未开始处理。 |
| `RUNNING` | Connector 已开始处理，但尚未形成终态结果；包括提交、等待供应商或供应商执行、状态核对和结果准备。 |
| `SUCCEEDED` | 完整且可恢复的结果清单已持久化，其来源可供 Java 导入。 |
| `FAILED` | 已记录明确的执行失败。 |

正常状态转换为从 `ACCEPTED` 进入 `RUNNING`，再从 `RUNNING` 进入 `SUCCEEDED` 或 `FAILED`。可信提交在校验时被拒绝，可以直接从 `ACCEPTED` 进入 `FAILED`，也可以与失败事件一起持久化创建为 `FAILED`。这种情况下不会调用供应商。

`SUCCEEDED` 和 `FAILED` 是不可变的执行终态。旧进度不能替换终态结果。冲突的迟到观察记录会保留用于诊断，但不会产生另一个终态结果。

任务处于 `RUNNING`，不表示供应商此刻正在生成。提交结果不确定、结果尚未完整保留时，都保持 `RUNNING`；仅有供应商完成的观察结果，不足以判定 connector 成功。

Connector 在处理可能产生外部副作用之前，先提交 `RUNNING` 状态。每次供应商提交还要求事先提交 `submission_started_at` 和固定的提交载荷。在这一不变量成立时，恢复的执行如果没有该标记，可以在持有有效租约的情况下继续准备并进行首次提交。有标记但没有已确认回执，表示请求可能已经被受理；在发送前崩溃和在受理后崩溃，可能留下相同记录。必须使用供应商的幂等或查询能力消除这个信息缺口。缺少 `provider_external_id`，不能证明提交从未发生；同步供应商可能不提供这种 ID。

提交结果不确定时保持 `RUNNING`。超时或用尽状态核对预算后，需要人工或运维关注，同时保留这种不确定性。如果供应商既不支持安全的幂等提交，也不提供查找已受理操作的方法，connector 就无法同时保证最终执行和不重复发起付费任务。接入供应商时，必须明确它支持的恢复方式。

轮询、本地恢复和消息发布可以采用有次数上限的尝试及退避策略。用尽这些尝试次数，本身并不能证明供应商执行失败。任何已经失败的终态都不会重新开启生成。

## 执行持久化

### 数据归属

Connector 管理轻量的生成执行记录和事件发布记录。Java 仍是其业务 Task、Session、授权和 Asset 表的唯一管理方。Connector 复用 Java 的 `task_id` 关联执行，不创建第二个业务 Task 标识，不复制业务数据结构，也不直接写入 Java 的表。本地执行记录的 `id` 仅是存储标识，不替代协议中的 `task_id` 或 `operation_id`。

独立的数据归属不要求使用独立数据库实例。共享实例可以为各服务提供各自独立的 Schema 和访问权限。Connector 事务只覆盖其自身管理的记录；Java 状态通过内部协议变更。当前实现选择 MySQL 8.0 或更新版本，使用预先创建且与 Java 分离的 connector Schema。部署和数据保留配置仍属于实现选择。

### 核心逻辑记录

`generation_execution` 和 `outbox_event` 是核心逻辑记录；以下字段描述其含义。实际 MySQL Schema 和受保护的迁移单独实现。

当前 [MySQL Schema](../../../src/lux_model_connector/schema.sql) 实现这两种记录，以及 `command_receipt`、`kafka_receipt`、`generation_result` 和 `connector_schema_version`。请求归档和结果使用 `LONGTEXT`，在适用处设置 `JSON_VALID` 校验，保留数字值。所属 v1 Schema 以增量方式迁移到 v2，增加可为空的执行认领字段及结果记录；打包的 [v1 快照](../../../src/lux_model_connector/schema_v1.sql) 用于校验迁移来源。MySQL DDL 不具有事务性，通过受保护的步骤恢复中断的迁移。其他项目或不兼容的 Schema 仍被拒绝。初始化校验列、类型、可空性、默认值、显式排序规则、自增、必需主键和唯一索引，以及 InnoDB。SQL 调用在事件循环之外运行。发布使用独立的 30 秒数据库时间认领，重试延迟为 2 至 60 秒；供应商提交和结果保留使用 15 分钟执行认领。只读供应商轮询不要求执行认领。

| `generation_execution` 字段 | 类型和用途 |
| --- | --- |
| `id` | 正的 64 位整数；独立的本地记录主键。其生成机制随数据库选择确定。 |
| `user_id`, `task_id` | UUID；表示归属和 Java Task 关联，通过唯一约束保护，不作为主键。 |
| `session_id`, `operation_id` | UUID；不可变的 Session 关联标识和提交幂等标识。 |
| `input_generation_payload` | 收到的请求体的不可变快照，用具名类型 `InputGenerationPayload` 表示，并可在合适时存为 JSON。可执行工作必须解码为已定义的请求类型。无效输入只能在具有明确类型的拒绝边界作为诊断证据归档；冲突检测必须比较该原始内容，不能比较两个解码后为 null 的请求。不要求单独的哈希列。 |
| `status` | 仅为 `ACCEPTED`、`RUNNING`、`SUCCEEDED` 或 `FAILED`。 |
| `submission_payload` | 可为空的具名类型 `ProviderSubmissionPayload`：包含 `provider_id`、`account_ref`、`kind`，以及确切的具名请求快照。记录要调用哪个供应商和账号，以及发送什么；在首次提交前固定，不能证明提交已被受理。 |
| `submission_started_at` | 可为空的时间戳，在任何供应商提交前提交；它表示可能发生过提交，不能证明供应商已受理。 |
| `provider_external_id` | 外部生成 API 返回的可选任务、请求或操作 ID，用于查询原操作。已确认的 ID 不能被替换。同步 API 可能不返回此 ID。 |
| `outcome` | 可为空的 `GenerationOutcome`；connector 的最终执行结果，进入终态时必需，之后不可变。成功结果包含完整且有序的结果来源清单；失败结果包含 `GenerationError`。 |
| `terminal_event_id` | 可选 UUID，在首次形成终态结果时分配；引用其原始且不可变的 Outbox 事件，使重试和重放找到同一条通知。 |
| `next_event_sequence` | 正的有符号 64 位整数，用于原子地分配下一个观察序号。事件通过 `sequence_no` 携带它，Java 通过 `provider_event_seq` 记录已观察序号。它与 Java Task 版本号以及 Agent/Session 计数器独立。 |
| `created_at`, `updated_at`, `started_at`, `finished_at` | 时间戳；`started_at` 记录进入 `RUNNING` 的时刻，`finished_at` 当且仅当执行处于终态时存在。 |

强制保证 `(user_id, task_id)` 和 `(user_id, operation_id)` 均唯一。前者在本次范围内保证每个业务 Task 只有一个执行；后者用于提交去重。独立主键不替代这些业务约束。重复操作必须保留原 Task、Session 和请求身份。无效输入证据保留在具有明确类型的接收或诊断边界，不作为匿名业务字典处理。请求校验失败的记录可以不包含提交载荷和供应商证据。

以下字段支持供应商恢复、worker 协调或诊断。是否需要它们取决于所选供应商和执行器；它们不是调用方协议字段：

| 条件性执行字段 | 保留条件和用途 |
| --- | --- |
| `provider_receipt` | 从供应商 API 响应中提取的具名证据，仅在 `provider_external_id` 和固定的提交信息不足以恢复时保留。例如，同步 API 可能返回输出地址，但没有任务 ID。保留恢复同一份输出所需的响应字段，不强制归档整个响应。 |
| `provider_completed_at` | 可选时间，记录观察到供应商明确完成的时刻；可用于诊断，但不能证明结果已完整保留。 |
| `execution_lease_token`, `execution_lease_until` | 提交或结果保留认领的令牌和过期时间；当前认领持续 15 分钟，结果工作期间续期。接管时更换令牌，属于该认领的写入要求持有当前且未过期的令牌。同一不可变原提交的回执检查点可以在租约过期后接收迟到响应，避免丢弃唯一的供应商受理证据。只读轮询不认领这些字段，Outbox 认领保持独立。 |
| `next_reconcile_at` | 初版轮询暂不采用。未来需要按执行设置轮询间隔或退避时，可以使用这个时间戳；固定周期扫描不要求它。它从不调度新的生成。 |
| `last_reconcile_at` | 可选的诊断时间戳，记录上次供应商状态检查；不列入初版必需字段。 |
| `last_observation_error` | 可选的显式类型诊断信息，记录上次轮询或查询错误；不列入初版必需字段。仅有观察错误，不等于明确的生成失败。 |

具有持久任务查询能力的异步供应商，在提交后可能只需保留 `provider_external_id`。无法恢复同步响应的供应商，必须在继续准备结果之前保存相关响应检查点。需要解释的响应字段始终使用显式类型；不透明的原始响应只能为诊断而归档。

### Worker 执行归属与状态观察调度

初版轮询机制不让某个 worker 永久负责一个执行。每个 worker 都基于持久化执行记录运行自己的周期循环。Worker 不等待其他轮询 worker 的租约过期，重启时也不需要恢复内存中归属于自己的轮询任务列表。

每轮循环：

1. 按规模受限的分页扫描处于 `RUNNING`、具有 `provider_external_id`，且供应商支持安全重复只读状态查询的执行。扫描必须最终访问每个符合条件的执行，不能反复只选第一页。
2. 在本地有界并发和请求超时限制下，查询原供应商、账号和外部 ID。每个 worker 同时只允许一个活动扫描周期；不同 worker 可以并发查询同一操作。
3. 将每个响应转换为具有明确类型的供应商观察，并通过共用的数据库观察处理路径应用。单次查询失败不会中止扫描中其他无关工作。
4. 等待配置的固定轮询间隔，再开始下一轮。启动和后续循环都从持久化记录中重新发现工作，不要求按执行设置调度时间戳。

扫描只是恢复路径之一，不是全部执行恢复机制。`ACCEPTED` 执行仍需要提交处理。没有供应商外部 ID 的 `RUNNING` 执行，可能需要查找提交结果不确定的操作，或根据同步响应检查点恢复。被中断的结果保留必须恢复原始输出，即使该执行不符合状态轮询条件。这些情况保留各自的专用恢复路径；外部 ID 为空绝不授权重新发起生成。

应用观察时，必须重新读取当前执行，并通过原子的条件更新、事务和唯一约束强制执行合法状态转换。迟到的运行中观察不能覆盖终态结果。超时、查询错误或尚不能定论的“未找到”响应，都不能证明生成失败。重复观察不能仅因又发生一轮轮询，就重复分配进度事件。Java 显式状态查询命令仍是单独去重的请求，可以获得新的响应事件。

只有完整的原始输出已被保留后，供应商成功才能成为 connector 成功。并发 worker 必须以幂等方式保留同一份输出，或使用独立的结果保留认领，不能以相互竞争的来源集合覆盖已接收的清单。结果就绪后，通过一次条件终态转换，将结果、原始终态事件 ID、序号分配和 Outbox 一起提交。竞争的 worker 重新读取已提交结果，避免创建第二个原始终态事件，并保留冲突的终态证据用于诊断，不能仅按响应到达顺序选择结果。

例如，worker B 已保留结果并提交成功后，worker A 的查询才返回旧的运行中观察。观察应用路径无需轮询执行权，也能拒绝状态回退。如果 A 崩溃，B 继续正常扫描持久化记录，无需进行轮询租约交接。

独立扫描有意允许重复查询供应商。请求量会随 worker 数量增长，因此初版使用本地有界并发和固定间隔。这一选择使恢复循环保持简单；实际查询负载需要时，可以增加协调轮询或按执行设置退避。`next_reconcile_at`、`last_reconcile_at` 和 `last_observation_error` 不是初版轮询机制的必需字段。

提交、结果保留和 Outbox 发布独立于只读轮询。它们采用数据库认领时，租约过期只赋予本地执行权，过期认领的写入被拒绝。它不会停止正在进行的供应商调用，也不授权再次发起付费生成。独立轮询不移除这些操作的幂等、顺序或过期确认保护。

| `outbox_event` 字段 | 类型和用途 |
| --- | --- |
| `event_id` | UUID 主键；稳定的传输事件标识。 |
| `user_id`, `task_id`, `sequence_no` | 执行关联信息和正整数观察序号；三者组合唯一。 |
| `topic`, `message_key`, `envelope` | 目标主题、分区键，以及完整的具名消息信封快照，包含原始事件类型、时间、身份标识和载荷。这些信息在重试或重放期间保持不变。 |
| `created_at`, `published_at` | 创建时间，以及可选的 broker 发布确认时间。`published_at` 不能证明 Java 已应用事件或导入 Asset。 |
| `attempt_count`, `next_attempt_at`, `last_error` | 非负的交付尝试次数、重试调度，以及可选的发布诊断信息。 |
| `lease_token`, `lease_until` | 发布认领令牌和过期时间；处理旧的确认时，不能将较新的发布尝试标记为完成。 |

执行结果、终态事件标识、序号分配和终态 Outbox 插入必须一起提交。一个终态执行恰好对应一个原始终态结果事件。状态查询响应是独立的观察，不能产生额外的终态转换。发布 worker 维持每个 Task 内的顺序；新分配的事件不能越过更早的待发布事件。

### 接收记录

这两种核心记录不会自动实现全部输入边界契约。提交对应的执行记录可以提供持久化接收和操作级去重。保留所有命令事件标识、诊断同一事件 ID 下发生变化的内容，或在异步应用 webhook 之前确认接收，都需要持久化接收机制。

如果无法在执行记录中安全地表示这种机制，就增加 Inbox/接收记录。它保留输入身份、具名命令或观察结果、载荷身份、接收与应用进度，以及适用时的拒绝诊断信息。输入处理进度独立于四种执行状态。格式错误的输入可以作为不透明证据归档在具名拒绝记录中，不能成为字段未定义的已知业务请求。

### 事务与恢复

| 边界或故障窗口 | 必需行为 |
| --- | --- |
| 接收提交 | 在确认 Kafka 消费之前，一起保存接收信息和执行记录；任何需要独立保存的接收记录也在该事务中保存。 |
| 持久化接收后，拒绝已知结构无效的请求 | 异步 worker 一起提交失败执行、结果、序号和失败 Outbox 事件；恢复使用已经确认消费的持久化执行。 |
| 进入 `RUNNING` 后、形成提交载荷前崩溃 | 恢复准备工作；只有在持有有效执行认领、并持久化提交载荷及标记后，才允许首次提交。 |
| 形成提交载荷后、确认回执前崩溃 | 核对原供应商、账号和请求；不能根据供应商外部 ID 为空或租约过期推断未曾提交。 |
| 已持久化供应商完成信息，但结果准备被中断 | 恢复对同一份输出的保留，期间仍保持 `RUNNING`。 |
| 独立轮询周期被中断 | 任一 worker 的后续循环重新发现符合条件的持久化执行，无需接管轮询租约。 |
| 应用并发或迟到的轮询观察 | 条件更新保持终态不可变；重复观察不创建第二个原始终态事件。 |
| 进入 `SUCCEEDED` 或 `FAILED` | 一起提交终态、不可变结果、原始事件标识、序号和 Outbox。 |
| 丢失 broker 发布确认 | 重试同一个不可变事件；更新交付信息，不改变执行结果。 |
| 重放已发布事件 | 保留其 ID、序号、时间和载荷。重新开启交付调度时，使旧发布认领失效，避免迟到的确认清除已请求的重放。 |
| 响应新的状态查询 | 在查询去重约束下，为当前观察分配新的响应事件 ID 和序号；不覆盖原终态事件。 |

数据库事务不包含 Kafka 发布、供应商调用或结果传输。持久化的提交证据支持恢复，但不能使外部调用与数据库操作成为原子操作。租约过期只赋予本地恢复的执行权，不会取消已发出的供应商请求，也不授权再次生成。

## 供应商状态观察

供应商适配器将同步响应、异步状态查询和 webhook 归一化为同一套具有明确类型的观察结果。只实现所选供应商实际支持的机制。

Webhook 适配器校验供应商要求的认证机制和任务关联信息，先持久化观察结果，再确认接收，并容忍重复和迟到交付。接收后的 webhook 处理必须可以恢复，不要求供应商再次发送它。

如果供应商提供状态查询，状态核对就可以恢复丢失的 webhook，或恢复被中断的轮询周期。核对过程保留原操作标识和已受理的供应商任务 ID。Connector 不能为了取得结果，将这个 ID 替换为一次新的付费执行。

初版查询实现遵循上述独立 worker 循环。接入供应商时，必须确认重复查询是安全的，不会发起提交或消耗一次性操作。同步响应和 webhook 使用相同的观察应用规则，并各自满足持久化接收要求。

## 终态结果交付

第一次进入终态时，在同一个本地数据库事务中保存不可变结果及其 Outbox 事件。事件发布在该事务之外进行。如果在事务提交与消息发布之间崩溃，仍然有可恢复的待发布事件；如果无法确定 broker 是否已确认，就再次发布同一个事件。

执行状态和交付状态分别记录。一个已经完成的执行，可能仍在等待发布、已经收到 broker 发布确认，或需要运维关注其发布问题。发布失败不会把成功生成改成生成失败，也不会触发另一次供应商调用。

Connector 保留持久化结果，供状态查询和重放使用。重放原终态事件时保留其原始身份和内容。独立的状态响应报告相同结果和结果清单，不创建新执行，也不产生新的业务完成事件。

Java 对入站事件去重，并持久化应用相应的 Task 状态转换。首次业务终态转换创建唯一的 `SYSTEM/TASK_EVENT` Session 输入，内容仅包含 `task_id`，并同时创建原始 `INPUT_ACCEPTED` 命令和 SSE 失效通知。去重依据 Task 身份，不依据 Task 版本或 connector 事件序号。进度和重复终态观察不能创建另一个终态输入。Python 通过 `lux.agent.commands.v2` 接收该输入；connector 不发布 Agent 命令，也不更新历史 ToolResult。Agent 输入排序、停止和唤醒决策，以及 checkpoint 纳入处理，仍由 Python 负责。

## 结果保留与 Asset 导入

Connector 执行成功要求有完整的结果来源清单，且其中内容可以保留或恢复，以供 Java 导入。在现有导入边界下，来源必须是返回 200、无需重定向的完整 HTTP(S) 资源。只有会过期的供应商 URL 并不足够，除非适配器可以独立保留或恢复同一份输出。

已接收的结果清单具有稳定顺序，Java 开始导入后就不再改变。恢复过程重新提供对同一内容的访问，不替换为新生成的输出。对于多份输出，只有完整的必需结果清单都可用时，才算成功。

如果续期供应商 URL 会改变已接收的结果清单，就必须在报告成功前使用持久有效的来源 URL，或由 connector 提供的稳定 URL。Java 接收清单后再更新其中即将过期的 URL，不符合当前导入边界的约定。

BytePlus 输出 URL 在 24 小时后过期。当前实现将原始字节保留在预先创建的私有 S3 或兼容 S3 的 bucket 中，根据归属用户、Task 和来源索引生成不可变对象键。`generation_result` 记录稳定索引、存储身份、对象元数据/哈希/长度，以及随机 256 位读取能力凭证。上传成功但 SQL 检查点丢失时，通过匹配已有对象元数据恢复，无需再次下载供应商 URL 或生成。成功之前必须保留全部必需原始输出，包括请求的视频末帧或图片图层。

成功清单使用 `${LUX_CONNECTOR_RESULT_BASE_URL}/internal/generation-results/{token}`。Connector 网关以 HTTP 200 和稳定内容元数据流式提供私有保留字节，不跳转到会过期的预签名 URL。在可信 Java/connector 边界内，将每个能力 URL 视为读取凭证；URL 不含 API key 或 S3 秘密。服务路由及保留配置必须使重启和 Outbox 重放后同一 URL 仍可访问。供应商响应读取限制为 128 MiB，每个保留媒体文件限制为 100 MiB。传输或存储不确定，包括供应商 URL 返回 404/410，都使执行保持 `RUNNING`：对象查询未找到，不能证明先前不确定的上传不会稍后到达。后续扫描先检查不可变对象，存在时恢复。URL 过期但没有已确认保留输出时，需要运维核对，不能再次生成。

| 观察结果 | Java 业务行为 |
| --- | --- |
| Connector 报告生成成功，并提供结果来源 | Task 进入 `IMPORTING`。 |
| Java 校验并持久化保存全部必需的文件字节及 Asset 元数据 | Task 进入 `SUCCEEDED`，并产生其唯一的 Session 终态输入。 |
| Connector 报告明确的生成失败 | Java 将 Task 记录为生成阶段失败。 |
| Java 记录明确的导入失败 | Task 进入 `FAILED`；此前已经有效的部分 Asset 仍可使用。 |

Java 可以恢复被中断、尚未确认完成的导入，并按原始从零开始的 `source_index` 复用已校验 Asset。稳定的 Task、来源和 Asset 身份使已接收清单的顺序在重投时保持不变。`get_task` 返回这些 Asset 引用，而不是供应商下载 URL。一旦导入失败被记录，Task 就结束；该失败记录不是重新开启导入或重复供应商生成的指令。

对于有效回调和来源，已确认的 GET、连接或读取失败，包括 HTTP 5xx 或整个导入期限仍有效时发生的单来源超时，以及内容校验失败，属于 Java IMPORT 失败：持久化 FAILED、通知和 Inbox APPLIED，然后确认消费。整个导入的传输预算耗尽、数据库失败、进程或 consumer 归属中断，以及对象发布结果不明确，则要求保持未确认消费，对同一事件和 Asset 身份进行技术恢复或核对。事件身份或归属冲突、格式错误，只记录拒绝诊断，不改变原 Task。不可变对象发布和 Task 状态及版本条件防止重复或旧 consumer 覆盖结果；这些分类不引入业务 Task 重试。

输出清理必须依据导入完成确认，或明确约定的保留与恢复策略。Kafka 发布成功不能作为释放输出的信号。在清理策略明确之前，发布成功不得触发保留输出的自动删除。

## 遗留兼容性与剩余实现

Java [v1 契约](../../../../lux-java-backend/docs/api-contract-v1.md) 是历史契约。当前集成基线是 [v2](../../../../lux-java-backend/docs/api-contract-v2.md)。Java 将 v1 生成接收适配器明确独立出来，映射如下：

| 遗留回调 | 当前 Java 适配 |
| --- | --- |
| `TASK_SUBMITTED`、`TASK_RUNNING` | 映射为 `RUNNING`；旧的供应商确认不转换为 connector `ACCEPTED`。 |
| `TASK_RESULT` | 映射为 `SUCCEEDED`，将遗留顶层 `sources` 转换为具名结果。 |
| `TASK_FAILED` | 映射为 `FAILED`，使用遗留 `error_message`、生成阶段 `PROVIDER`，以及兼容错误码。 |
| `TASK_STATUS` | 旧 `SUBMITTED` 转换为 `RUNNING`；终态使用相应结果；`UNKNOWN` 仍是不确定观察，不是 connector 的第五种执行状态。 |

当前适配器不将 `provider_task_id` 复制到 Java Task 字段。新的 connector 事件使用四状态 v2 消息信封，`provider_external_id` 和 `provider_receipt` 继续由 connector 管理。V2 对象保持在 v2 主题上，旧的 Run 绑定命令保持隔离。历史 `TASK_UPDATED` ToolMessage 替换流程已废弃。

Java 和 Python 使用已实现的 v2 存储基线和消费者契约。Connector 已在该生成路径上实现持久化命令接收、状态查询观察、保存原始字节的 Outbox 发布、可选 BytePlus 执行、独立轮询和 S3 保留。通过显式运行时配置启用其 v2 消费者及供应商执行，并处理旧消息和数据，不能静默改写其含义。共享 v2 fixture 验证的是 Java/Agent 契约示例；本地 connector 检查不能证明真实跨服务生成或付费供应商行为。

当前选择 MySQL、带 RSA 支持的 PyMySQL、aiokafka、HTTPX 和 boto3。BytePlus 请求 Schema 和 S3 保留边界已实现；API key、模型/账号开通、真实云配置、Java Asset-ID 输入解析、webhook 接收和输出保留时长，仍属于部署或后续实现工作。在显式配置前，真实生成保持关闭。

## 验证要求

| 场景 | 可观察的保证 |
| --- | --- |
| 重复提交，包括相同身份下内容发生变化的提交 | 只存在一个已接收的执行；冲突被拒绝并保留记录。 |
| V2 提交及状态查询命令解码，以及主题和版本检查 | 用户、Session、Task 的扁平消息信封与 Java 一致；命令不要求 Run、工具或供应商 ID。 |
| Java 提交回执先于 connector 接收 | 稳定的 Task 回执不会错误地报告 connector 已接收或供应商已执行。 |
| 原 Run 结束后生成继续进行 | 同一操作完成，Java 创建独立的 Session 终态输入。 |
| 在持久化接收前后崩溃 | 未确认输入被重新投递，或已持久化输入被恢复，不会丢失。 |
| Worker 竞争或租约过期 | 只有一个提交执行者；过期写入被拒绝，接管不会盲目重新提交。 |
| 不同 worker 轮询同一供应商操作 | 允许重复只读查询；不要求轮询租约，观察始终保留原操作。 |
| 已提交成功后收到旧的运行中响应 | 不发生终态回退，也不产生重复终态通知。 |
| 两个 worker 并发观察到完成 | 安全保留同一份原始输出；只有一次终态提交和一个原始终态 Outbox 事件。 |
| 轮询 worker 重启，或扫描与查询被中断 | 重新发现符合条件的持久化工作；后续分页和无关任务不会长期得不到处理。 |
| 查询超时，或返回尚不能定论的任务缺失结果 | 执行保持非终态，后续扫描可以重试观察，不重新提交生成。 |
| 进入 `RUNNING` 后、形成提交载荷前崩溃 | 恢复准备工作，不错误地假设已向外部提交，并在首次调用前持久化提交载荷。 |
| 供应商已受理，但响应接收或本地回执写入被中断 | 核对原操作并恢复，不创建第二个付费任务。 |
| 提交结果仍不确定，或供应商成功时结果尚未完整保留 | 执行保持 `RUNNING`；两种情况都不会人为产生终态结果。 |
| 重复或迟到的供应商 webhook | 有效观察结果只生效一次；终态保持不可变。 |
| 在终态提交和发布过程中崩溃 | 结果与 Outbox 记录保持一致；重放保留同一个事件。 |
| Broker 确认丢失，或 Java 暂时不可用 | 交付可以恢复，不改变结果，也不再次调用供应商。 |
| 重放与迟到的发布确认同时发生 | 原事件保持不变，旧认领不能清除较新的重放请求。 |
| 结果访问失效，或导入被中断 | 在能力支持时恢复同一结果；复用已校验 Asset，不重新生成。 |
| 供应商生成成功，但 Java 随后导入失败 | Connector 结果保持成功；业务 Task 报告导入失败，不建立重试关联。 |
| Java 导入或失败后收到重复终态观察 | 只有一个 Task 终态输入，不改写原 Python ToolResult。 |
| Agent 收到终态通知后读取 Task | 新的不可变查询快照包含有序 Asset ID 和来源索引，或 GENERATION/IMPORT 失败。 |
| 结果、来源、错误超出限制，或消息信封过大 | 拒绝不兼容的交付数据，不截断，也不再次调用供应商。 |
| 来源超时与整个导入传输预算耗尽 | 已确认的来源失败提交为 IMPORT 失败；技术性预算耗尽保持未确认消费，用于恢复同一次导入。 |
| 遗留回调与 v2 观察并存 | 显式主题和版本适配保留四状态语义及原事件身份。 |
| 已知请求结构无效或图片 streaming 不受支持 | 持久化 `VALIDATION` 失败，不调用供应商，也不静默丢弃提供的字段。 |
| 上游提供任意 Model/Endpoint ID 或模型特定参数组合 | 确切 Model ID 和已提供参数发送给 BytePlus；远端拒绝被保留，不替换模型，也不自动重试。 |
| 原 S3 上传成功但 SQL 检查点丢失 | 已有不可变对象元数据恢复同一索引输出，不再向供应商发起 GET 或生成。 |
| 已配置凭证，但执行开关缺失或为 false | 接收仍可用，但不启动供应商生成。 |

状态、持久化和恢复测试使用行为可控的供应商测试替身。真实 Kafka 与 Java 集成需要单独验证序列化、确认顺序、重复处理和结果导入。付费的真实供应商测试需要另行确认目标、范围、预算和停止策略。
