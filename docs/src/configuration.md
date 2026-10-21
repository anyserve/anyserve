# Configuration

This reference describes the current Runtime, Limits and JournalConfig fields.
Defaults below come from `crates/anyserve-core/src/runtime/api.rs`; the tables
also identify limits that are available only to an embedded Rust Runtime. No
additional configuration knobs are implied by this document.

Read [execution architecture](architecture.md) for the lifecycle behind these
limits and [deployment and recovery](deployment.md) for listener and persistence
choices.

The CLI maps `[runtime]` keys `max_attempts` and `max_queue` to
`default_max_attempts` and `default_max_queue`. Time keys use `_ms` for
heartbeat interval/timeout, consumer stall, cancel grace and per-interface queue
timeout; terminal retention uses `_secs`. Its remaining supported keys have the
same names: `max_pending_jobs`, `max_pending_bytes`, `max_event_bytes`,
`max_result_bytes`, `max_sessions`, `max_terminal_jobs`, `event_log_max_events`
and `event_log_max_bytes`, plus `[runtime.interfaces.<name>]` overrides.
Priority levels, attempt ceiling, retry backoff and Limits currently use the
compiled defaults in the CLI; they remain fields of the embedded API.

The CLI enables `[journal]` by default. It accepts `enabled`, `path`, `full_sync`,
`queue_capacity`, `max_batch`, `retention_secs`; database `max_bytes` is currently
an embedded API field. `serve --ephemeral` disables the journal,
`serve --journal PATH` selects it. CLI flags override file settings. Unknown
configuration keys are rejected, including the removed backend selectors and
the removed retained-bytes key.

## Native services and management

`[server]` configures native Client/Worker gRPC. The optional `[console]` section
configures the management API, including `/healthz`, `/readyz` and `/metrics`.
Its fields are `enabled` (true when the section exists), `listen` (default
`127.0.0.1:3001`) and `allowed_origins` (default empty). See
`examples/server.toml` and [usage](usage.md) for startup and authentication.

Application execution uses native gRPC. Provider-compatible HTTP gateways,
generic HTTP forwarding and built-in backend workers have been removed;
`[openai]` and `[invoke]` are unknown configuration sections and fail parsing.
Worker handlers and backend clients live in application code using the SDK.

## 三个全局字节预算

三者对共享的 `Bytes` 存在重叠计量：同一份二进制数据可能同时被输入、结果或 Output 事件引用；Finished 仅按事件固定开销记入 event，结果数据仍由 result 计量。因此不能把三者相加当作 RSS 上限。

| 预算 | 字段 | 默认值 | 计量什么 | 生命周期 | 超限时 |
|---|---|---|---|---|---|
| input | `max_pending_bytes` | 2 GiB | 静态 inputs、params、流式输入（每块另计 64 B 开销和 metadata） | 从接受开始，到 job 终态释放 | Submit/Invoke 返回 `RESOURCE_EXHAUSTED`；流式输入的 `send` 返回 `RESOURCE_EXHAUSTED` |
| result | `max_result_bytes` | 2 GiB | worker 上传的结果数据 | 从上传开始，到最后一份引用被丢弃（包括 journal 提交中、保留在内存里的终态结果、Finished 事件） | 先按从旧到新驱逐终态 job 一次，再重试；仍放不下时，Attempt 流以 `RESOURCE_EXHAUSTED` 结束，worker 退避后只重传结果 |
| event | `max_event_bytes` | 1 GiB | 所有 job 的事件日志（每条另计 96 B 开销） | 写入日志到淘汰、ephemeral job 终态压缩（只留 Finished；结束时仍在跟随的读者读完后才压缩）或 job 被驱逐 | 写入前先按需驱逐终态 job 一次。本 job 的 Streamed 消费者落后时反压 worker，`consumer_stall_timeout` 后 job 以 `consumer_stalled` 失败；全局预算不足时最多等待 `consumer_stall_timeout`，仍不足则 Attempt 流以 `RESOURCE_EXHAUSTED` 结束；没有主消费者的 job 丢弃最旧事件，读者收到 `CursorExpired{oldest}`。 |

Output 帧的 payload 不允许 force_reserve。全局预算不足时每 250 ms 重试，
消费者进度变化也触发重试；超时只结束本次 Attempt 流，服务端不直接判 job 失败。
应用 Worker 可以将该传输失败作为 retryable 失败报告；已经暴露输出的 attempt 按既有
规则以 `output_interrupted` 结束。result 预算仍是驱逐后只重试一次，失败立即返回。

生命周期事件（Accepted / Assigned / AttemptRestarted / Finished）始终写入，
每条按 96 B 固定开销计费，可以超出 event 预算。保留日志的固定开销上界按
`(max_pending_jobs + max_terminal_jobs) × (2 × max_attempts_limit + 1) × 96 B`
计，默认值为 `110,000 × 41 × 96 B ≈ 433 MB`，低于默认 1 GiB event 预算。
因此该预算既不是全部元数据的精确内存计量，也不是进程 RSS 硬上限。

## RuntimeConfig

| 字段 | 默认值 | 作用 | 超限或触发时 |
|---|---|---|---|
| `heartbeat_interval` | 5 s | 通过 Welcome 下发给 worker 的心跳周期 | — |
| `heartbeat_timeout` | 15 s | 控制会话的静默上限，同时也是断线后的宽限期 | 会话关闭；宽限期内未重连的租约记为 `worker_lost` 并重试 |
| `default_max_attempts` | 3 | job 没有设置 `max_attempts` 时使用的值 | 用完后 job 失败，错误码 `attempts_exhausted` |
| `max_attempts_limit` | 20 | job 自设 `max_attempts` 的上限 | 超出的值被截断到上限 |
| `default_initial_backoff` | 200 ms | 第一次重试前的等待时间（之后按指数增长） | — |
| `default_max_backoff` | 10 s | 重试退避的上限 | — |
| `priority_levels` | 4 | 优先级档数 | `priority` 超出时被截断为 `levels-1` |
| `default_max_queue` | 10 000 | 每个 interface 的排队上限（Queued 加 Accepting） | `RESOURCE_EXHAUSTED`（queue full） |
| `interfaces.<name>.max_queue` | 无 | 覆盖某个 interface 的排队上限 | 同上 |
| `interfaces.<name>.queue_timeout` | 无 | 该 interface 的默认排队超时 | job 失败，错误码 `queue_timeout` |
| `max_pending_jobs` | 100 000 | 全局未完成 job 数 | `RESOURCE_EXHAUSTED` |
| `max_pending_bytes` | 2 GiB | input 预算，见上表 | 见上表 |
| `consumer_stall_timeout` | 30 s | Streamed 消费者不读取多久后判定为停滞 | job 失败，错误码 `consumer_stalled`；worker 收到 Cancel |
| `cancel_grace` | 15 s | 取消后等待 worker 确认的时间；期间仍占用容量 | 发出 Fence 并释放容量 |
| `terminal_retention` | 10 min | 终态 job 在内存中的保留时间 | 从内存驱逐；durable job 仍可从 journal 读取，ephemeral job 之后 `NotFound` |
| `max_terminal_jobs` | 10 000 | 内存中终态 job 的数量上限 | 驱逐最旧的终态 job |
| `event_log_max_events` | 8 192 | 每个 job 事件日志的条数上限 | 同 event 预算 |
| `event_log_max_bytes` | 8 MiB | 每个 job 事件日志的字节上限 | 同 event 预算 |
| `max_event_bytes` | 1 GiB | event 预算，见上表 | 见上表 |
| `max_result_bytes` | 2 GiB | result 预算，见上表 | 见上表 |
| `max_sessions` | 4 096 | worker 会话数（包括宽限期中的会话） | 新的 Hello 收到 `RESOURCE_EXHAUSTED` |
| `interfaces` | 空映射 | interface 策略集合，各项见上面的两行 | 未覆盖的 interface 使用全局默认值 |
| `limits` | `Limits::default()` | 下表中的协议和 payload 限制 | 见 Limits 表 |
| `journal` | `None`（嵌入式 Runtime） | 启用下表的 durable journal | 未启用时 durable Submit 被拒绝；CLI 默认启用 |

## Limits（协议与 payload 限制，通过 Welcome 下发前 4 项）

| 字段 | 默认值 | 作用 | 超限时 |
|---|---|---|---|
| `max_control_message_bytes` | 256 KiB | 控制会话单条消息的上限 | Assign 在构造时保证不超出 |
| `max_chunk_bytes` | 64 KiB | InputData、Frame 单块的上限 | Frame 超出时 Attempt 流以 `INVALID_ARGUMENT` 结束 |
| `attempt_window_bytes` | 4 MiB | worker 每个 attempt 未确认字节的窗口（由 SDK 执行） | 由 SDK 自行等待 Ack |
| `assign_inline_limit` | 64 KiB | inputs、params 加 metadata 不超过此值时随 Assign 内联发送 | 超过时 `inputs_via_attempt=true`，改由数据流发送 |
| `max_inline_object_bytes` | 16 MiB | 单个输入或结果对象的上限 | 输入：`INVALID_ARGUMENT`；结果：job 失败，错误码 `output_too_large` |
| `max_inline_total_bytes` | 32 MiB | 一个 job 的输入总量（含 params）或一个 attempt 的结果总量（含 URI、metadata、每对象 64 B） | 同上 |
| `max_metadata_bytes` | 32 KiB | job metadata、attributes、interface 名，以及 worker attributes/metadata/interfaces | `INVALID_ARGUMENT`；结果 metadata 超出时为 `output_too_large` |
| `max_objects` | 1 024 | 每个 job 的输入对象数、每个 attempt 的结果对象数、每个 worker 的 interface 数和 held lease 数 | 输入或 Hello：`INVALID_ARGUMENT`；结果：`output_too_large` |
| `max_identifier_bytes` | 256 | job_id、interface、worker_id、resume_session_id 的长度 | `INVALID_ARGUMENT` |

## JournalConfig（配置 `journal` 后启用 durable）

| 字段 | 默认值 | 作用 | 超限或故障时 |
|---|---|---|---|
| `path` | （CLI：`.anyserve/journal.sqlite`） | SQLite 文件路径；同一路径同时只允许一个进程使用 | 第二个进程启动失败 |
| `full_sync` | true | true 为 `synchronous=FULL`，false 为 `NORMAL`（降低断电时的持久性保证） | — |
| `queue_capacity` | 4 096 | 等待提交的操作数 | durable 操作返回 `RESOURCE_EXHAUSTED` |
| `max_batch` | 256 | 一次组提交最多合并的操作数 | — |
| `retention` | 7 天 | 终态 durable job 在 journal 中保留的时间，按批（每批 500）清理 | 之后 `get_job` 返回 `NotFound` |
| `max_bytes` | 无 | 数据库大小上限 | 写入回滚，durable 被拒绝且 readiness 变为 false；ephemeral 不受影响；可写后自动恢复 |
