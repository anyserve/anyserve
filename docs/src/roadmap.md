# 定位与路线图（2026-10-23）

本页回答一个问题：anyserve 适不适合做边缘与具身智能的推理引擎？结论、依据和
下一步都在这里。它是方向文档，不扩展 [架构页](architecture.md) 定义的 API 或拓扑；
路线图上的每一项都要先过 [测试](testing.md) 的门禁和一次预注册的实验，才算进入支持范围。

## 结论先行

1. anyserve 不是推理引擎，也不应该变成推理引擎。它不加载模型、不做批处理、
   不理解张量；这些能力属于 worker 里的 ONNX Runtime、TensorRT、vLLM 或任何应用代码。
2. anyserve 适合做**具身与边缘推理的调度运行时**：把一次推理调用从设备路由到最合适的
   worker（本机 GPU、边缘机柜、云），带截止期、就近优先、超时溢出和零错误故障转移。
   这一层在推理引擎之上、集群编排之下，目前是空的，而且已有实测证据。
3. 下一步不是加功能，而是用一个真实的具身工作负载做一次预注册实验（下文 R1），
   再决定 SDK 和构建上的三项投入（R2–R4）。

## 1. 它现在是什么

一个 Rust 单进程控制面，worker 出站连接，可选 SQLite journal。核心契约只有六个
Client RPC 和两条 Worker 流（[协议](https://github.com/anyserve/anyserve/blob/main/crates/anyserve-proto/protos/execution.proto)）。
已经测过的数字：

| 指标 | 结果 | 来源 |
|---|---|---|
| 控制面每次调用的额外延迟（真实 ONNX GPU 后端，同机） | p50 +0.91–0.95 ms，p99 +1.05–1.17 ms | [GPU 实测](https://github.com/anyserve/anyserve/blob/main/bench/gpu/REPORT-2026-10-21.md) Q1 |
| 跨主机分发（prefer A，5 ms 溢出，A/B 各 1 slot） | B 承接 48%，两轮 p50 差 <0.4%，零错误 | 同上 Q2 |
| 负载中杀掉一台 worker | 客户端可见错误 0，最多 2 次尝试，之后 100% 由 A 承接 | 同上 Q3 |
| 控制面吞吐（mock worker，16 vCPU，c=64） | Invoke 12.4k req/s，流式 4.1k req/s（26 万帧/s），durable 1.0k req/s | [负载基准](benchmark-results.md) |
| 控制面资源（GPU 实测期间） | CPU 约 0.25 核，RSS 峰值 114 MiB | GPU 实测 Q2 |

## 2. 边缘与具身智能：需求对照

| 具身/边缘的需求 | 现状 | 判断 |
|---|---|---|
| 感知 30 fps、策略 5–10 Hz、控制环 10–100 Hz | 同机 1 ms 开销，远低于一帧预算 | 满足。1 kHz 级的进程内控制环不属于任何 RPC 的范围 |
| 传感器流与视频帧的双工传输 | Invoke 流式输入 + Output 帧，`progress` 帧 | 满足，[视频示例](https://github.com/anyserve/anyserve/blob/main/examples/python/README.md)已覆盖 |
| 就近优先、超时后溢出到远端 | `preferred_attributes` + `spillover_after_ms` + `queue_timeout_ms` | 满足，Q2 实测 |
| 一台 GPU 掉线不影响机器人 | 租约、重试预算、`AttemptRestarted` | 满足，Q3 实测 |
| 设备在 NAT 后、无入站端口 | worker 出站连接 | 满足 |
| 截止期与优先级 | 三种超时 + 4 档优先级 | 满足；没有"丢弃过期帧"语义，用 ephemeral + `queue_timeout_ms` 代替 |
| 弱网或离线时设备继续工作 | 控制面是单进程；短暂重启可恢复 durable 任务，Invoke 丢失 | 部分满足。离线降级是客户端模式：按 `total_timeout_ms` 失败后调本地模型。需要 R1 证明 |
| ARM 设备、小内存 | 控制面 RSS 114 MiB；未在 aarch64 上构建或测量 | 未验证，R3 |
| asyncio / ROS 2 风格的集成 | Python SDK 是同步阻塞，一个进程一个 handler，不接受 async 函数 | 不满足，R2 |
| 模型加载、批处理、KV cache | 留给 worker | 正确，保持不做 |

## 3. 判断

**作为推理引擎：不合适，也不该做。** 第一，引擎是另一个产品，赛道上已有 ORT、
TensorRT、llama.cpp、vLLM，anyserve 没有任何优势。第二，2026-10-18 和 10-21 两次
决策把 OpenAI 网关、内置 worker 和一切后端语义移出核心，正是因为每一层后端语义都要
由单一 Runtime 保持一致；把模型语义放回来会推翻这两次决策。第三，"批处理在 worker 里做"
已经在 GPU 实测中验证：c=64 的 222 ms 是 Little 定律下 worker 容量的问题，不是调度问题。

**作为具身/边缘推理的调度运行时：合适。** 典型拓扑是"机器人 ↔ 边缘站点控制面 ↔
站点 GPU worker ↔ 云 GPU worker"：控制面放在站点（一个机柜一个进程），机器人是
客户端，GPU 机器是 worker。机器人本地模型不经过 anyserve；需要更大模型时发起带截止期
的调用，就近优先，超时溢出到云，失败则本地降级。这些机制已经存在并有数字。

**一句话定位**：anyserve 把一次推理调用路由到最合适的 worker，不运行模型。

## 4. 路线图

按顺序做；每一项写明验收问题，达不到就停。不做的事列在最后。

### R1 一个真实具身工作负载的预注册实验

- 拓扑：一台 ARM 客户端（Jetson 或 Graviton 实例）以 30 fps 发送 JPEG 帧；worker A 在
  边缘 T4，worker B 在云上；客户端 prefer A，`spillover_after_ms=20`，
  `queue_timeout_ms=100`，`total_timeout_ms=500`。
- Q4 端到端：30 fps 下客户端看到的 p50/p99，与进程内直接推理比较；判定 p50 增量 ≤ 3 ms
  （比 Q1 多一次跨主机 RTT）。
- Q5 离线降级：切断到控制面的网络 60 s。判定：客户端在 `total_timeout_ms` 内切到本地模型，
  零异常逃逸；恢复后 10 s 内远端承接比例回到切断前。
- Q6 资源：ARM 上控制面（若作为站点控制面部署在 ARM）的 RSS 与启动时间。
- 产物与 [GPU 实测](https://github.com/anyserve/anyserve/blob/main/bench/gpu/README.md) 相同：几个脚本和一份两页报告，不建框架。

### R2 SDK 面向具身栈

- Python：`AsyncAnyserveClient`（asyncio，同样的 `call`/`invoke_stream`）和
  `serve([h1, h2])`——一个 worker 进程多个 interface，协议的 `Hello.interfaces` 本来就是列表。
  async handler 可以在此基础上加，但先看 R1 需要什么。
- Rust：一个高层 `serve(handler)`。现在 [示例 worker](https://github.com/anyserve/anyserve/blob/main/examples/rust/src/main.rs) 和
  [基准 worker](https://github.com/anyserve/anyserve/blob/main/bench/load/src/worker.rs) 各自重实现了会话循环（Welcome、credit、心跳、
  取消、重连），抽成一个函数后两处都删掉。验收：两处 worker 代码各减少一半以上，
  `sdk_e2e` 和负载基准不变。
- 不加 tensor 类型系统：`numpy → bytes` 的约定放在示例里。

### R3 ARM 构建与体积

- CI 产出 aarch64 的控制面二进制和 Python wheel（交叉编译，Rust 没有障碍）。
- 在 R1 的 ARM 客户端上实测一次，把数字写进 R1 报告。

### R4 实时流语义（等证据）

- 候选是"最新帧优先、过期帧丢弃"。在 R1 之前不做：ephemeral + `queue_timeout_ms`
  已经覆盖"过期就失败"，是否需要"过期就跳过"要看 Q4 的 p99。

### 不做

- 模型加载、批处理、张量类型、KV cache 调度：worker 的事。
- 多个控制面共享状态或 Active/Active：单进程 + 短暂重启是有意的边界，
  见 [部署](deployment.md)。
- Kubernetes operator、自动扩缩：部署平台的事。
- 任何 HTTP/OpenAI 兼容网关：2026-10-21 已删除，不回来。
- 新的配置项：没有先出现在 [配置表](configuration.md) 里的开关不进代码。
