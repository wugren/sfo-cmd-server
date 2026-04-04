# Client / Server 设计说明

## 1. 范围

本文基于当前仓库实现，梳理 `sfo-cmd-server` 的 client/server 设计，覆盖：

- 协议帧与 body 生命周期
- 默认 client 与 classified client 的建连、收发、选路
- server 的接入、连接管理、发送语义
- 并发约束与已知实现风险
- 当前测试覆盖

相关代码：

- `src/cmd.rs`
- `src/client/client.rs`
- `src/client/classified_client.rs`
- `src/server/server.rs`
- `src/server/peer_manager.rs`
- `tests/communication_matrix.rs`

## 2. 协议模型

所有消息都按 `header_len(u8) + CmdHeader + body` 发送，其中 `header_len` 是 `CmdHeader` 编码后的字节长度，因此 header 本身必须满足 `<= 255`。

`CmdHeader<LEN, CMD>` 包含：

- `pkg_len`: body 长度
- `version`: 协议版本
- `cmd_code`: 命令码
- `is_resp`: 是否响应包
- `seq`: 请求序号；仅请求-响应模式使用

协议的两层泛型边界分别负责：

- `LEN`: body 长度字段的编码类型，例如 `u16`、`u32`
- `CMD`: 命令码编码类型，例如 `u8`、`u16` 或自定义可编解码类型

## 3. Body 生命周期

`CmdBody` 既可以承载内存中的字节，也可以承载流式 reader。

接收侧真正关键的是 `CmdBodyRead`：

- 它把当前 tunnel 的读半边包装成一个限长 reader
- handler 读取多少，就推进多少偏移
- 如果 handler 没把 body 读完，`Drop` 会异步 drain 剩余字节
- body 被完整消费或 drain 完成后，读半边才会归还给接收循环

这个设计保证了“单条连接上连续读取多帧”不会因为上一个 handler 只读了部分 body 而错位。

## 4. Tunnel 抽象

底层传输通过三类 trait 抽象：

- `CmdTunnelRead<M>`
- `CmdTunnelWrite<M>`
- `CmdTunnelMeta`

读写半边都要提供：

- 本地 `PeerId`
- 远端 `PeerId`
- 可选 `tunnel_meta`

因此上层只需要关心命令收发与响应关联，不关心底层是 TCP、TLS 还是测试中的内存双工流。

## 5. Client 设计

### 5.1 默认 client

默认实现是 `DefaultCmdClient`。内部核心状态有三类：

- `CmdHandlerMap`
- `RespWaiter`
- `ClassifiedWorkerPool<TunnelId, CommonCmdSend, CmdWriteFactory>`

虽然底层用了 classified worker pool，但默认 client 只把 `TunnelId` 当作 worker key。`tunnel_count` 控制池子最多维持多少条 tunnel。

`CmdWriteFactory::create()` 会：

1. 调用用户提供的 `CmdTunnelFactory::create_tunnel()`
2. 为新 tunnel 分配 `tunnel_id`
3. 拆分读写半边
4. 启动该 tunnel 的后台接收任务
5. 封装成 `CommonCmdSend`

接收任务按如下流程循环：

1. 读取 `header_len`
2. 解码 `CmdHeader`
3. 构造限长 `CmdBody`
4. 若是响应包，则按 `gen_resp_id(tunnel_id, cmd, seq)` 投递给 `RespWaiter`
5. 若是普通请求，则查 `CmdHandlerMap` 执行 handler
6. handler 返回 `Some(CmdBody)` 时，自动沿原 `cmd/seq/version` 回包
7. 等待 body 被消费或 drain 完成，再继续读取下一帧

发送侧能力集中在 `CommonCmdSend`：

- `send` / `send_with_resp`
- `send_parts` / `send_parts_with_resp`
- `send_cmd` / `send_cmd_with_resp`
- 兼容别名 `send2*`，但已 deprecated，当前主接口是 `send_parts*`

同一条 tunnel 的 writer 通过 `ObjectHolder` 串行化，避免并发写导致 header/body 交叉。

`send*_with_resp` 还显式禁止在当前 tunnel 的接收任务里同步等待响应；若检测到调用方就是该 tunnel 的 recv task，会直接返回错误，避免自阻塞。

### 5.2 Classified client

`DefaultClassifiedCmdClient` 在收发模型上与默认 client 基本一致，差异主要是选路键变成：

`CmdClientTunnelClassification<C> { tunnel_id, classification }`

行为分成三类：

- 指定 `tunnel_id`：只命中已存在的精确 tunnel，不会为该 `tunnel_id` 新建 tunnel
- 指定 `classification`：从匹配分类的 tunnel 中取一个；若不存在，可由 factory 新建对应分类 tunnel
- 两者都不指定：退化为任意可用 tunnel

因此 classified client 本质上是在默认 client 之上增加“按分类建连/选路”的能力，没有引入新的协议语义。

## 6. Server 设计

server 由三层组成：

- `DefaultCmdServerIncoming`: accept loop
- `DefaultCmdServerService`: 连接管理、收包、发包、响应关联
- `DefaultCmdServer`: 对外组合对象

### 6.1 接入与连接管理

`DefaultCmdServerIncoming::run()` 持续从 `CmdTunnelListener` 接收新 tunnel，并将其交给 `DefaultCmdServerService::handle_tunnel()`。

`DefaultCmdServerService::serve_tunnel()` 会：

1. 为新连接分配 `tunnel_id`
2. 拆分读写半边
3. 启动该 tunnel 的接收任务
4. 封装成 `PeerConnection`
5. 交给 `PeerManager` 纳管

`PeerManager` 维护两份索引：

- `TunnelId -> PeerConnection`
- `PeerId -> Vec<TunnelId>`

并负责在：

- 某个 `peer_id` 的首条连接建立时触发 `on_peer_connected`
- 该 `peer_id` 的最后一条连接断开时触发 `on_peer_disconnected`

### 6.2 收包与分发

server 每条 tunnel 同样有独立接收任务，流程和 client 基本对称：

- 响应包：按 `resp_id` 投递到 `RespWaiter`
- 普通请求：查 `CmdHandlerMap` 执行 handler
- handler 返回 `Some(CmdBody)` 时自动回包

server 额外维护了 `NamedStateHolder<tokio::task::Id>`，用于标记“当前 task 正在处理入站命令”。带响应的发送接口在发现自己正处于该状态时，会拒绝同步等待响应，避免典型死锁：

- 接收循环正在执行 handler
- handler 内再次发起 `send*_with_resp`
- 接收循环在 handler 返回前不会继续读取后续帧
- 响应无法被当前连接读到

因此，handler 内如果需要反向请求对端，应尽快 `spawn` 独立任务。

### 6.3 发送语义

server 当前暴露三类目标范围：

- 按 `peer_id` 发送
- 按 `tunnel_id` 精确发送
- 按 `peer_id` 广播到全部 tunnel

当前具体语义如下：

- `send` / `send_parts` / `send_cmd`
  - 先取 `peer_id` 下的连接列表
  - 为空时返回 `PeerConnectionNotFound`
  - 否则按连接顺序串行 failover，首个成功即返回
  - 全部失败时返回最后一个错误

- `send_with_resp`
  - 同样按连接顺序尝试
  - 若当前 task 正在处理入站命令，会跳过该连接
  - 某个连接发送成功且收到响应后立即返回
  - 全部失败或都不可用时，返回通用 `Failed`

- `send_cmd` / `send_cmd_with_resp`
  - 当前实现已走流式 `tokio::io::copy`
  - 不会先把 `CmdBody` 完整读入内存
  - 大 body 的资源行为和 client 侧保持一致

- `send_by_specify_tunnel*`
  - 通过 `TunnelId` 直接查 `PeerManager`
  - tunnel 不存在时返回 `PeerConnectionNotFound`
  - 当前实现不会再校验该 tunnel 是否属于传入的 `peer_id`

- `send_by_all_tunnels` / `send_parts_by_all_tunnels`
  - 对当前 `peer_id` 下的每条 tunnel 逐个尝试
  - 单条 tunnel 失败只记日志，不影响整体返回
  - 即使没有任何 tunnel，也会返回 `Ok(())`
  - 语义是明确的 best-effort broadcast

## 7. 响应关联

client/server 共用 `gen_resp_id()` 生成 waiter key。

当前实现不是简单截断 `CMD` 编码，而是：

1. 先拿到 `CMD` 的原始编码字节
2. 将各个 8-byte chunk 混入一个 `u64` 指纹
3. 再与 `tunnel_id`、`seq` 拼成 `u128`

逻辑上可理解为：

- 高 32 bit：`tunnel_id`
- 中间 32 bit：`seq`
- 低 64 bit：`cmd` 编码的混合指纹

这保证了：

- 不同 tunnel 的响应不会串
- 同一 tunnel 内不同 `seq` 的响应不会串
- `CMD` 超过 8 字节时仍会参与混合，而不是只保留前/后 8 字节

仓库已有单元测试覆盖 `seq`、`tunnel_id`、不同命令码，以及长命令码尾部变化的区分能力。

## 8. 并发约束

- 同一条 tunnel 的读循环是串行的；一个 body 未完成消费前，不会进入下一帧。
- 不同 tunnel 之间天然并行；每条 tunnel 各自有独立接收任务。
- 同一条 tunnel 的写通过 `ObjectHolder` 串行化。
- handler 可以只读取部分 body，但必须依赖 `CmdBodyRead` 的完成/自动 drain 机制恢复流状态。
- 带响应的同步发送不能在“当前入站命令处理 task”里直接等待，否则会被 client/server 的保护逻辑拒绝。

## 9. 当前测试覆盖

当前仓库已不再是“0 测试”状态。主要覆盖集中在 `tests/communication_matrix.rs`，并补充了 `src/client/mod.rs` 中的 `gen_resp_id` 单元测试。

已明确覆盖的场景包括：

- default client <-> server 双向请求/响应
- classified client <-> server 双向请求/响应
- default node / classified node 与 server 的互通
- client、classified client、server、node 的主要发送接口矩阵
- timeout 行为
- missing peer 时各发送接口的返回语义
- server 多 tunnel failover
- broadcast 的 best-effort 语义
- `send_cmd` / `send_cmd_with_resp` 的大 body 流式发送
- 长命令码参与 `resp_id` 计算

因此，文档中此前关于“未覆盖 failover / broadcast / 大 body / 长命令码”的结论已经不再成立。

## 10. 当前剩余风险

### 10.1 server 发送接口的错误语义仍不完全统一

`send` / `send_parts` / `send_cmd` 在无连接时返回 `PeerConnectionNotFound`，全部连接失败时返回最后一个错误；但 `send_with_resp` / `send_parts_with_resp` 在所有连接都失败或都被跳过时，最终返回的是较泛化的 `Failed`。

这会让调用方更难区分：

- 根本没有可用连接
- 某条连接写失败
- 因当前 task 处于 handler 上下文而被跳过

### 10.2 广播接口是 best-effort，但返回类型仍是普通 `CmdResult<()>`

`send_by_all_tunnels` 与 `send_parts_by_all_tunnels` 当前即使：

- `peer_id` 没有任何连接
- 某些连接甚至全部连接发送失败

也会返回 `Ok(())`，失败仅体现在日志里。

这个行为本身是明确的，但接口名和返回类型不会把“best-effort、不可感知部分失败”直接暴露给调用方。

### 10.3 指定 tunnel 的 server 接口没有校验 `peer_id`

`send_by_specify_tunnel*` / `send_cmd_by_specify_tunnel*` 当前只按 `TunnelId` 查连接，传入的 `peer_id` 主要用于日志和 API 形状对齐，不参与实际校验。

如果调用方手里持有某个有效 `tunnel_id`，即使给错 `peer_id`，发送仍会命中该 tunnel。

### 10.4 `tunnel_meta` 的来源在不同 client 实现间仍不一致

- 默认 client 在建 tunnel 时从 write half 读取 `tunnel_meta`
- classified client 在建 tunnel 时从 read half 读取 `tunnel_meta`

如果某种 tunnel 实现的 read/write half 暴露出的 meta 不完全对称，调用方可见行为会不一致。

## 11. 建议

后续如果要继续收敛语义，优先级建议如下：

1. 统一 server 各发送接口在“无连接 / 跳过 / 写失败 / 超时”下的错误分类。
2. 明确 broadcast 是否要保持 best-effort；若保持，最好在命名或返回类型上体现。
3. 为 `send_by_specify_tunnel*` 增加 `peer_id` 一致性校验，或在 API 文档里明确说明忽略 `peer_id`。
4. 统一 `tunnel_meta` 的权威来源。
