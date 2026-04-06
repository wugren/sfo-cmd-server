# sfo-cmd-server

Rust 命令通道库，提供 `client`、`classified client`、`node`、`classified node`、`server` 等抽象。

设计与当前发送语义说明见：

- [docs/client-server-design.md](./docs/client-server-design.md)

当前实现中，`client` / `node` 的 `send_by_specify_tunnel*` 与 `get_send(..., tunnel_id)` 已区分两类情况：

- tunnel 真实不存在：返回 `tunnel not found`
- tunnel 存在但正被其他 guard 持有：在库内部等待其归还，不再误报 `not found`
