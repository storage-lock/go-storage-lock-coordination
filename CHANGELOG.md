# Changelog

## Unreleased

### 修复

- **`TaskRun.Complete` 先释放租约再写 done 标记**：中间窗口内别的进程能抢到同一
  "一次性任务"，让 `OnceTask` 承诺的"全区只执行一次"失效。改为先写标记再释放租约；
  崩溃兜底不受影响（标记没写成时租约仍在，过期后他人可接管重试）。
- `RateLimiter.TryAcquire` 令牌不足时返回 `ErrLockBusy`，调用方必须知道内部实现
  才能判断，日志里也会显示成"锁被占用"。新增语义正确的 `ErrNoTokens` 与
  `IsNoTokens`（后者同时识别旧的 `ErrLockBusy`，兼容既有判断）。

### 新增

- `Queue.Peek` / `Queue.AckByOwner`、`DelayedQueue.AckByOwner`：
  Agent 每次调用都是新进程，`Claim` 与 `Ack` 常跨进程发生，只暴露对象做不到。
  `Peek` 还让"看一眼队列"不再产生副作用。
- `AtomicLong.Set`：运维手动纠正计数器值（业务状态机仍应使用 `CompareAndSet`）。
- `PendingItem` 补 JSON tag，便于直接输出给调用方解析。

## v0.1.0
_发布日期：2026-11-18_

- build: 升级依赖到 go-storage/go-storage-lock/go-memory-storage v0.1.0

## v0.0.3
_发布日期：2026-10-23_

- feat: add distributed counters queues and rate limiting

## v0.0.2
_发布日期：2026-10-23_

- build: consume published go-events v0.0.5

## v0.0.1
_发布日期：2026-10-12_

- build: 依赖改为已发布版本，移除本地 replace

## v0.0.1-pre

- chore: 补 MIT LICENSE
