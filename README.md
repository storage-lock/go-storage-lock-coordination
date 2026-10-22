# go-storage-lock-coordination

在 `go-storage` 的单 key 线性一致 CAS、可靠时间源和租约锁之上提供有限的协调原语。

## 已实现

- **Semaphore**：固定容量 slot 信号量。每个 permit 是独立的 `StorageLock`，具有独立 slot 和 fencing token；不承诺 FIFO，也没有跨 slot 的全局 token。`SemaphoreOptions.PermitTTL > 0` 时获得**可过期许可**：到期由 watcher 主动释放 slot，其他客户端无需等待持有者释放即可抢占——适合"必须在限时内用完许可"的场景。
- **Leader Election**：基于单个租约锁的选主。`Leadership` 提供显式 `Renew`、`Resign`、`Lost` 和 fencing token。
- **OrderedCompositeLock**：按 lock ID 字典序逐个获取的组合锁。全部成功后才返回，失败时逆序尽力释放；它不是跨 key 原子事务。
- **OnceTask**：同一个任务标识全集群只成功执行一次。执行中持有租约和 fencing token，`Complete` 后永久拒绝，`Abandon` 后允许接管且 token 继续递增。
- **Queue**：可见性超时队列。`Claim` 独占一个元素并返回 fencing token，处理方崩溃后租约到期，元素重新可见。
- **DelayedQueue**：延迟队列。`Push` 写入元素并声明延迟，延迟到点前元素隐藏（`Claim` 返回空），到点后任意客户端可抢占；`Ack` 后元素成为墓碑不再可见。处理方崩溃后租约到期、元素重新可见。
- **RateLimiter**：分布式令牌桶限流器。桶状态由单条记录以 CAS 演进（值放 `LockCount`、上次补充时间放 `LockBeginTime`），`TryAcquire` 原子放行或拒绝，支持整数令牌、突发容量与手动 `Refill`；不感知调用者身份，只提供次数限流。
- **AtomicLong**：分布式原子计数器。基于 CAS 循环的 `IncrementAndGet` / `GetAndAdd` / `CompareAndSet` / `Reset`，跨节点线性一致，可安全用于库存扣减、发号、统计等场景。

所有新能力都要求底层 Storage 的 CAS 真正线性一致，并使用可靠、单调的时间源。Watch 只能优化等待，不能作为正确性依据。业务资源必须原子校验 fencing token；token 不是锁有效性的持续证明。

## 明确不实现的语义

当前模型中的 `LockInformation` 只有单个 owner 和重入计数，不能安全表达参与者集合、幂等 countDown、屏障代际或 broken/reset 状态。因此本模块不提供伪装成完整分布式语义的 CountDownLatch/CyclicBarrier。

本模块也不实现 RedLock。多个独立后端的多数成功无法在网络分区、异步复制、进程暂停、延迟和时钟不确定时证明线性一致 ownership。需要严格全有全无的多 key 锁时，应使用单一协调记录、事务性数据库接口或 etcd/ZooKeeper 等共识租约。

## 运行测试

```bash
go test ./...
go test -race ./...
go vet ./...
```
