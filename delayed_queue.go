package coordination

import (
	"context"
	"errors"
	"time"

	storage "github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
)

var (
	// ErrInvalidDelay 表示延迟必须为正。
	ErrInvalidDelay = errors.New("delay must be greater than zero")
)

// DelayedItem 是一次成功抢占到的、已到可见时间的延迟元素。
type DelayedItem struct {
	Queue   string
	ID      string
	Payload string
	ReadyAt time.Time
	Token   storage.Version
	Owner   string
	lease   *storage_lock.StorageLock
}

// Ack 确认元素已处理并释放它的租约；释放后元素成为墓碑，不再可见。
func (i *DelayedItem) Ack(ctx context.Context) error { return i.lease.UnLock(ctx, i.Owner) }

// Context 返回处理期间的业务上下文，持有权丢失时取消。
func (i *DelayedItem) Context(ctx context.Context) (context.Context, context.CancelFunc) {
	return i.lease.OwnedContext(ctx)
}

// Renew 手动延长处理租约。看门狗开启时不需要调用。
func (i *DelayedItem) Renew(ctx context.Context) error { return i.lease.Renew(ctx, i.Owner) }

// DelayedQueue 是分布式延迟队列。
//
// Push 写入一条独立租约记录，租约时长等于延迟；延迟未到之前元素保持"隐藏"
// （LockCount=1 且租约未过期），Claim 会跳过。延迟到点后租约自然过期，
// 任何客户端都可以抢占该元素，无需持有者主动释放。Ack 后记录变为墓碑
// （LockCount=0），不再可见。
//
// 元素到点后没有显式"激活"步骤：到期即重可见，由 Claim 的抢占路径完成。
type DelayedQueue struct {
	storage storage.Storage
	name    string
	options storage_lock.StorageLockOptions
	clock   func() time.Time
}

// NewDelayedQueue 创建延迟队列。name 是全集群共享的队列名。
func NewDelayedQueue(s storage.Storage, name string, options storage_lock.StorageLockOptions) (*DelayedQueue, error) {
	if s == nil {
		return nil, storage_lock.ErrStorageNil
	}
	if name == "" {
		return nil, storage_lock.ErrLockIdEmpty
	}
	if options.LeaseExpireAfter == 0 {
		options = *storage_lock.NewStorageLockOptions()
	}
	return &DelayedQueue{storage: s, name: name, options: options, clock: time.Now}, nil
}

// Push 追加一个带延迟的元素。id 在队列内必须唯一，重复返回 storage_lock.ErrLockBusy。
func (q *DelayedQueue) Push(ctx context.Context, id, payload string, delay time.Duration) error {
	if id == "" {
		return storage_lock.ErrLockIdEmpty
	}
	if delay <= 0 {
		return ErrInvalidDelay
	}
	// 隐藏期必须禁用看门狗：看门狗会把租约一直续下去，延迟就永远不会到点。
	lock, err := q.itemLock(id, true)
	if err != nil {
		return err
	}
	if _, err := lock.Inspect(ctx); err == nil {
		return storage_lock.ErrLockBusy
	} else if !errors.Is(err, storage_lock.ErrLockNotFound) {
		return err
	}
	owner := delayedOwner(payload)
	if err := lock.TryLock(ctx, owner); err != nil {
		return err
	}
	// 把可见时刻（now + delay）写进 LeaseExpireTime。
	// 锁租约本身仍有 >=3s 下限（StorageLock 约束），但可见性只认 LeaseExpireTime，
	// 因此 delay 可以小于 3s；租约只作为"永不激活"的兜底。
	return q.writeReadyAt(ctx, lock, q.prefix()+id, owner, delay)
}

// writeReadyAt 在 TryLock 成功后把记录的 LeaseExpireTime 改写为 now + delay，
// 使元素在延迟到点前保持隐藏（Claim 跳过 LeaseExpireTime 在未来 的记录）。
func (q *DelayedQueue) writeReadyAt(ctx context.Context, lock *storage_lock.StorageLock, lockID, owner string, delay time.Duration) error {
	status, err := lock.Inspect(ctx)
	if err != nil {
		_ = lock.UnLock(context.WithoutCancel(ctx), owner)
		return err
	}
	now, err := q.storage.GetTime(ctx)
	if err != nil {
		_ = lock.UnLock(context.WithoutCancel(ctx), owner)
		return err
	}
	info := status.Information
	next := info
	next.LeaseExpireTime = now.Add(delay)
	next.Version++
	return q.storage.UpdateWithVersion(ctx, lockID, info.Version, next.Version, &next)
}

// Claim 抢占一个已到可见时间的元素。没有可领元素时返回 ErrQueueEmpty。
// 抢占后元素由看门狗续租，处理方崩溃后租约到期、元素重新可见。
func (q *DelayedQueue) Claim(ctx context.Context, owner string) (*DelayedItem, error) {
	if owner == "" {
		return nil, storage_lock.ErrOwnerIdEmpty
	}
	items, err := storage_lock.ListLocks(ctx, q.storage, q.prefix())
	if err != nil {
		return nil, err
	}
	for items.Next() {
		info := items.Value()
		if info == nil {
			continue
		}
		if info.LockCount == 0 {
			// 已 Ack 的墓碑，不再可见。
			continue
		}
		if !info.LeaseExpireTime.IsZero() && info.LeaseExpireTime.After(q.clock()) {
			// 延迟未到，隐藏。
			continue
		}
		id := info.LockId[len(q.prefix()):]
		payload := splitDelayedOwner(info.OwnerId)
		readyAt := info.LeaseExpireTime
		// 抢占后按调用方配置（默认开启看门狗）续租：处理方崩溃后租约到期、元素重新可见。
		lock, err := q.itemLock(id, q.options.DisableWatchDog)
		if err != nil {
			return nil, err
		}
		if err := lock.TryLock(ctx, owner); err != nil {
			if errors.Is(err, storage_lock.ErrLockBusy) || errors.Is(err, storage_lock.ErrVersionMiss) || errors.Is(err, storage_lock.ErrLockAlreadyExists) {
				continue
			}
			return nil, err
		}
		token, err := lock.GetFencingToken(ctx, owner)
		if err != nil {
			_ = lock.UnLock(context.WithoutCancel(ctx), owner)
			return nil, err
		}
		return &DelayedItem{Queue: q.name, ID: id, Payload: payload, ReadyAt: readyAt, Token: token, Owner: owner, lease: lock}, nil
	}
	return nil, ErrQueueEmpty
}

// PendingItem 是 Peek 返回的队列观测条目。
// Queue 与 DelayedQueue 共用这个类型：Queue 不使用 ReadyAt（其元素没有延迟概念）。
type PendingItem struct {
	ID      string    `json:"id"`
	Payload string    `json:"payload,omitempty"`
	ReadyAt time.Time `json:"ready_at,omitzero"`
	Held    bool      `json:"held"`
}

// Peek 列出当前可见（延迟已到且未被领走）的元素，只读不抢占。
// 返回值与列表快照一致，不保证与随后 Claim 的结果一一对应。
func (q *DelayedQueue) Peek(ctx context.Context) ([]PendingItem, error) {
	items, err := storage_lock.ListLocks(ctx, q.storage, q.prefix())
	if err != nil {
		return nil, err
	}
	result := make([]PendingItem, 0)
	now := q.clock()
	for items.Next() {
		info := items.Value()
		if info == nil || info.LockCount == 0 {
			continue
		}
		if info.LeaseExpireTime.After(now) {
			continue
		}
		result = append(result, PendingItem{
			ID:      info.LockId[len(q.prefix()):],
			Payload: splitDelayedOwner(info.OwnerId),
			ReadyAt: info.LeaseExpireTime,
			Held:    info.LockCount > 0,
		})
	}
	return result, nil
}

// AckByOwner 按 "元素 id + owner" 确认完成，等价于 DelayedItem.Ack。
//
// Claim 与 Ack 通常发生在不同进程里（Agent 每次调用都是新进程），
// 只有 DelayedItem 的话调用方必须把对象留在内存里，跨进程做不到。
// owner 对不上时底层 UnLock 会拒绝，不会误删别人的元素。
func (q *DelayedQueue) AckByOwner(ctx context.Context, id, owner string) error {
	if id == "" {
		return storage_lock.ErrLockIdEmpty
	}
	if owner == "" {
		return storage_lock.ErrOwnerIdEmpty
	}
	lock, err := q.itemLock(id, q.options.DisableWatchDog)
	if err != nil {
		return err
	}
	return lock.UnLock(ctx, owner)
}

func (q *DelayedQueue) prefix() string { return "dq-" + q.name + "-" }

func (q *DelayedQueue) itemLock(id string, disableWatchDog bool) (*storage_lock.StorageLock, error) {
	options := q.options
	options.LockId = q.prefix() + id
	if disableWatchDog {
		options.DisableWatchDog = true
	}
	return storage_lock.NewStorageLockWithOptions(q.storage, &options)
}

func delayedOwner(payload string) string { return "dq-owner:" + payload }

func splitDelayedOwner(owner string) string {
	const prefix = "dq-owner:"
	if len(owner) >= len(prefix) && owner[:len(prefix)] == prefix {
		return owner[len(prefix):]
	}
	return ""
}
