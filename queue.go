package coordination

import (
	"context"
	"errors"
	"time"

	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
)

// ErrQueueEmpty 表示队列里没有可见的任务。
var ErrQueueEmpty = errors.New("queue is empty")

// QueueItem 是一次成功租约到的队列元素。
type QueueItem struct {
	Queue   string
	ID      string
	Payload string
	Token   storage.Version
	Owner   string
	lease   *storage_lock.StorageLock
}

// Ack 确认元素已处理并释放它的租约。
func (i *QueueItem) Ack(ctx context.Context) error { return i.lease.UnLock(ctx, i.Owner) }

// Context 返回处理期间的业务上下文，持有权丢失时取消。
func (i *QueueItem) Context(ctx context.Context) (context.Context, context.CancelFunc) {
	return i.lease.OwnedContext(ctx)
}

// Queue 是可见性超时队列。
// Push 写入一条独立租约记录；Claim 在租约过期或无人持有时抢占，处理期间由看门狗续租。
// 处理方崩溃后租约到期，元素自动重新可见。
type Queue struct {
	storage   storage.Storage
	name      string
	options   storage_lock.StorageLockOptions
	clock     func() time.Time
	nextIndex int
}

// NewQueue 创建队列。name 是全集群共享的队列名。
func NewQueue(s storage.Storage, name string, options storage_lock.StorageLockOptions) (*Queue, error) {
	if s == nil {
		return nil, storage_lock.ErrStorageNil
	}
	if name == "" {
		return nil, storage_lock.ErrLockIdEmpty
	}
	return &Queue{storage: s, name: name, options: options, clock: time.Now}, nil
}

// Push 追加一个元素。id 在队列内必须唯一，重复返回 storage_lock.ErrLockBusy。
func (q *Queue) Push(ctx context.Context, id, payload string) error {
	if id == "" {
		return storage_lock.ErrLockIdEmpty
	}
	lock, err := q.itemLock(id)
	if err != nil {
		return err
	}
	owner := queueOwner(q.name, id, payload)
	if _, err := lock.Inspect(ctx); err == nil {
		return storage_lock.ErrLockBusy
	} else if !errors.Is(err, storage_lock.ErrLockNotFound) {
		return err
	}
	if err := lock.TryLock(ctx, owner); err != nil {
		return err
	}
	// 入队后立即释放，留下墓碑记录承载 payload；LockCount 为 0 表示可见。
	return lock.UnLock(ctx, owner)
}

// Claim 抢占一个可见元素。没有可见元素时返回 ErrQueueEmpty。
func (q *Queue) Claim(ctx context.Context, owner string) (*QueueItem, error) {
	if owner == "" {
		return nil, storage_lock.ErrOwnerIdEmpty
	}
	items, err := storage_lock.ListLocks(ctx, q.storage, q.prefix())
	if err != nil {
		return nil, err
	}
	for items.Next() {
		info := items.Value()
		if info == nil || info.LockCount > 0 && info.LeaseExpireTime.After(q.clock()) {
			continue
		}
		id := info.LockId[len(q.prefix()):]
		lock, err := q.itemLock(id)
		if err != nil {
			return nil, err
		}
		if err := lock.TryLock(ctx, owner); err != nil {
			if errors.Is(err, storage_lock.ErrLockBusy) {
				continue
			}
			return nil, err
		}
		token, err := lock.GetFencingToken(ctx, owner)
		if err != nil {
			_ = lock.UnLock(context.WithoutCancel(ctx), owner)
			return nil, err
		}
		_, payload := splitQueueOwner(info.OwnerId)
		return &QueueItem{Queue: q.name, ID: id, Payload: payload, Token: token, Owner: owner, lease: lock}, nil
	}
	return nil, ErrQueueEmpty
}

func (q *Queue) prefix() string { return "queue-" + q.name + "-" }

func (q *Queue) itemLock(id string) (*storage_lock.StorageLock, error) {
	options := q.options
	options.LockId = q.prefix() + id
	return storage_lock.NewStorageLockWithOptions(q.storage, &options)
}

func queueOwner(queue, id, payload string) string { return "queue-owner:" + payload }

func splitQueueOwner(owner string) (string, string) {
	const prefix = "queue-owner:"
	if len(owner) >= len(prefix) && owner[:len(prefix)] == prefix {
		return "", owner[len(prefix):]
	}
	return "", ""
}

// Peek 列出当前可见（未被任何人有效持有）的元素，只读不抢占。
//
// 存在的意义：Agent 是无状态的，每次调用都是新进程。
// 没有它，调用方只能靠"试一次 Claim"来猜队列里有没有活干，
// 而 Claim 一旦成功就已经占用了元素——观测不该产生副作用。
func (q *Queue) Peek(ctx context.Context) ([]PendingItem, error) {
	items, err := storage_lock.ListLocks(ctx, q.storage, q.prefix())
	if err != nil {
		return nil, err
	}
	result := make([]PendingItem, 0)
	now := q.clock()
	for items.Next() {
		info := items.Value()
		if info == nil {
			continue
		}
		visible := info.LockCount == 0 || !info.LeaseExpireTime.After(now)
		if !visible {
			continue
		}
		_, payload := splitQueueOwner(info.OwnerId)
		result = append(result, PendingItem{
			ID:      info.LockId[len(q.prefix()):],
			Payload: payload,
			Held:    info.LockCount > 0,
		})
	}
	return result, nil
}

// AckByOwner 按 "元素 id + owner" 确认完成，等价于 QueueItem.Ack。
//
// 为什么需要它：Claim 与 Ack 常常发生在**不同进程**里（Agent 每次调用都是新进程，
// 甚至可能由别的 Agent 接手）。只有 QueueItem 的话，调用方必须把整个对象留在内存里，
// 跨进程就做不到。owner 是租约的凭证，对不上时 UnLock 会拒绝，
// 因此这不会让任何人"顺手删掉别人的元素"。
func (q *Queue) AckByOwner(ctx context.Context, id, owner string) error {
	if id == "" {
		return storage_lock.ErrLockIdEmpty
	}
	if owner == "" {
		return storage_lock.ErrOwnerIdEmpty
	}
	lock, err := q.itemLock(id)
	if err != nil {
		return err
	}
	return lock.UnLock(ctx, owner)
}
