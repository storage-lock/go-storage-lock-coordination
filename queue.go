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
