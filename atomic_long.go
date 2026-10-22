package coordination

import (
	"context"
	"errors"
	"math"
	"strings"
	"time"

	storage "github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
)

var (
	// ErrAtomicNamespaceEmpty 表示原子计数器的 namespace 不能为空。
	ErrAtomicNamespaceEmpty = errors.New("atomic counter namespace must not be empty")
)

// AtomicLong 是分布式原子计数器。
//
// 计数由一条 StorageLock 记录承载：值放在 LockCount 字段（int64），版本号
// 承载乐观锁，LockBeginTime 记录最近一次更新时刻。所有读写操作都是基于
// CreateWithVersion / UpdateWithVersion 的 CAS 循环，跨节点线性一致，
// 可安全用于库存扣减、发号、统计等场景。
type AtomicLong struct {
	storage   storage.Storage
	namespace string
}

// NewAtomicLong 创建分布式原子计数器。namespace 是全集群共享的计数名。
func NewAtomicLong(s storage.Storage, namespace string) (*AtomicLong, error) {
	if s == nil {
		return nil, storage_lock.ErrStorageNil
	}
	if strings.TrimSpace(namespace) == "" {
		return nil, ErrAtomicNamespaceEmpty
	}
	return &AtomicLong{storage: s, namespace: namespace}, nil
}

// Namespace 返回计数器 namespace。
func (c *AtomicLong) Namespace() string { return c.namespace }

// CounterID 返回计数器对应的存储记录 ID。
func (c *AtomicLong) CounterID() string { return "atomic-" + c.namespace }

// Get 返回当前计数值。从未写入时返回 0。
func (c *AtomicLong) Get(ctx context.Context) (int64, error) {
	value, _, _, err := c.readState(ctx)
	return value, err
}

// IncrementAndGet 自增 1 并返回新值。
func (c *AtomicLong) IncrementAndGet(ctx context.Context) (int64, error) {
	return c.AddAndGet(ctx, 1)
}

// GetAndIncrement 返回旧值并把计数值 +1。
func (c *AtomicLong) GetAndIncrement(ctx context.Context) (int64, error) {
	return c.GetAndAdd(ctx, 1)
}

// DecrementAndGet 自减 1 并返回新值。
func (c *AtomicLong) DecrementAndGet(ctx context.Context) (int64, error) {
	return c.AddAndGet(ctx, -1)
}

// GetAndDecrement 返回旧值并把计数值 -1。
func (c *AtomicLong) GetAndDecrement(ctx context.Context) (int64, error) {
	return c.GetAndAdd(ctx, -1)
}

// AddAndGet 加上 delta 并返回新值。
func (c *AtomicLong) AddAndGet(ctx context.Context, delta int64) (int64, error) {
	for {
		value, version, since, err := c.readState(ctx)
		if err != nil {
			return 0, err
		}
		next := value + delta
		if err := c.write(ctx, version, next, since); err != nil {
			if isRetryableRateErr(err) {
				continue
			}
			return 0, err
		}
		return next, nil
	}
}

// GetAndAdd 返回旧值并把计数值加上 delta。
func (c *AtomicLong) GetAndAdd(ctx context.Context, delta int64) (int64, error) {
	for {
		value, version, since, err := c.readState(ctx)
		if err != nil {
			return 0, err
		}
		next := value + delta
		if err := c.write(ctx, version, next, since); err != nil {
			if isRetryableRateErr(err) {
				continue
			}
			return 0, err
		}
		return value, nil
	}
}

// CompareAndSet 仅当当前值等于 expect 时更新为 update，返回是否成功。
func (c *AtomicLong) CompareAndSet(ctx context.Context, expect, update int64) (bool, error) {
	for {
		value, version, since, err := c.readState(ctx)
		if err != nil {
			return false, err
		}
		if value != expect {
			return false, nil
		}
		if err := c.write(ctx, version, update, since); err != nil {
			if isRetryableRateErr(err) {
				continue
			}
			return false, err
		}
		return true, nil
	}
}

// Reset 把计数值归零。
func (c *AtomicLong) Reset(ctx context.Context) error {
	for {
		_, version, since, err := c.readState(ctx)
		if err != nil {
			return err
		}
		if version == 0 {
			return nil
		}
		if err := c.write(ctx, version, 0, since); err != nil {
			if isRetryableRateErr(err) {
				continue
			}
			return err
		}
		return nil
	}
}

func (c *AtomicLong) readState(ctx context.Context) (int64, storage.Version, time.Time, error) {
	raw, err := c.storage.Get(ctx, c.CounterID())
	if err != nil {
		if errors.Is(err, storage_lock.ErrLockNotFound) {
			return 0, 0, time.Time{}, nil
		}
		return 0, 0, time.Time{}, err
	}
	info, err := storage.LockInformationFromJsonString(raw)
	if err != nil {
		return 0, 0, time.Time{}, err
	}
	if info == nil {
		return 0, 0, time.Time{}, nil
	}
	return int64(info.LockCount), info.Version, info.LockBeginTime, nil
}

func (c *AtomicLong) write(ctx context.Context, version storage.Version, value int64, since time.Time) error {
	now, err := c.storage.GetTime(ctx)
	if err != nil {
		return err
	}
	info := &storage.LockInformation{
		LockId:          c.CounterID(),
		OwnerId:         "atomic-counter",
		LockCount:       int(value),
		LockBeginTime:   since,
		LeaseExpireTime: now.Add(365 * 24 * time.Hour),
	}
	if version == 0 {
		info.Version = 1
		return c.storage.CreateWithVersion(ctx, c.CounterID(), 1, info)
	}
	next := version + 1
	if next == math.MaxUint64 {
		next = 1
	}
	info.Version = next
	return c.storage.UpdateWithVersion(ctx, c.CounterID(), version, next, info)
}
