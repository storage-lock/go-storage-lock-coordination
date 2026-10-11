package coordination

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	storage "github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
)

var (
	ErrCompositeLockIDsEmpty    = errors.New("composite lock ids must not be empty")
	ErrCompositeDuplicateLockID = errors.New("composite lock ids must be unique")
)

// OrderedCompositeLock 是受限的组合锁：逐个获取独立锁，全部成功后才返回。
// 它不是跨 key 原子锁，也不是 RedLock；返回的 fencing token 按 lock ID 分别计算。
type OrderedCompositeLock struct {
	storage storage.Storage
	lockIDs []string
	options storage_lock.StorageLockOptions
}

// NewOrderedCompositeLock 创建按字典序获取的组合锁。
func NewOrderedCompositeLock(s storage.Storage, lockIDs []string, options storage_lock.StorageLockOptions) (*OrderedCompositeLock, error) {
	if s == nil {
		return nil, storage_lock.ErrStorageNil
	}
	if len(lockIDs) == 0 {
		return nil, ErrCompositeLockIDsEmpty
	}
	ids := append([]string(nil), lockIDs...)
	sort.Strings(ids)
	for i, id := range ids {
		if id == "" {
			return nil, storage_lock.ErrLockIdEmpty
		}
		if i > 0 && ids[i-1] == id {
			return nil, ErrCompositeDuplicateLockID
		}
	}
	if options.LeaseExpireAfter == 0 {
		options = *storage_lock.NewStorageLockOptions()
	}
	return &OrderedCompositeLock{storage: s, lockIDs: ids, options: options}, nil
}

// Lock 获取所有子锁；任意失败时按逆序释放已获取的子锁。
func (c *OrderedCompositeLock) Lock(ctx context.Context, ownerID string) (*CompositeLease, error) {
	if ownerID == "" {
		return nil, storage_lock.ErrOwnerIdEmpty
	}
	sessionOwner := ownerID + "/" + newSessionOwner("composite")
	acquired := make([]*storage_lock.StorageLock, 0, len(c.lockIDs))
	for _, id := range c.lockIDs {
		options := c.options
		options.LockId = id
		options.DisableWatchDog = false
		lock, err := storage_lock.NewStorageLockWithOptions(c.storage, &options)
		if err != nil {
			return nil, c.rollback(ctx, sessionOwner, acquired, err)
		}
		if err := lock.Lock(ctx, sessionOwner); err != nil {
			return nil, c.rollback(ctx, sessionOwner, acquired, err)
		}
		acquired = append(acquired, lock)
	}
	tokens := make(map[string]storage.Version, len(acquired))
	for i, lock := range acquired {
		token, err := lock.GetFencingToken(ctx, sessionOwner)
		if err != nil {
			return nil, c.rollback(ctx, sessionOwner, acquired, err)
		}
		tokens[c.lockIDs[i]] = token
	}
	return &CompositeLease{locks: acquired, lockIDs: append([]string(nil), c.lockIDs...), ownerID: sessionOwner, tokens: tokens}, nil
}

func (c *OrderedCompositeLock) rollback(ctx context.Context, ownerID string, acquired []*storage_lock.StorageLock, cause error) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), unlockTimeout(c.options.UnlockTimeout))
	defer cancel()
	for i := len(acquired) - 1; i >= 0; i-- {
		_ = acquired[i].UnLock(cleanupCtx, ownerID)
	}
	return cause
}

// CompositeLease 是所有子锁成功后的组合句柄。
type CompositeLease struct {
	locks    []*storage_lock.StorageLock
	lockIDs  []string
	ownerID  string
	tokens   map[string]storage.Version
	mu       sync.Mutex
	released bool
}

// FencingTokens 返回每个子锁独立的 token 副本。
func (l *CompositeLease) FencingTokens() map[string]storage.Version {
	l.mu.Lock()
	defer l.mu.Unlock()
	result := make(map[string]storage.Version, len(l.tokens))
	for id, token := range l.tokens {
		result[id] = token
	}
	return result
}

// Release 按逆序释放全部子锁；返回第一个错误。
func (l *CompositeLease) Release(ctx context.Context) error {
	l.mu.Lock()
	if l.released {
		l.mu.Unlock()
		return storage_lock.ErrLockNotFound
	}
	l.released = true
	l.mu.Unlock()
	var first error
	for i := len(l.locks) - 1; i >= 0; i-- {
		if err := l.locks[i].UnLock(ctx, l.ownerID); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Renew 续租所有子锁；失败后调用方必须停止临界区业务并按 token 栅栏。
func (l *CompositeLease) Renew(ctx context.Context) error {
	l.mu.Lock()
	if l.released {
		l.mu.Unlock()
		return fmt.Errorf("composite lease released")
	}
	l.mu.Unlock()
	for _, lock := range l.locks {
		if err := lock.Renew(ctx, l.ownerID); err != nil {
			return err
		}
	}
	return nil
}
