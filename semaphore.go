// Package coordination 提供建立在单 key 线性一致租约锁之上的分布式协调原语。
//
// 本包不会把单 OwnerId/LockCount 模型错误地解释为多持有者状态：信号量使用
// 固定的独立 slot，每个 slot 都是一把独立的 StorageLock。
package coordination

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	storage "github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
)

var (
	ErrInvalidSemaphoreCapacity  = errors.New("semaphore capacity must be greater than zero")
	ErrInvalidSemaphoreNamespace = errors.New("semaphore namespace must not be empty")
	ErrPermitReleased            = errors.New("semaphore permit already released")
	ErrPermitLost                = errors.New("semaphore permit lease lost")
	ErrPermitUncertain           = errors.New("semaphore permit state is uncertain")
	ErrNoPermitAvailable         = errors.New("no semaphore permit available")
)

// SemaphoreOptions 配置固定 slot 信号量。
type SemaphoreOptions struct {
	LockOptions storage_lock.StorageLockOptions
	// OwnerID 是业务 owner 的可选标识。每次 Acquire 都会在它后面追加唯一 nonce。
	OwnerID string
	// PermitTTL 是许可的最大持有时间。为 0 时许可由 supervisor 持续续租，
	// 持有者不释放就一直占着（现有行为）。大于 0 时许可到期自动过期：
	// 到期后持有者持有的 slot 被主动释放，其他客户端可立即抢占，无需等待
	// 持有者主动释放——适合"必须在限时内用完许可"的场景。
	PermitTTL time.Duration
}

// Semaphore 是固定容量的分布式信号量。
// capacity 是部署契约：同一 namespace 的所有客户端必须使用相同容量。
// 它不承诺 FIFO 公平性，也不提供跨所有 slot 的全局 fencing token。
type Semaphore struct {
	storage   storage.Storage
	namespace string
	capacity  int
	options   SemaphoreOptions
	sequence  atomic.Uint64
}

// NewSemaphore 创建固定容量信号量。
func NewSemaphore(s storage.Storage, namespace string, capacity int, options SemaphoreOptions) (*Semaphore, error) {
	if s == nil {
		return nil, storage_lock.ErrStorageNil
	}
	if strings.TrimSpace(namespace) == "" {
		return nil, ErrInvalidSemaphoreNamespace
	}
	if capacity <= 0 {
		return nil, ErrInvalidSemaphoreCapacity
	}
	if options.LockOptions.LeaseExpireAfter == 0 {
		options.LockOptions = *storage_lock.NewStorageLockOptions()
	}
	return &Semaphore{storage: s, namespace: namespace, capacity: capacity, options: options}, nil
}

// Capacity 返回固定容量。
func (s *Semaphore) Capacity() int { return s.capacity }

// Namespace 返回信号量 namespace。
func (s *Semaphore) Namespace() string { return s.namespace }

// Acquire 阻塞等待一个 permit，直到 ctx 取消。
func (s *Semaphore) Acquire(ctx context.Context) (*Permit, error) {
	for {
		permit, err := s.tryAcquire(ctx)
		if err == nil {
			return permit, nil
		}
		if !errors.Is(err, storage_lock.ErrLockBusy) && !errors.Is(err, storage_lock.ErrVersionMiss) && !errors.Is(err, ErrNoPermitAvailable) {
			return nil, err
		}
		if err := waitContext(ctx, s.retryInterval()); err != nil {
			return nil, err
		}
	}
}

// TryAcquire 只扫描一次 slot，不等待其他 permit 释放。
func (s *Semaphore) TryAcquire(ctx context.Context) (*Permit, error) {
	return s.tryAcquire(ctx)
}

func (s *Semaphore) tryAcquire(ctx context.Context) (*Permit, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ownerBase := s.options.OwnerID
	if ownerBase == "" {
		ownerBase = "semaphore-owner"
	}
	seq := s.sequence.Add(1)
	ownerID := fmt.Sprintf("%s/%d/%s", ownerBase, seq, newSessionOwner("permit"))
	for i := 0; i < s.capacity; i++ {
		lockID := s.slotID(i)
		options := s.options.LockOptions
		options.LockId = lockID
		options.DisableWatchDog = true
		lock, err := storage_lock.NewStorageLockWithOptions(s.storage, &options)
		if err != nil {
			return nil, err
		}
		err = lock.TryLock(ctx, ownerID)
		if err != nil {
			if errors.Is(err, storage_lock.ErrLockBusy) || errors.Is(err, storage_lock.ErrVersionMiss) || errors.Is(err, storage_lock.ErrLockAlreadyExists) {
				continue
			}
			return nil, err
		}
		token, err := lock.GetFencingToken(ctx, ownerID)
		if err != nil {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), unlockTimeout(options.UnlockTimeout))
			_ = lock.UnLock(cleanupCtx, ownerID)
			cancel()
			return nil, err
		}
		permit := &Permit{
			lock: lock, ownerID: ownerID, slotID: lockID, token: token,
			refreshEvery: supervisorInterval(options.LeaseExpireAfter),
			ttl:          s.options.PermitTTL,
			acquiredAt:   time.Now(),
			lost:         make(chan struct{}), done: make(chan struct{}),
		}
		// 普通许可：supervisor 持续续租，持有者不释放就一直占着。
		// 可过期许可（PermitTTL>0）：不续租，到期由 watcher 主动释放。
		if permit.ttl <= 0 {
			permit.startSupervisor()
		} else {
			permit.startExpiryWatcher(permit.ttl)
		}
		return permit, nil
	}
	return nil, ErrNoPermitAvailable
}

func (s *Semaphore) slotID(index int) string {
	return s.namespace + "/permit/" + strconv.Itoa(index)
}

func (s *Semaphore) retryInterval() time.Duration {
	interval := s.options.LockOptions.VersionMissRetryInterval
	if interval <= 0 {
		return 10 * time.Millisecond
	}
	return interval
}

// Permit 是绑定到具体 slot 的不可转移 permit。
type Permit struct {
	lock    *storage_lock.StorageLock
	ownerID string
	slotID  string
	token   storage.Version

	mu           sync.Mutex
	released     bool
	uncertain    bool
	lostState    bool
	refreshEvery time.Duration
	ttl          time.Duration
	acquiredAt   time.Time
	lost         chan struct{}
	done         chan struct{}
	lostOnce     sync.Once
	doneOnce     sync.Once
}

// SlotID 返回此 permit 占用的 slot。
func (p *Permit) SlotID() string { return p.slotID }

// OwnerID 返回内部唯一 owner。它包含每次 Acquire 的 nonce。
func (p *Permit) OwnerID() string { return p.ownerID }

// FencingToken 返回此 slot 本次持有的 fencing token。
func (p *Permit) FencingToken() storage.Version { return p.token }

// Lost 在 permit 确认失效或续租状态不确定时关闭。
func (p *Permit) Lost() <-chan struct{} { return p.lost }

// Renew 手动续租。Permit 使用显式 supervisor，因此调用方也可主动续租。
func (p *Permit) Renew(ctx context.Context) error {
	p.mu.Lock()
	if p.released {
		p.mu.Unlock()
		return ErrPermitReleased
	}
	if p.uncertain {
		p.mu.Unlock()
		return ErrPermitUncertain
	}
	if p.lostState {
		p.mu.Unlock()
		return ErrPermitLost
	}
	p.mu.Unlock()
	if err := p.lock.Renew(ctx, p.ownerID); err != nil {
		p.markLost(err)
		return err
	}
	return nil
}

// TTL 返回 permit 剩余租期。
func (p *Permit) TTL(ctx context.Context) (time.Duration, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.released {
		return 0, ErrPermitReleased
	}
	if p.uncertain {
		return 0, ErrPermitUncertain
	}
	return p.lock.TTL(ctx, p.ownerID)
}

// ExpiresIn 返回可过期许可的剩余有效期。
// 可过期许可（PermitTTL > 0）到期由 watcher 主动释放，之后其他客户端可抢占该 slot；
// 普通许可由 supervisor 持续续租，返回 0 表示"不会因时间到期而失效"。
func (p *Permit) ExpiresIn(ctx context.Context) (time.Duration, error) {
	p.mu.Lock()
	ttl := p.ttl
	acquiredAt := p.acquiredAt
	released := p.released
	uncertain := p.uncertain
	p.mu.Unlock()
	if released {
		return 0, ErrPermitReleased
	}
	if uncertain {
		return 0, ErrPermitUncertain
	}
	if ttl <= 0 {
		return 0, nil
	}
	remaining := ttl - time.Since(acquiredAt)
	if remaining < 0 {
		remaining = 0
	}
	return remaining, nil
}

// Release 释放当前 permit。重复调用返回 ErrPermitReleased。
func (p *Permit) Release(ctx context.Context) error {
	p.mu.Lock()
	if p.released {
		p.mu.Unlock()
		return ErrPermitReleased
	}
	p.released = true
	p.mu.Unlock()
	p.doneOnce.Do(func() { close(p.done) })
	return p.lock.UnLock(ctx, p.ownerID)
}

func (p *Permit) startSupervisor() {
	go func() {
		interval := p.renewalInterval()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-p.done:
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), interval)
				err := p.Renew(ctx)
				cancel()
				if err != nil {
					return
				}
			}
		}
	}()
}

// startExpiryWatcher 在 ttl 到期后主动释放许可占用的 slot。
// 只在可过期许可（PermitTTL > 0）上启动。
func (p *Permit) startExpiryWatcher(ttl time.Duration) {
	go func() {
		timer := time.NewTimer(ttl)
		defer timer.Stop()
		select {
		case <-p.done:
			return
		case <-timer.C:
			p.mu.Lock()
			if p.released {
				p.mu.Unlock()
				return
			}
			p.released = true
			p.mu.Unlock()
			p.doneOnce.Do(func() { close(p.done) })
			// 主动释放，让出 slot；尽力而为，失败也无妨（锁租约兜底）。
			_ = p.lock.UnLock(context.Background(), p.ownerID)
		}
	}()
}

func (p *Permit) renewalInterval() time.Duration {
	if p.refreshEvery > 0 {
		return p.refreshEvery
	}
	return 1 * time.Second
}

func supervisorInterval(lease time.Duration) time.Duration {
	interval := lease / 3
	if interval <= 0 {
		return time.Second
	}
	return interval
}

func (p *Permit) markLost(err error) {
	p.mu.Lock()
	if p.released {
		p.mu.Unlock()
		return
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		p.uncertain = true
	} else {
		p.lostState = true
	}
	p.mu.Unlock()
	p.lostOnce.Do(func() { close(p.lost) })
}

func waitContext(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func unlockTimeout(d time.Duration) time.Duration {
	if d <= 0 {
		return storage_lock.DefaultUnlockTimeout
	}
	return d
}
