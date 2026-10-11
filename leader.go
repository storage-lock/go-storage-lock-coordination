package coordination

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	storage "github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
)

var (
	ErrInvalidLeaderLockID = errors.New("leader lock id must not be empty")
	ErrLeadershipLost      = errors.New("leadership lost")
	ErrLeadershipUncertain = errors.New("leadership state is uncertain")
	ErrLeadershipResigned  = errors.New("leadership already resigned")
)

// LeaderOptions 配置租约选主。OwnerID 为空时由本包生成唯一 session owner。
type LeaderOptions struct {
	LockOptions storage_lock.StorageLockOptions
	OwnerID     string
}

// LeaderElector 在一把线性一致租约锁上提供 leader campaign。
type LeaderElector struct {
	storage  storage.Storage
	lockID   string
	options  LeaderOptions
	sequence uint64
	mu       sync.Mutex
}

// NewLeaderElector 创建选主器。
func NewLeaderElector(s storage.Storage, lockID string, options LeaderOptions) (*LeaderElector, error) {
	if s == nil {
		return nil, storage_lock.ErrStorageNil
	}
	if lockID == "" {
		return nil, ErrInvalidLeaderLockID
	}
	if options.LockOptions.LeaseExpireAfter == 0 {
		options.LockOptions = *storage_lock.NewStorageLockOptions()
	}
	return &LeaderElector{storage: s, lockID: lockID, options: options}, nil
}

// Campaign 等待直到成为 leader 或 ctx 取消。
func (e *LeaderElector) Campaign(ctx context.Context) (*Leadership, error) {
	for {
		leadership, err := e.TryCampaign(ctx)
		if err == nil {
			return leadership, nil
		}
		if !errors.Is(err, storage_lock.ErrLockBusy) && !errors.Is(err, storage_lock.ErrVersionMiss) && !errors.Is(err, storage_lock.ErrLockAlreadyExists) {
			return nil, err
		}
		if err := waitContext(ctx, e.retryInterval()); err != nil {
			return nil, err
		}
	}
}

// TryCampaign 只尝试一次，不等待当前 leader 释放。
func (e *LeaderElector) retryInterval() time.Duration {
	interval := e.options.LockOptions.VersionMissRetryInterval
	if interval <= 0 {
		return 10 * time.Millisecond
	}
	return interval
}

func (e *LeaderElector) TryCampaign(ctx context.Context) (*Leadership, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	e.mu.Lock()
	e.sequence++
	seq := e.sequence
	e.mu.Unlock()
	base := e.options.OwnerID
	if base == "" {
		base = "leader"
	}
	ownerID := fmt.Sprintf("%s/%d/%s", base, seq, newSessionOwner("leader"))
	options := e.options.LockOptions
	options.LockId = e.lockID
	options.DisableWatchDog = true
	lock, err := storage_lock.NewStorageLockWithOptions(e.storage, &options)
	if err != nil {
		return nil, err
	}
	if err := lock.TryLock(ctx, ownerID); err != nil {
		return nil, err
	}
	token, err := lock.GetFencingToken(ctx, ownerID)
	if err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), unlockTimeout(options.UnlockTimeout))
		_ = lock.UnLock(cleanupCtx, ownerID)
		cancel()
		return nil, err
	}
	leadership := &Leadership{lock: lock, ownerID: ownerID, token: token, refreshInterval: supervisorInterval(options.LeaseExpireAfter), lostCh: make(chan struct{}), done: make(chan struct{})}
	leadership.startSupervisor()
	return leadership, nil
}

// Leadership 是一次选主 session 的句柄。
type Leadership struct {
	lock            *storage_lock.StorageLock
	ownerID         string
	token           storage.Version
	refreshInterval time.Duration

	mu        sync.Mutex
	resigned  bool
	lost      bool
	uncertain bool
	lostCh    chan struct{}
	done      chan struct{}
	lostOnce  sync.Once
	doneOnce  sync.Once
}

// OwnerID 返回内部唯一 session owner。
func (l *Leadership) OwnerID() string { return l.ownerID }

// FencingToken 返回本次领导权的 fencing token。
func (l *Leadership) FencingToken() storage.Version { return l.token }

// Lost 在领导权确认丢失或状态不确定时关闭。
func (l *Leadership) Lost() <-chan struct{} { return l.lostCh }

// Status 表示当前句柄状态。
type LeadershipStatus string

const (
	LeadershipActive    LeadershipStatus = "active"
	LeadershipLost      LeadershipStatus = "lost"
	LeadershipUncertain LeadershipStatus = "uncertain"
	LeadershipResigned  LeadershipStatus = "resigned"
)

func (l *Leadership) Status() LeadershipStatus {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.resigned {
		return LeadershipResigned
	}
	if l.uncertain {
		return LeadershipUncertain
	}
	if l.lost {
		return LeadershipLost
	}
	return LeadershipActive
}

// Renew 手动续租；任何不确定错误都 fail-closed。
func (l *Leadership) Renew(ctx context.Context) error {
	l.mu.Lock()
	if l.resigned {
		l.mu.Unlock()
		return ErrLeadershipResigned
	}
	if l.uncertain {
		l.mu.Unlock()
		return ErrLeadershipUncertain
	}
	if l.lost {
		l.mu.Unlock()
		return ErrLeadershipLost
	}
	l.mu.Unlock()
	if err := l.lock.Renew(ctx, l.ownerID); err != nil {
		l.markLost(err)
		return err
	}
	return nil
}

// Resign 使用 owner/CAS 语义释放并保留墓碑版本。
func (l *Leadership) Resign(ctx context.Context) error {
	l.mu.Lock()
	if l.resigned {
		l.mu.Unlock()
		return ErrLeadershipResigned
	}
	if l.uncertain {
		l.mu.Unlock()
		return ErrLeadershipUncertain
	}
	if l.lost {
		l.mu.Unlock()
		return ErrLeadershipLost
	}
	l.resigned = true
	l.mu.Unlock()
	l.doneOnce.Do(func() { close(l.done) })
	if err := l.lock.UnLock(ctx, l.ownerID); err != nil {
		// 释放提交状态未知时不能报告成功；保持状态为 uncertain。
		l.mu.Lock()
		l.uncertain = true
		l.mu.Unlock()
		l.lostOnce.Do(func() { close(l.lostCh) })
		return err
	}
	return nil
}

func (l *Leadership) startSupervisor() {
	go func() {
		interval := l.refreshInterval
		if interval <= 0 {
			interval = time.Second
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-l.done:
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), interval)
				err := l.Renew(ctx)
				cancel()
				if err != nil {
					return
				}
			}
		}
	}()
}

func (l *Leadership) markLost(err error) {
	l.mu.Lock()
	if l.resigned {
		l.mu.Unlock()
		return
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		l.uncertain = true
	} else {
		l.lost = true
	}
	l.mu.Unlock()
	l.lostOnce.Do(func() { close(l.lostCh) })
}
