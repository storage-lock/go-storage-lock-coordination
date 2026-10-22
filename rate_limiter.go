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
	// ErrInvalidRateLimit 表示限流速率必须大于零。
	ErrInvalidRateLimit = errors.New("rate must be greater than zero")
	// ErrInvalidRateNamespace 表示限流器 namespace 不能为空。
	ErrInvalidRateNamespace = errors.New("rate limiter namespace must not be empty")
	// ErrTokenBurstCapacity 表示令牌桶容量不能小于每秒补充速率。
	ErrTokenBurstCapacity = errors.New("burst capacity must be >= tokens per second")
)

// RateLimiterOptions 配置分布式令牌桶限流器。
type RateLimiterOptions struct {
	// TokensPerSecond 每秒补充的令牌数。必须为正整数。
	TokensPerSecond int64
	// BurstCapacity 令牌桶最大容量。为 0 时默认等于 TokensPerSecond。
	BurstCapacity int64
	// OwnerID 是业务 owner 的可选标识（仅用于事件链路）。
	OwnerID string
}

// RateLimiter 是分布式令牌桶限流器。
//
// 桶状态由一条 StorageLock 记录承载，直接使用底层 Storage 的原子 CAS
// （CreateWithVersion / UpdateWithVersion）演进，不经过 StorageLock 的
// 锁语义——限流不是互斥。令牌数与上次补充时间编码在 LockCount /
// LockBeginTime 字段里，版本号承载乐观锁，天然线性一致。
type RateLimiter struct {
	storage   storage.Storage
	namespace string
	options   RateLimiterOptions
}

// NewRateLimiter 创建分布式令牌桶限流器。namespace 是全集群共享的桶名。
func NewRateLimiter(s storage.Storage, namespace string, options RateLimiterOptions) (*RateLimiter, error) {
	if s == nil {
		return nil, storage_lock.ErrStorageNil
	}
	if strings.TrimSpace(namespace) == "" {
		return nil, ErrInvalidRateNamespace
	}
	if options.TokensPerSecond < 0 {
		return nil, ErrInvalidRateLimit
	}
	burst := options.BurstCapacity
	if burst == 0 {
		burst = options.TokensPerSecond
	}
	if options.TokensPerSecond > 0 && burst < options.TokensPerSecond {
		return nil, ErrTokenBurstCapacity
	}
	options.BurstCapacity = burst
	return &RateLimiter{storage: s, namespace: namespace, options: options}, nil
}

// Namespace 返回限流器 namespace。
func (r *RateLimiter) Namespace() string { return r.namespace }

// Allow 等价于 TryAcquire(ctx, 1)，只报告是否放行。
func (r *RateLimiter) Allow(ctx context.Context) bool {
	ok, _ := r.TryAcquire(ctx, 1)
	return ok
}

// TryAcquire 尝试一次性消耗 n 个令牌（n 必须为正）。令牌不足时返回 (false, ErrLockBusy)，
// 调用方可以据此决定是否等待后重试。CAS 冲突时内部自动重试。
func (r *RateLimiter) TryAcquire(ctx context.Context, n int64) (bool, error) {
	if n <= 0 {
		return false, ErrInvalidRateLimit
	}
	for {
		state, err := r.readState(ctx)
		if err != nil {
			return false, err
		}
		now, err := r.storage.GetTime(ctx)
		if err != nil {
			return false, err
		}
		state.tokens = refillTokens(state.tokens, state.since, now, r.options.TokensPerSecond, r.options.BurstCapacity)
		if state.tokens < n {
			// 顺带把补充后的状态写回去（尽力而为），避免长时间无人消费时积累满桶不生效。
			_ = r.commit(ctx, state, state.tokens, now)
			return false, storage_lock.ErrLockBusy
		}
		next := state.tokens - n
		if err := r.commit(ctx, state, next, now); err != nil {
			if isRetryableRateErr(err) {
				continue
			}
			return false, err
		}
		return true, nil
	}
}

// Refill 手动向桶里补充 n 个令牌（不超容量上限）。常与 TokensPerSecond=0 的纯手动模式配合。
func (r *RateLimiter) Refill(ctx context.Context, n int64) error {
	if n <= 0 {
		return ErrInvalidRateLimit
	}
	for {
		state, err := r.readState(ctx)
		if err != nil {
			return err
		}
		now, err := r.storage.GetTime(ctx)
		if err != nil {
			return err
		}
		next := state.tokens + n
		if next > r.options.BurstCapacity {
			next = r.options.BurstCapacity
		}
		if err := r.commit(ctx, state, next, now); err != nil {
			if isRetryableRateErr(err) {
				continue
			}
			return err
		}
		return nil
	}
}

// Tokens 返回当前桶内可用令牌数（含按时间已补充的部分），只读，不消耗。
func (r *RateLimiter) Tokens(ctx context.Context) (int64, error) {
	state, err := r.readState(ctx)
	if err != nil {
		return 0, err
	}
	now, err := r.storage.GetTime(ctx)
	if err != nil {
		return 0, err
	}
	return refillTokens(state.tokens, state.since, now, r.options.TokensPerSecond, r.options.BurstCapacity), nil
}

// BucketID 返回桶对应的存储记录 ID。
func (r *RateLimiter) BucketID() string { return "ratelimit-" + r.namespace }

// bucketState 是解析后的桶状态。
type bucketState struct {
	tokens  int64
	since   time.Time
	version storage.Version
}

func (r *RateLimiter) readState(ctx context.Context) (bucketState, error) {
	raw, err := r.storage.Get(ctx, r.BucketID())
	if err != nil {
		if errors.Is(err, storage_lock.ErrLockNotFound) {
			return bucketState{}, nil
		}
		return bucketState{}, err
	}
	info, err := storage.LockInformationFromJsonString(raw)
	if err != nil {
		return bucketState{}, err
	}
	if info == nil {
		return bucketState{}, nil
	}
	state := bucketState{tokens: int64(info.LockCount), version: info.Version, since: info.LockBeginTime}
	if state.since.IsZero() {
		state.since = time.Now()
	}
	return state, nil
}

func (r *RateLimiter) commit(ctx context.Context, state bucketState, nextTokens int64, now time.Time) error {
	info := &storage.LockInformation{
		LockId:          r.BucketID(),
		OwnerId:         rateOwnerID(r.options.OwnerID),
		LockCount:       int(nextTokens),
		LockBeginTime:   now,
		LeaseExpireTime: now.Add(365 * 24 * time.Hour),
	}
	if state.version == 0 {
		info.Version = 1
		return r.storage.CreateWithVersion(ctx, r.BucketID(), 1, info)
	}
	nextVersion := state.version + 1
	if nextVersion == math.MaxUint64 {
		nextVersion = 1
	}
	info.Version = nextVersion
	return r.storage.UpdateWithVersion(ctx, r.BucketID(), state.version, nextVersion, info)
}

func rateOwnerID(owner string) string {
	if owner == "" {
		return "rate-limiter"
	}
	return "rate-limiter:" + owner
}

// refillTokens 把从 since 到 now 之间按 perSecond 产生的令牌累加进桶，且不超 cap。
// 整数除法保证"每秒最多 perSecond 个令牌"的契约精确成立。
func refillTokens(current int64, since, now time.Time, perSecond, cap int64) int64 {
	if perSecond <= 0 {
		return current
	}
	elapsed := now.Sub(since)
	if elapsed <= 0 {
		return current
	}
	add := (int64(elapsed) / int64(time.Second)) * perSecond
	if add > 0 {
		current += add
	}
	if current > cap {
		current = cap
	}
	return current
}

func isRetryableRateErr(err error) bool {
	return errors.Is(err, storage_lock.ErrVersionMiss) || errors.Is(err, storage_lock.ErrLockAlreadyExists)
}
