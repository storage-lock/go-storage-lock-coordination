package coordination

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	storage_lock "github.com/storage-lock/go-storage-lock"
)

func TestRateLimiterAllowAndConsume(t *testing.T) {
	ctx := context.Background()
	rl, err := NewRateLimiter(testStorage(t), "tests/ratelimit-basic", RateLimiterOptions{TokensPerSecond: 3, BurstCapacity: 3})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if !rl.Allow(ctx) {
			t.Fatalf("allow %d failed, bucket should be full", i)
		}
	}
	if rl.Allow(ctx) {
		t.Fatal("allow succeeded beyond burst capacity")
	}
	tokens, err := rl.Tokens(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if tokens != 0 {
		t.Fatalf("tokens = %d, want 0", tokens)
	}
}

func TestRateLimiterRefillOverTime(t *testing.T) {
	ctx := context.Background()
	rl, err := NewRateLimiter(testStorage(t), "tests/ratelimit-refill", RateLimiterOptions{TokensPerSecond: 2, BurstCapacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	if !rl.Allow(ctx) || !rl.Allow(ctx) {
		t.Fatal("initial bucket should allow 2")
	}
	if rl.Allow(ctx) {
		t.Fatal("bucket should be empty")
	}
	time.Sleep(1100 * time.Millisecond)
	tokens, err := rl.Tokens(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if tokens != 2 {
		t.Fatalf("tokens after 1s refill = %d, want 2", tokens)
	}
	if !rl.Allow(ctx) {
		t.Fatal("refilled token should allow acquire")
	}
}

func TestRateLimiterBurstLargerThanRate(t *testing.T) {
	ctx := context.Background()
	rl, err := NewRateLimiter(testStorage(t), "tests/ratelimit-burst", RateLimiterOptions{TokensPerSecond: 1, BurstCapacity: 5})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if !rl.Allow(ctx) {
			t.Fatalf("burst allow %d failed", i)
		}
	}
	if rl.Allow(ctx) {
		t.Fatal("burst consumed, should fail")
	}
}

func TestRateLimiterManualRefill(t *testing.T) {
	ctx := context.Background()
	rl, err := NewRateLimiter(testStorage(t), "tests/ratelimit-manual", RateLimiterOptions{TokensPerSecond: 0, BurstCapacity: 10})
	if err != nil {
		t.Fatal(err)
	}
	if rl.Allow(ctx) {
		t.Fatal("manual mode with zero tokens should not allow")
	}
	if err := rl.Refill(ctx, 3); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if !rl.Allow(ctx) {
			t.Fatalf("manual allow %d failed", i)
		}
	}
	if rl.Allow(ctx) {
		t.Fatal("manual bucket should be empty after 3")
	}
}

func TestRateLimiterConcurrentAcquire(t *testing.T) {
	ctx := context.Background()
	rl, err := NewRateLimiter(testStorage(t), "tests/ratelimit-concurrent", RateLimiterOptions{TokensPerSecond: 10, BurstCapacity: 10})
	if err != nil {
		t.Fatal(err)
	}
	const total = 40
	var wg sync.WaitGroup
	success := make(chan bool, total)
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := rl.TryAcquire(ctx, 1)
			// ErrLockBusy 是"令牌不足"的正常信号，不是错误。
			if err != nil && !errors.Is(err, storage_lock.ErrLockBusy) {
				t.Error(err)
				return
			}
			success <- ok
		}()
	}
	wg.Wait()
	close(success)
	okCount := 0
	for ok := range success {
		if ok {
			okCount++
		}
	}
	if okCount != 10 {
		t.Fatalf("concurrent allows = %d, want exactly 10 (initial burst, no time to refill)", okCount)
	}
}

func TestRateLimiterParameterValidation(t *testing.T) {
	if _, err := NewRateLimiter(testStorage(t), "", RateLimiterOptions{TokensPerSecond: 1}); !errors.Is(err, ErrInvalidRateNamespace) {
		t.Fatalf("empty namespace err = %v", err)
	}
	if _, err := NewRateLimiter(testStorage(t), "rl", RateLimiterOptions{TokensPerSecond: 1, BurstCapacity: 0}); err != nil {
		t.Fatalf("burst default should pass: %v", err)
	}
	if _, err := NewRateLimiter(testStorage(t), "rl", RateLimiterOptions{TokensPerSecond: 5, BurstCapacity: 2}); !errors.Is(err, ErrTokenBurstCapacity) {
		t.Fatalf("burst < rate err = %v", err)
	}
	if _, err := NewRateLimiter(testStorage(t), "rl", RateLimiterOptions{TokensPerSecond: -1}); !errors.Is(err, ErrInvalidRateLimit) {
		t.Fatalf("negative rate err = %v", err)
	}
}

func TestRateLimiterZeroBurstDefaultsToRate(t *testing.T) {
	ctx := context.Background()
	rl, err := NewRateLimiter(testStorage(t), "tests/ratelimit-default-burst", RateLimiterOptions{TokensPerSecond: 4})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if !rl.Allow(ctx) {
			t.Fatalf("allow %d failed", i)
		}
	}
	if rl.Allow(ctx) {
		t.Fatal("should be capped at rate when burst defaults to rate")
	}
}

func TestRateLimiterRejectsNonPositiveAcquire(t *testing.T) {
	ctx := context.Background()
	rl, err := NewRateLimiter(testStorage(t), "tests/ratelimit-n", RateLimiterOptions{TokensPerSecond: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rl.TryAcquire(ctx, 0); !errors.Is(err, ErrInvalidRateLimit) {
		t.Fatalf("acquire 0 err = %v", err)
	}
	// 并发 CAS 冲突应自动重试不报错
	if _, err := rl.TryAcquire(ctx, 1); err != nil && !errors.Is(err, storage_lock.ErrLockBusy) {
		t.Fatalf("acquire 1 err = %v", err)
	}
}
