package coordination

import (
	"context"
	"testing"
	"time"
)

// ttlOptions 返回带 3s 默认租约的选项；PermitTTL 覆盖租约长度并禁用看门狗。
func ttlOptions(ttl time.Duration) SemaphoreOptions {
	return SemaphoreOptions{LockOptions: testOptions(), OwnerID: "worker", PermitTTL: ttl}
}

func TestExpirableSemaphorePermitExpiresAutomatically(t *testing.T) {
	ctx := context.Background()
	sem, err := NewSemaphore(testStorage(t), "tests/expirable", 1, ttlOptions(150*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	first, err := sem.TryAcquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if first.SlotID() == "" {
		t.Fatal("empty slot")
	}
	// 持有期间 slot 被占用。
	if _, err := sem.TryAcquire(ctx); err == nil {
		t.Fatal("second acquire should fail while permit held")
	}
	// 等待 permit TTL 过期，无需持有者释放即可被抢占。
	time.Sleep(250 * time.Millisecond)
	second, err := sem.TryAcquire(ctx)
	if err != nil {
		t.Fatalf("expired permit should be reclaimable: %v", err)
	}
	if second.SlotID() != first.SlotID() {
		t.Fatalf("reclaimed slot = %q, want %q", second.SlotID(), first.SlotID())
	}
	if err := second.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestExpirableSemaphoreTokenIncreasesAfterExpiry(t *testing.T) {
	ctx := context.Background()
	sem, err := NewSemaphore(testStorage(t), "tests/expirable-token", 1, ttlOptions(120*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	first, err := sem.TryAcquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	oldToken := first.FencingToken()
	time.Sleep(220 * time.Millisecond)
	second, err := sem.TryAcquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if second.FencingToken() <= oldToken {
		t.Fatalf("fencing token %d after %d, must increase", second.FencingToken(), oldToken)
	}
	if err := second.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestExpirableSemaphoreExpiresIn(t *testing.T) {
	ctx := context.Background()
	const ttl = 200 * time.Millisecond
	sem, err := NewSemaphore(testStorage(t), "tests/expirable-ttl", 1, ttlOptions(ttl))
	if err != nil {
		t.Fatal(err)
	}
	permit, err := sem.TryAcquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	remaining, err := permit.ExpiresIn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if remaining <= 0 || remaining > ttl {
		t.Fatalf("expires-in = %v, want in (0, %v]", remaining, ttl)
	}
	if err := permit.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestExpirableSemaphoreZeroTTLKeepsHeld(t *testing.T) {
	ctx := context.Background()
	// PermitTTL=0 保持原有语义：supervisor 续租，不释放就一直占着。
	sem, err := NewSemaphore(testStorage(t), "tests/expirable-zero", 1, SemaphoreOptions{LockOptions: testOptions(), OwnerID: "worker"})
	if err != nil {
		t.Fatal(err)
	}
	permit, err := sem.TryAcquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if remaining, err := permit.ExpiresIn(ctx); err != nil || remaining != 0 {
		t.Fatalf("zero-ttl expires-in = %v, %v, want 0, nil", remaining, err)
	}
	if _, err := sem.TryAcquire(ctx); err == nil {
		t.Fatal("zero-ttl permit should stay held until released")
	}
	if err := permit.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if reacquired, err := sem.TryAcquire(ctx); err != nil {
		t.Fatalf("after release should reacquire: %v", err)
	} else if err := reacquired.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestExpirableSemaphoreReleaseBeforeExpiry(t *testing.T) {
	ctx := context.Background()
	sem, err := NewSemaphore(testStorage(t), "tests/expirable-release", 1, ttlOptions(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	permit, err := sem.TryAcquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := permit.Release(ctx); err != nil {
		t.Fatal(err)
	}
	// 显式释放后同一 slot 立即可用。
	next, err := sem.TryAcquire(ctx)
	if err != nil {
		t.Fatalf("release should free slot immediately: %v", err)
	}
	if next.SlotID() != permit.SlotID() {
		t.Fatalf("slot = %q, want %q", next.SlotID(), permit.SlotID())
	}
	_ = next.Release(ctx)
}

func TestExpirableSemaphoreDoubleReleaseError(t *testing.T) {
	ctx := context.Background()
	sem, err := NewSemaphore(testStorage(t), "tests/expirable-double-release", 1, ttlOptions(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	permit, err := sem.TryAcquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := permit.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := permit.Release(ctx); err != ErrPermitReleased {
		t.Fatalf("double release err = %v, want %v", err, ErrPermitReleased)
	}
}
