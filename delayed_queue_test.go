package coordination

import (
	"context"
	"errors"
	"testing"
	"time"

	memory_storage "github.com/storage-lock/go-memory-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
)

// delayedOptions 返回适合延迟队列的短租约选项。
// DisableWatchDog=false 时处理方由看门狗续租；true 时模拟"处理方崩溃后租约到期重可见"。
func delayedOptions(disableWatchDog bool) storage_lock.StorageLockOptions {
	options := onceTestOptions()
	// StorageLock 要求租约 >=3s；取最小值，让"崩溃后重可见"测试等待时间最短。
	options.LeaseExpireAfter = 3 * time.Second
	options.DisableWatchDog = disableWatchDog
	return options
}

// delayedStore 返回支持 List 遍历的真实内存存储（延迟队列依赖 List 扫描元素）。
func delayedStore() *memory_storage.MemoryStorage {
	return memory_storage.NewMemoryStorage()
}

func TestDelayedQueuePushHidesUntilDelay(t *testing.T) {
	ctx := context.Background()
	queue, err := NewDelayedQueue(delayedStore(), "tests/dq-hide", delayedOptions(true))
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Push(ctx, "job-1", "payload-a", 150*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	// 延迟未到：不可见。
	if _, err := queue.Claim(ctx, "consumer-1"); !errors.Is(err, ErrQueueEmpty) {
		t.Fatalf("claim before ready = %v, want ErrQueueEmpty", err)
	}
	// 延迟到点：可被抢占。
	time.Sleep(220 * time.Millisecond)
	item, err := queue.Claim(ctx, "consumer-1")
	if err != nil {
		t.Fatalf("claim after ready = %v", err)
	}
	if item.ID != "job-1" || item.Payload != "payload-a" {
		t.Fatalf("item = %#v", item)
	}
	if !item.ReadyAt.IsZero() && item.ReadyAt.After(time.Now()) {
		t.Fatalf("ready-at in future: %v", item.ReadyAt)
	}
	if err := item.Ack(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestDelayedQueueDifferentDelays(t *testing.T) {
	ctx := context.Background()
	queue, err := NewDelayedQueue(delayedStore(), "tests/dq-delays", delayedOptions(true))
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Push(ctx, "fast", "f", 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := queue.Push(ctx, "slow", "s", 500*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(180 * time.Millisecond)
	item, err := queue.Claim(ctx, "consumer")
	if err != nil {
		t.Fatalf("fast item should be ready: %v", err)
	}
	if item.ID != "fast" {
		t.Fatalf("claim id = %q, want fast", item.ID)
	}
	_ = item.Ack(ctx)
	// slow 还未到点。
	if _, err := queue.Claim(ctx, "consumer"); !errors.Is(err, ErrQueueEmpty) {
		t.Fatalf("claim slow early = %v, want ErrQueueEmpty", err)
	}
	time.Sleep(400 * time.Millisecond)
	item, err = queue.Claim(ctx, "consumer")
	if err != nil {
		t.Fatalf("slow item should be ready now: %v", err)
	}
	if item.ID != "slow" {
		t.Fatalf("claim id = %q, want slow", item.ID)
	}
	_ = item.Ack(ctx)
}

func TestDelayedQueueAckTombstones(t *testing.T) {
	ctx := context.Background()
	queue, err := NewDelayedQueue(delayedStore(), "tests/dq-ack", delayedOptions(true))
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Push(ctx, "j", "p", 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(120 * time.Millisecond)
	item, err := queue.Claim(ctx, "consumer")
	if err != nil {
		t.Fatal(err)
	}
	if err := item.Ack(ctx); err != nil {
		t.Fatal(err)
	}
	// Ack 后元素成为墓碑，不再可见。
	if _, err := queue.Claim(ctx, "consumer"); !errors.Is(err, ErrQueueEmpty) {
		t.Fatalf("claim after ack = %v, want ErrQueueEmpty", err)
	}
}

func TestDelayedQueueHandlerCrashRevisits(t *testing.T) {
	ctx := context.Background()
	queue, err := NewDelayedQueue(delayedStore(), "tests/dq-crash", delayedOptions(true))
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Push(ctx, "job", "p", 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(120 * time.Millisecond)
	item, err := queue.Claim(ctx, "consumer-1")
	if err != nil {
		t.Fatal(err)
	}
	// 模拟处理方崩溃：不 Ack、不释放，看门狗也被禁用，租约到期后元素重新可见。
	_ = item
	time.Sleep(3200 * time.Millisecond)
	again, err := queue.Claim(ctx, "consumer-2")
	if err != nil {
		t.Fatalf("crashed handler lease should expire and revisit: %v", err)
	}
	if again.ID != "job" {
		t.Fatalf("revisit id = %q, want job", again.ID)
	}
	_ = again.Ack(ctx)
}

func TestDelayedQueueDuplicatePushRejected(t *testing.T) {
	ctx := context.Background()
	queue, err := NewDelayedQueue(delayedStore(), "tests/dq-dup", delayedOptions(true))
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Push(ctx, "same", "p", time.Second); err != nil {
		t.Fatal(err)
	}
	if err := queue.Push(ctx, "same", "p2", time.Second); !errors.Is(err, storage_lock.ErrLockBusy) {
		t.Fatalf("duplicate push err = %v, want ErrLockBusy", err)
	}
}

func TestDelayedQueuePeekOnlyReady(t *testing.T) {
	ctx := context.Background()
	queue, err := NewDelayedQueue(delayedStore(), "tests/dq-peek", delayedOptions(true))
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Push(ctx, "ready", "r", 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := queue.Push(ctx, "pending", "p", time.Second); err != nil {
		t.Fatal(err)
	}
	time.Sleep(120 * time.Millisecond)
	pending, err := queue.Peek(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].ID != "ready" {
		t.Fatalf("peek = %#v, want only ready item", pending)
	}
}

func TestDelayedQueueValidation(t *testing.T) {
	if _, err := NewDelayedQueue(delayedStore(), "", delayedOptions(true)); !errors.Is(err, storage_lock.ErrLockIdEmpty) {
		t.Fatalf("empty name err = %v", err)
	}
	queue, err := NewDelayedQueue(delayedStore(), "tests/dq-validate", delayedOptions(true))
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Push(context.Background(), "x", "p", 0); !errors.Is(err, ErrInvalidDelay) {
		t.Fatalf("zero delay err = %v", err)
	}
}
