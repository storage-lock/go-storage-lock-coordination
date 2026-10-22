package coordination

import (
	"context"
	"errors"
	"sync"
	"testing"

	storage_lock "github.com/storage-lock/go-storage-lock"
)

func TestAtomicLongIncrementAndGet(t *testing.T) {
	ctx := context.Background()
	counter, err := NewAtomicLong(testStorage(t), "tests/atomic-incr")
	if err != nil {
		t.Fatal(err)
	}
	if v, err := counter.Get(ctx); err != nil || v != 0 {
		t.Fatalf("initial get = %d, %v", v, err)
	}
	for i := 1; i <= 5; i++ {
		v, err := counter.IncrementAndGet(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if v != int64(i) {
			t.Fatalf("increment %d = %d, want %d", i, v, i)
		}
	}
	if v, _ := counter.Get(ctx); v != 5 {
		t.Fatalf("final get = %d, want 5", v)
	}
}

func TestAtomicLongGetAndIncrement(t *testing.T) {
	ctx := context.Background()
	counter, err := NewAtomicLong(testStorage(t), "tests/atomic-get-and-incr")
	if err != nil {
		t.Fatal(err)
	}
	old, err := counter.GetAndIncrement(ctx)
	if err != nil || old != 0 {
		t.Fatalf("get-and-incr = %d, %v", old, err)
	}
	if v, _ := counter.Get(ctx); v != 1 {
		t.Fatalf("after get-and-incr = %d, want 1", v)
	}
}

func TestAtomicLongDecrementAndAdd(t *testing.T) {
	ctx := context.Background()
	counter, err := NewAtomicLong(testStorage(t), "tests/atomic-add")
	if err != nil {
		t.Fatal(err)
	}
	if v, err := counter.AddAndGet(ctx, 10); err != nil || v != 10 {
		t.Fatalf("add 10 = %d, %v", v, err)
	}
	if v, err := counter.DecrementAndGet(ctx); err != nil || v != 9 {
		t.Fatalf("decrement = %d, %v", v, err)
	}
	old, err := counter.GetAndDecrement(ctx)
	if err != nil || old != 9 {
		t.Fatalf("get-and-decrement = %d, %v", old, err)
	}
	if v, _ := counter.Get(ctx); v != 8 {
		t.Fatalf("final = %d, want 8", v)
	}
}

func TestAtomicLongCompareAndSet(t *testing.T) {
	ctx := context.Background()
	counter, err := NewAtomicLong(testStorage(t), "tests/atomic-cas")
	if err != nil {
		t.Fatal(err)
	}
	if v, err := counter.AddAndGet(ctx, 5); err != nil || v != 5 {
		t.Fatalf("setup = %d, %v", v, err)
	}
	ok, err := counter.CompareAndSet(ctx, 5, 42)
	if err != nil || !ok {
		t.Fatalf("cas matching = %v, %v", ok, err)
	}
	if v, _ := counter.Get(ctx); v != 42 {
		t.Fatalf("after cas = %d, want 42", v)
	}
	ok, err = counter.CompareAndSet(ctx, 5, 100)
	if err != nil || ok {
		t.Fatalf("cas mismatching = %v, %v, want false", ok, err)
	}
	if v, _ := counter.Get(ctx); v != 42 {
		t.Fatalf("mismatched cas changed value to %d", v)
	}
}

func TestAtomicLongReset(t *testing.T) {
	ctx := context.Background()
	counter, err := NewAtomicLong(testStorage(t), "tests/atomic-reset")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = counter.AddAndGet(ctx, 100)
	if err := counter.Reset(ctx); err != nil {
		t.Fatal(err)
	}
	if v, _ := counter.Get(ctx); v != 0 {
		t.Fatalf("after reset = %d, want 0", v)
	}
}

func TestAtomicLongConcurrentIncrement(t *testing.T) {
	ctx := context.Background()
	counter, err := NewAtomicLong(testStorage(t), "tests/atomic-concurrent")
	if err != nil {
		t.Fatal(err)
	}
	const workers = 20
	const perWorker = 100
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				if _, err := counter.IncrementAndGet(ctx); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if v, _ := counter.Get(ctx); v != workers*perWorker {
		t.Fatalf("concurrent total = %d, want %d", v, workers*perWorker)
	}
}

func TestAtomicLongValidation(t *testing.T) {
	if _, err := NewAtomicLong(testStorage(t), ""); !errors.Is(err, ErrAtomicNamespaceEmpty) {
		t.Fatalf("empty namespace err = %v", err)
	}
	if _, err := NewAtomicLong(nil, "x"); !errors.Is(err, storage_lock.ErrStorageNil) {
		t.Fatalf("nil storage err = %v", err)
	}
}
