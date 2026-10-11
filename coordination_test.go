package coordination

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/golang-infrastructure/go-iterator"
	storage "github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
)

type testMemoryStorage struct {
	mu     sync.RWMutex
	values map[string]string
}

func testStorage(t *testing.T) *testMemoryStorage {
	t.Helper()
	return &testMemoryStorage{values: make(map[string]string)}
}

func (s *testMemoryStorage) GetName() string { return "coordination-test-memory" }
func (s *testMemoryStorage) Capabilities() []storage.StorageCapability {
	return []storage.StorageCapability{storage.CapabilityCAS, storage.CapabilityReliableTime, storage.CapabilityAtomicDelete}
}
func (s *testMemoryStorage) Init(context.Context) error { return nil }
func (s *testMemoryStorage) Get(_ context.Context, id string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.values[id]
	if !ok {
		return "", storage_lock.ErrLockNotFound
	}
	return value, nil
}
func (s *testMemoryStorage) GetTime(context.Context) (time.Time, error) { return time.Now(), nil }
func (s *testMemoryStorage) Close(context.Context) error                { return nil }
func (s *testMemoryStorage) List(context.Context) (iterator.Iterator[*storage.LockInformation], error) {
	return iterator.FromSlice([]*storage.LockInformation{}), nil
}
func (s *testMemoryStorage) CreateWithVersion(_ context.Context, id string, _ storage.Version, info *storage.LockInformation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.values[id]; ok {
		return storage_lock.ErrLockAlreadyExists
	}
	s.values[id] = info.ToJsonString()
	return nil
}
func (s *testMemoryStorage) UpdateWithVersion(_ context.Context, id string, expected, _ storage.Version, info *storage.LockInformation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.values[id]
	if !ok {
		return storage_lock.ErrVersionMiss
	}
	oldInfo, err := storage.LockInformationFromJsonString(old)
	if err != nil || oldInfo.Version != expected {
		return storage_lock.ErrVersionMiss
	}
	s.values[id] = info.ToJsonString()
	return nil
}
func (s *testMemoryStorage) DeleteWithVersion(_ context.Context, id string, expected storage.Version, _ *storage.LockInformation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.values[id]
	if !ok {
		return storage_lock.ErrLockNotFound
	}
	info, err := storage.LockInformationFromJsonString(old)
	if err != nil || info.Version != expected {
		return storage_lock.ErrVersionMiss
	}
	delete(s.values, id)
	return nil
}

func testOptions() storage_lock.StorageLockOptions {
	return *storage_lock.NewStorageLockOptions().
		SetLeaseExpireAfter(3 * time.Second).
		SetLeaseRefreshInterval(time.Second).
		SetVersionMissRetryInterval(10 * time.Millisecond)
}

func TestSemaphoreCapacityAndDistinctSlots(t *testing.T) {
	ctx := context.Background()
	semaphore, err := NewSemaphore(testStorage(t), "tests/semaphore", 2, SemaphoreOptions{LockOptions: testOptions(), OwnerID: "worker"})
	if err != nil {
		t.Fatal(err)
	}
	first, err := semaphore.TryAcquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	second, err := semaphore.TryAcquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if first.SlotID() == second.SlotID() {
		t.Fatal("permits share a slot")
	}
	if _, err := semaphore.TryAcquire(ctx); !errors.Is(err, ErrNoPermitAvailable) {
		t.Fatalf("third acquire = %v", err)
	}
	if err := first.Release(ctx); err != nil {
		t.Fatal(err)
	}
	third, err := semaphore.TryAcquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if third.SlotID() != first.SlotID() {
		t.Fatalf("reclaimed slot = %q, want %q", third.SlotID(), first.SlotID())
	}
	if err := second.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := third.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestSemaphoreAcquireCancellation(t *testing.T) {
	ctx := context.Background()
	semaphore, err := NewSemaphore(testStorage(t), "tests/semaphore-cancel", 1, SemaphoreOptions{LockOptions: testOptions()})
	if err != nil {
		t.Fatal(err)
	}
	permit, err := semaphore.TryAcquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	deferred, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	if _, err := semaphore.Acquire(deferred); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Acquire error = %v", err)
	}
	if err := permit.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestLeaderSingleActiveAndResign(t *testing.T) {
	ctx := context.Background()
	storage := testStorage(t)
	options := LeaderOptions{LockOptions: testOptions(), OwnerID: "node"}
	first, err := NewLeaderElector(storage, "tests/leader", options)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewLeaderElector(storage, "tests/leader", options)
	if err != nil {
		t.Fatal(err)
	}
	leadership, err := first.TryCampaign(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.TryCampaign(ctx); !errors.Is(err, storage_lock.ErrLockBusy) {
		t.Fatalf("second campaign = %v", err)
	}
	oldToken := leadership.FencingToken()
	if err := leadership.Resign(ctx); err != nil {
		t.Fatal(err)
	}
	next, err := second.TryCampaign(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if next.FencingToken() <= oldToken {
		t.Fatalf("tokens %d then %d", oldToken, next.FencingToken())
	}
	if next.Status() != LeadershipActive {
		t.Fatalf("status = %s", next.Status())
	}
	if err := next.Resign(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestOrderedCompositeLockSortsAndReleases(t *testing.T) {
	ctx := context.Background()
	storage := testStorage(t)
	composite, err := NewOrderedCompositeLock(storage, []string{"tests/z", "tests/a"}, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	lease, err := composite.Lock(ctx, "caller")
	if err != nil {
		t.Fatal(err)
	}
	tokens := lease.FencingTokens()
	if len(tokens) != 2 || tokens["tests/a"] == 0 || tokens["tests/z"] == 0 {
		t.Fatalf("tokens = %#v", tokens)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(ctx); !errors.Is(err, storage_lock.ErrLockNotFound) {
		t.Fatalf("second release = %v", err)
	}
}
