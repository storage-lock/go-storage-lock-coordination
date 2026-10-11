package coordination

import (
	"context"
	"errors"
	"testing"
	"time"

	memory_storage "github.com/storage-lock/go-memory-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
	"github.com/stretchr/testify/require"
)

func onceTestOptions() storage_lock.StorageLockOptions {
	options := *storage_lock.NewStorageLockOptions()
	options.LeaseExpireAfter = 5 * time.Minute
	options.LeaseRefreshInterval = time.Second
	return options
}

func TestOnceTaskRunsOnlyOnce(t *testing.T) {
	store := memory_storage.NewMemoryStorage()
	task, err := NewOnceTask(store, "daily-report", onceTestOptions())
	require.NoError(t, err)
	ctx := context.Background()

	first, err := task.TryRun(ctx, "instance-a")
	require.NoError(t, err)
	require.NotZero(t, first.Token())

	_, err = task.TryRun(ctx, "instance-b")
	require.ErrorIs(t, err, ErrTaskBusy)

	owned, cancel := first.Context(ctx)
	defer cancel()
	require.NoError(t, owned.Err())

	require.NoError(t, first.Complete(ctx))
	_, err = task.TryRun(ctx, "instance-b")
	require.ErrorIs(t, err, ErrTaskAlreadyRan)

	// 放弃后的任务可以被别人重新执行。
	second, err := NewOnceTask(store, "retryable", onceTestOptions())
	require.NoError(t, err)
	run, err := second.TryRun(ctx, "instance-a")
	require.NoError(t, err)
	require.NoError(t, run.Abandon(ctx))
	again, err := second.TryRun(ctx, "instance-b")
	require.NoError(t, err)
	require.Greater(t, again.Token(), run.Token(), "接管后的 fencing token 必须更大")
	require.NoError(t, again.Abandon(ctx))
}

func TestQueueClaimIsExclusiveAndReappears(t *testing.T) {
	store := memory_storage.NewMemoryStorage()
	queue, err := NewQueue(store, "jobs", onceTestOptions())
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, queue.Push(ctx, "job-1", "payload-1"))
	require.Error(t, queue.Push(ctx, "job-1", "payload-1"))

	first, err := queue.Claim(ctx, "worker-a")
	require.NoError(t, err)
	require.Equal(t, "payload-1", first.Payload)
	require.NotZero(t, first.Token)

	_, err = queue.Claim(ctx, "worker-b")
	require.ErrorIs(t, err, ErrQueueEmpty)

	require.NoError(t, first.Ack(ctx))
	again, err := queue.Claim(ctx, "worker-b")
	require.NoError(t, err)
	require.Equal(t, "job-1", again.ID)
	require.Greater(t, again.Token, first.Token)
	require.NoError(t, again.Ack(ctx))
}

func TestQueueEmpty(t *testing.T) {
	store := memory_storage.NewMemoryStorage()
	queue, err := NewQueue(store, "empty", onceTestOptions())
	require.NoError(t, err)
	_, err = queue.Claim(context.Background(), "worker")
	require.ErrorIs(t, err, ErrQueueEmpty)
	require.Error(t, queue.Push(context.Background(), "", "payload"))
}

var _ = errors.Is
