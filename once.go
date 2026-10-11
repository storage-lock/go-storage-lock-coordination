package coordination

import (
	"context"
	"errors"

	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
)

var (
	// ErrTaskAlreadyRan 表示这个任务已经被某个持有者执行过，不能再跑一次。
	ErrTaskAlreadyRan = errors.New("task already ran")
	// ErrTaskBusy 表示另一个持有者正在执行这个任务且租约仍有效。
	ErrTaskBusy = errors.New("task is running")
)

// TaskRun 是一次成功抢到的单次执行权。
type TaskRun struct {
	TaskID  string
	Owner   string
	Fence   storage.Version
	storage storage.Storage
	lease   *storage_lock.StorageLock
}

// Token 返回本次执行的 fencing token。业务写入必须校验它。
func (r *TaskRun) Token() storage.Version { return r.Fence }

// Context 返回在持有权丢失时取消的上下文。取消不能追回已发出的写入。
func (r *TaskRun) Context(ctx context.Context) (context.Context, context.CancelFunc) {
	return r.lease.OwnedContext(ctx)
}

// Renew 手动延长执行租约。看门狗开启时不需要调用。
func (r *TaskRun) Renew(ctx context.Context) error { return r.lease.Renew(ctx, r.Owner) }

// Complete 标记任务已完成。完成后任何实例再抢都会得到 ErrTaskAlreadyRan。
func (r *TaskRun) Complete(ctx context.Context) error {
	if err := r.lease.UnLock(ctx, r.Owner); err != nil {
		return err
	}
	marker, err := storage_lock.NewStorageLock(r.leaseStorage(), doneLockID(r.TaskID))
	if err != nil {
		return err
	}
	return marker.TryLock(ctx, doneOwner(r.TaskID))
}

func (r *TaskRun) leaseStorage() storage.Storage { return r.storage }

// Abandon 放弃本次执行，让其他实例可以重新抢。任务没有被标记完成。
func (r *TaskRun) Abandon(ctx context.Context) error { return r.lease.UnLock(ctx, r.Owner) }

// OnceTask 保证同一个 taskID 在全集群只成功执行一次。
// 执行中的租约过期后允许另一个实例接管；完成后永久拒绝。
type OnceTask struct {
	storage storage.Storage
	taskID  string
	options storage_lock.StorageLockOptions
}

// NewOnceTask 创建单次任务。taskID 是全集群共享的任务标识。
func NewOnceTask(s storage.Storage, taskID string, options storage_lock.StorageLockOptions) (*OnceTask, error) {
	if s == nil {
		return nil, storage_lock.ErrStorageNil
	}
	if taskID == "" {
		return nil, storage_lock.ErrLockIdEmpty
	}
	return &OnceTask{storage: s, taskID: taskID, options: options}, nil
}

// TryRun 尝试获取一次执行权，不等待。
// 已完成返回 ErrTaskAlreadyRan，他人正在执行返回 ErrTaskBusy。
func (t *OnceTask) TryRun(ctx context.Context, owner string) (*TaskRun, error) {
	lock, err := t.lock(owner)
	if err != nil {
		return nil, err
	}
	if done, err := t.completed(ctx); err != nil {
		return nil, err
	} else if done {
		return nil, ErrTaskAlreadyRan
	}
	status, err := lock.Inspect(ctx)
	if err != nil && !errors.Is(err, storage_lock.ErrLockNotFound) {
		return nil, err
	}
	if err == nil && status.Held && status.Information.OwnerId != owner {
		return nil, ErrTaskBusy
	}
	if err := lock.TryLock(ctx, owner); err != nil {
		if errors.Is(err, storage_lock.ErrLockBusy) {
			return nil, ErrTaskBusy
		}
		return nil, err
	}
	token, err := lock.GetFencingToken(ctx, owner)
	if err != nil {
		_ = lock.UnLock(context.WithoutCancel(ctx), owner)
		return nil, err
	}
	return &TaskRun{TaskID: t.taskID, Owner: owner, Fence: token, storage: t.storage, lease: lock}, nil
}

func (t *OnceTask) lock(owner string) (*storage_lock.StorageLock, error) {
	options := t.options
	options.LockId = "once-task-" + t.taskID
	return storage_lock.NewStorageLockWithOptions(t.storage, &options)
}

func doneOwner(taskID string) string  { return "once-task-done:" + taskID }
func doneLockID(taskID string) string { return "once-task-done-" + taskID }

func (t *OnceTask) completed(ctx context.Context) (bool, error) {
	marker, err := storage_lock.NewStorageLock(t.storage, doneLockID(t.taskID))
	if err != nil {
		return false, err
	}
	return marker.IsLocked(ctx)
}
