package embedshim

import (
	"context"
	"path/filepath"
	"time"

	pkgbundle "github.com/fuweid/embedshim/pkg/bundle"
	"github.com/fuweid/embedshim/pkg/memevents"

	eventstypes "github.com/containerd/containerd/api/events"
	"github.com/containerd/containerd/events"
	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/namespaces"
	"github.com/containerd/containerd/runtime"
)

// memoryBaselineFile is stored in the bundle so that the baseline survives a
// shim restart together with the rest of the task state.
const memoryBaselineFile = "memory.events.baseline.json"

// memoryPublisher adapts the outlet's events onto the containerd event
// exchange. It runs synchronously on the outlet's goroutine, so it must not
// block: Publish only marshals and enqueues.
//
// The namespace is captured when the publisher is created because the exit
// and delete paths may run from contexts that no longer carry it (a pidfd
// callback or the shim's teardown path).
type memoryPublisher struct {
	publisher events.Publisher
	ctx       context.Context
}

func (p *memoryPublisher) Publish(ev memevents.Event) error {
	switch ev.Kind {
	case memevents.KindOOM:
		return p.publisher.Publish(p.ctx, runtime.TaskOOMEventTopic, &eventstypes.TaskOOM{
			ContainerID: ev.TaskID,
		})
	case memevents.KindExit:
		return p.publisher.Publish(p.ctx, runtime.TaskExitEventTopic, &eventstypes.TaskExit{
			ContainerID: ev.TaskID,
			ID:          ev.TaskID,
			Pid:         uint32(ev.Exit.Pid),
			ExitStatus:  uint32(ev.Exit.Status),
			ExitedAt:    ev.Exit.ExitedAt,
		})
	case memevents.KindDelete:
		return p.publisher.Publish(p.ctx, runtime.TaskDeleteEventTopic, &eventstypes.TaskDelete{
			ContainerID: ev.TaskID,
			Pid:         uint32(ev.Delete.Pid),
			ExitStatus:  uint32(ev.Delete.Status),
			ExitedAt:    ev.Delete.ExitedAt,
		})
	default:
		return nil
	}
}

// memoryEvents is the shim-level holder of a task's outlet. It is created
// after the container process exists (the cgroup path is resolved from the
// pid) and used by every path that can observe an event of that task.
type memoryEvents struct {
	outlet *memevents.Outlet
	// degraded is the marker persisted on the task so an operator can tell a
	// task that will never emit OOM from one that simply had none.
	degraded *memevents.Degraded
}

// newMemoryEvents creates and attaches the outlet for a task. Every failure
// to observe the cgroup degrades the outlet; none of them is returned as an
// error to the caller, because starting the container must not depend on
// observability.
func newMemoryEvents(ctx context.Context, bundle *pkgbundle.Bundle, pid int, exchange events.Publisher) *memoryEvents {
	return newMemoryEventsWithOptions(ctx, bundle, pid, exchange, false)
}

// newMemoryEventsWithOptions is newMemoryEvents with the exit-already-reported
// flag used by the restart path.
func newMemoryEventsWithOptions(ctx context.Context, bundle *pkgbundle.Bundle, pid int, exchange events.Publisher, exitReported bool) *memoryEvents {
	pub := &memoryPublisher{
		publisher: exchange,
		ctx:       namespaces.WithNamespace(context.Background(), bundle.Namespace),
	}

	cfg := memevents.Config{
		TaskID:       bundle.ID,
		Publisher:    pub,
		Store:        memevents.NewBaselineStore(filepath.Join(bundle.Path, memoryBaselineFile)),
		Logger:       memLogger{ctx: ctx},
		ExitReported: exitReported,
	}

	src, err := memevents.NewSourceForPID(pid)
	if err != nil {
		// Resolution failed (no cgroup file, process not in a memory
		// cgroup). Pass no source: Attach records the degradation.
		log.G(ctx).WithError(err).WithField("task", bundle.ID).
			Warn("memory events disabled: cannot resolve the cgroup counter file")
	}
	cfg.Source = src

	outlet := memevents.New(cfg)
	outlet.Attach()

	m := &memoryEvents{outlet: outlet, degraded: outlet.Degraded()}
	if m.degraded != nil {
		if err := memevents.SaveDegraded(bundle.Path, m.degraded); err != nil {
			log.G(ctx).WithError(err).WithField("task", bundle.ID).
				Warn("failed to persist the memory-events degradation marker")
		}
	}
	return m
}

// publishExit forwards the exit observed by the init process to the outlet.
func (s *shim) publishExit(status int, exitedAt time.Time) {
	if s.mem == nil {
		return
	}
	s.mem.onExit(s.init.Pid(), status, exitedAt)
}

// onExit publishes any memory event revealed by the exit-time read and then
// the exit event. It is called by the exit path before the cgroup is cleaned
// up.
func (m *memoryEvents) onExit(pid, status int, exitedAt time.Time) {
	if m == nil {
		return
	}
	m.outlet.OnExit(memevents.ExitInfo{
		Pid:      pid,
		Status:   status,
		ExitedAt: exitedAt,
	})
}

// prepareDelete performs the last synchronous counter read while the cgroup
// still exists. It must run before the OCI runtime delete.
func (m *memoryEvents) prepareDelete() {
	if m == nil {
		return
	}
	m.outlet.PrepareDelete()
}

// onDelete publishes the delete event and closes the outlet.
func (m *memoryEvents) onDelete(pid, status int, exitedAt time.Time) {
	if m == nil {
		return
	}
	m.outlet.OnDelete(memevents.DeleteInfo{
		Pid:      pid,
		Status:   status,
		ExitedAt: exitedAt,
	})
}

// close releases the outlet without publishing anything, for rollback paths.
func (m *memoryEvents) close() {
	if m == nil {
		return
	}
	m.outlet.Close()
}

// memLogger adapts the containerd logger to memevents.Logger.
type memLogger struct {
	ctx context.Context
}

func (l memLogger) Debugf(format string, args ...interface{}) {
	log.G(l.ctx).Debugf(format, args...)
}

func (l memLogger) Infof(format string, args ...interface{}) {
	log.G(l.ctx).Infof(format, args...)
}

func (l memLogger) Warnf(format string, args ...interface{}) {
	log.G(l.ctx).Warnf(format, args...)
}

var _ memevents.Logger = memLogger{}
