/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package reconcile

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

// DefaultReconcileInterval is the default scan period.
//
// The interval trades detection latency against scan cost. Thirty seconds is
// long enough that a periodic /proc enumeration on a machine with a few
// thousand containers stays negligible compared with the event path (which
// remains the primary, near-real-time notification), while bounding the window
// in which a lost event leaves a stopped task looking running. It is
// configurable through Config because dense nodes may prefer a longer period.
const DefaultReconcileInterval = 30 * time.Second

// maxJitterFraction spreads periodic scans so that many nodes do not all scan
// at the same instant. Each period is perturbed by at most +/-20%.
const maxJitterFraction = 0.2

// Publisher receives the events produced by the reconciler and the normal
// exit path. A production implementation forwards them onto the containerd
// event exchange. Implementations must be safe for concurrent use.
type Publisher interface {
	PublishExit(ExitEvent)
	PublishOrphan(OrphanEvent)
}

// Config configures a Reconciler.
type Config struct {
	// Interval is the nominal scan period. Zero means
	// DefaultReconcileInterval.
	Interval time.Duration
	// Jitter, when true, randomizes each period by +/-20%.
	Jitter bool
	// Probe supplies the batch process snapshot.
	Probe ProcessProbe
	// Clock supplies the current time. Defaults to SystemClock.
	Clock Clock
	// Publisher receives all emitted events.
	Publisher Publisher
	// Rand is used for jitter and may be injected. Defaults to a fresh
	// source.
	Rand *rand.Rand
}

// Result summarizes one reconciliation pass, primarily for tests and metrics.
type Result struct {
	// Scanned is the number of table entries inspected.
	Scanned int
	// Republished is the number of synthesized exit events.
	Republished int
	// SkippedNotRunning counts entries skipped because they never reached
	// PhaseRunning (created/starting) or were already stopped.
	SkippedNotRunning int
	// SkippedAlive counts entries whose process is still alive.
	SkippedAlive int
	// SkippedDeleted counts entries deleted while the pass was running.
	SkippedDeleted int
	// Orphans is the number of orphaned, attributed processes reported.
	Orphans int
}

// Reconciler runs the reconciliation path. All exit events — normal and
// reconciled — converge on Entry.reportExit, whose atomic compare-and-swap on
// the "exit reported" marker guarantees a single publication per task.
type Reconciler struct {
	registry *Registry
	probe    ProcessProbe
	clock    Clock
	pub      Publisher
	rng      *rand.Rand
	interval time.Duration
	jitter   bool

	// scanMu ensures two scans never overlap (a slow scan must not pile up
	// on top of itself). It does NOT provide the dedup guarantee — the
	// reportExit CAS does — so ReportNormalExit is deliberately not taken
	// through it.
	scanMu sync.Mutex
}

// New builds a Reconciler bound to the given task table.
func New(registry *Registry, cfg Config) *Reconciler {
	clock := cfg.Clock
	if clock == nil {
		clock = SystemClock()
	}
	interval := cfg.Interval
	if interval <= 0 {
		interval = DefaultReconcileInterval
	}
	rng := cfg.Rand
	if rng == nil {
		rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	return &Reconciler{
		registry: registry,
		probe:    cfg.Probe,
		clock:    clock,
		pub:      cfg.Publisher,
		rng:      rng,
		interval: interval,
		jitter:   cfg.Jitter,
	}
}

// MarkStarting records that Start has been requested for a task but its
// process has not yet been observed running. Such a task remains ineligible
// for reconciliation until MarkRunning is reached.
func (r *Reconciler) MarkStarting(taskID string) error {
	e, err := r.registry.Get(taskID)
	if err != nil {
		return err
	}
	e.transitionToStarting()
	return nil
}

// MarkRunning records that a task's process has been observed running, which
// makes the task eligible for reconciliation. A task that never reaches this
// point is allowed to have no process yet.
func (r *Reconciler) MarkRunning(taskID string) error {
	e, err := r.registry.Get(taskID)
	if err != nil {
		return err
	}
	e.transitionToRunning()
	return nil
}

// ReportNormalExit is the entry point used by the pidfd/BPF event path.
//
// It routes through the same serial exit point and the same atomic marker as
// reconciliation, so a normal event and a reconciled event can never both be
// published. published is false when an exit was already reported (for
// example when a delayed normal event finally arrives after reconciliation
// won the race).
func (r *Reconciler) ReportNormalExit(taskID string, status int) (published bool, err error) {
	e, err := r.registry.Get(taskID)
	if err != nil {
		return false, err
	}

	if e.reportExit(status, "") {
		r.pub.PublishExit(ExitEvent{
			TaskID: e.id,
			Pid:    e.PID(),
			Status: status,
			At:     r.clock.Now(),
			Kind:   EventNormal,
		})
		return true, nil
	}
	return false, nil
}

// Reconcile performs a single pass over the task table.
//
// It takes one batch process snapshot and, for every task confirmed to have
// run, republishes an exit when the process is gone and no exit was reported.
// The pass is defensive about concurrent mutation: tasks deleted mid-pass are
// skipped, and a probe failure aborts the pass without concluding anything
// about liveness.
func (r *Reconciler) Reconcile(ctx context.Context) (Result, error) {
	r.scanMu.Lock()
	defer r.scanMu.Unlock()

	var res Result

	// One batch enumeration for the whole pass — see ProcessProbe.
	procs, err := r.probe.SnapshotProcesses()
	if err != nil {
		return res, err
	}

	for _, snap := range r.registry.Snapshot() {
		// Re-resolve by id rather than trusting the snapshot pointer: a task
		// deleted after the snapshot was taken is gone from the registry and
		// must not receive an event.
		e, err := r.registry.Get(snap.id)
		if err == ErrTaskNotFound {
			res.SkippedDeleted++
			continue
		}
		if err != nil {
			res.SkippedDeleted++
			continue
		}
		res.Scanned++

		// Only tasks confirmed to have run participate. A created/starting
		// task with no process is not a missing event — the process simply
		// has not started yet. Stopped tasks already reported their exit.
		if e.Phase() != PhaseRunning {
			res.SkippedNotRunning++
			continue
		}

		info, ok := procs[e.PID()]
		if ok && info.Alive {
			// Process still present: nothing to do.
			res.SkippedAlive++
			continue
		}

		// Process gone while the task still claims to be running and no exit
		// was reported. The real status has already been reaped and cannot be
		// recovered, so use the explicit unknown sentinel with a reason —
		// never zero.
		if e.reportExit(ExitStatusUnknown, ReasonProcessGone) {
			r.pub.PublishExit(ExitEvent{
				TaskID: e.id,
				Pid:    e.PID(),
				Status: ExitStatusUnknown,
				Reason: ReasonProcessGone,
				At:     r.clock.Now(),
				Kind:   EventReconciled,
			})
			res.Republished++
		}
	}

	// Orphan handling: an attributed process still alive without a task
	// record. Alert only — never kill, because the cost of a mistaken kill is
	// high and an owner mark alone does not prove the process is safe to
	// remove.
	res.Orphans = r.detectOrphans(procs)
	return res, nil
}

// detectOrphans reports processes that carry an ownership mark whose task no
// longer exists. Unattributed processes are ignored because nothing ties them
// to the runtime.
func (r *Reconciler) detectOrphans(procs map[int]ProcessInfo) int {
	orphans := 0
	for _, p := range procs {
		if !p.Alive || p.OwnerID == "" {
			continue
		}
		if _, err := r.registry.Get(p.OwnerID); err == ErrTaskNotFound {
			r.pub.PublishOrphan(OrphanEvent{
				OwnerID: p.OwnerID,
				Pid:     p.Pid,
				At:      r.clock.Now(),
			})
			orphans++
		}
	}
	return orphans
}

// nextInterval returns the scan period, optionally with +/-20% jitter.
func (r *Reconciler) nextInterval() time.Duration {
	if !r.jitter {
		return r.interval
	}
	// factor in [1-jitter, 1+jitter)
	factor := 1 + (r.rng.Float64()*2-1)*maxJitterFraction
	return time.Duration(float64(r.interval) * factor)
}

// Run executes reconciliation passes until ctx is cancelled. The first pass
// runs immediately so a loss is detected without waiting a full interval;
// subsequent passes follow the (possibly jittered) interval.
func (r *Reconciler) Run(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if _, err := r.Reconcile(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				// A failed scan is retried on the next tick; liveness
				// conclusions are never drawn from a probe failure.
			}
			timer.Reset(r.nextInterval())
		}
	}
}
