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

// Package reconcile implements a periodic reconciliation path that backs up
// the event-driven exit monitoring (pidfd + sched_process_exit tracepoint).
//
// The event path can lose an exit notification in several ways: the userspace
// event ring buffer fills up and the sample is dropped, the control process is
// killed before it can forward the event, the kernel tracepoint buffer
// overflows, and so on. When the notification is lost the task stays in the
// running state forever and the upper layer keeps holding the container's
// name and port. The reconciler periodically compares the task table against
// the set of live processes and republishes an exit event for every task that
// is confirmed to have run but whose process has disappeared without an exit
// event being reported.
//
// The package only depends on the standard library so that the concurrency
// and idempotency guarantees can be exercised without a kernel, a BPF
// subsystem or the real /proc: tests inject a fake clock and a fake process
// probe.
package reconcile

import (
	"errors"
	"sync"
)

// Phase is the lifecycle stage of a task. Reconciliation only acts on tasks
// that have been confirmed to actually run.
type Phase int

const (
	// PhaseCreated means the task record exists but the container process
	// has never been started. A missing process at this stage is expected
	// (the process simply has not been spawned yet) and must NOT be treated
	// as a lost exit event.
	PhaseCreated Phase = iota

	// PhaseStarting covers the window in which Start has been requested but
	// the process has not been observed running yet. As with PhaseCreated,
	// the process is allowed to be absent.
	PhaseStarting

	// PhaseRunning means the process has been observed running at least
	// once. Only tasks in this phase are eligible for reconciliation.
	PhaseRunning

	// PhaseStopped means the process exited and the state transition has
	// already been applied. Nothing to reconcile.
	PhaseStopped

	// PhaseDeleted means the task has been deleted and its entry removed
	// from the task table. The reconciler must never publish for a deleted
	// task.
	PhaseDeleted
)

func (p Phase) String() string {
	switch p {
	case PhaseCreated:
		return "created"
	case PhaseStarting:
		return "starting"
	case PhaseRunning:
		return "running"
	case PhaseStopped:
		return "stopped"
	case PhaseDeleted:
		return "deleted"
	default:
		return "unknown"
	}
}

// ExitStatusUnknown is the sentinel exit status used when reconciliation
// observes that a process is gone but the real exit status has already been
// reaped and cannot be recovered.
//
// It is deliberately NOT zero: zero denotes a successful exit and reporting
// success for a process whose outcome is unknown would mislead the upper
// layer into releasing resources as if the workload had completed cleanly.
// The ExitInfo.Reason field carried alongside it explains why the real status
// is unavailable.
const ExitStatusUnknown = 255

// Reasons recorded on reconciled exit events. They make the sentinel status
// self-explanatory in logs and event payloads.
const (
	// ReasonProcessGone is used when the process has disappeared and no
	// exit status can be read any more.
	ReasonProcessGone = "process exited but exit status was already reaped; real exit code unknown"
)

// ErrTaskNotFound is returned when the registry has no entry for the task,
// for instance because it was deleted concurrently with a reconciliation
// pass. Callers treat it as a benign skip rather than a hard failure.
var ErrTaskNotFound = errors.New("task not found in task table")

// Entry is one row of the task table.
//
// The "exit reported" marker lives here as an atomic flag. Both the normal
// event path and the reconciler go through Entry.reportExit; the first
// caller to complete the state transition wins the compare-and-swap and is
// the only one that publishes. The loser observes exitReported == true and
// publishes nothing, which makes republishing idempotent and free of
// duplicates regardless of how the two paths interleave.
//
// The marker is deliberately in-memory only and is not persisted. After a
// crash/restart the task table is rebuilt from the on-disk bundle state and
// the BPF exited-events map (see reload.go in the parent package): a task
// whose exit was already reported is rebuilt directly into PhaseStopped, so
// the reconstructed flag (phase == stopped) already reflects reality. Only
// tasks that were genuinely still running are rebuilt as PhaseRunning and
// given a fresh, unset flag. Recovery therefore relies on state
// reconstruction rather than on persisting the flag itself.
type Entry struct {
	mu sync.Mutex

	id    string
	pid   int
	phase Phase

	// exitReported is the atomic "exit already reported" marker. It is only
	// read and written while mu is held, which gives the compare-and-swap
	// atomicity together with the phase transition.
	exitReported bool
	exitStatus   int
	exitReason   string
}

// NewEntry creates a task-table entry in PhaseCreated.
func NewEntry(id string, pid int) *Entry {
	return &Entry{
		id:    id,
		pid:   pid,
		phase: PhaseCreated,
	}
}

// ID returns the task identifier.
func (e *Entry) ID() string {
	return e.id
}

// PID returns the process id recorded for the task.
func (e *Entry) PID() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.pid
}

// Phase returns the current lifecycle phase.
func (e *Entry) Phase() Phase {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.phase
}

// ExitReported reports whether an exit event has already been published for
// this entry.
func (e *Entry) ExitReported() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.exitReported
}

// ExitStatus returns the recorded exit status, or ExitStatusUnknown when the
// real status could not be recovered.
func (e *Entry) ExitStatus() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.exitStatus
}

// ExitReason returns the explanation associated with a reconciled exit, or
// the empty string for a normal exit event.
func (e *Entry) ExitReason() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.exitReason
}

// transitionToStarting marks that Start has been requested but the process has
// not been observed running yet.
func (e *Entry) transitionToStarting() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.phase == PhaseCreated {
		e.phase = PhaseStarting
	}
}

// transitionToRunning moves a created/starting task into PhaseRunning. It is
// called once the process has been confirmed to have started, which is what
// makes the task eligible for reconciliation.
func (e *Entry) transitionToRunning() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.phase == PhaseCreated || e.phase == PhaseStarting {
		e.phase = PhaseRunning
	}
}

// markDeleted moves the entry into PhaseDeleted. Deleted tasks are skipped by
// the reconciler and removed from the table by the registry.
func (e *Entry) markDeleted() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.phase = PhaseDeleted
}

// reportExit is the single, serial exit point shared by the normal event
// path and reconciliation.
//
// It performs an atomic compare-and-swap on the "exit reported" marker. The
// winner applies the stopped transition, records the status (or the unknown
// sentinel plus reason) and returns published == true so the caller emits the
// event exactly once. Every subsequent call — a delayed normal event arriving
// after reconciliation, or a second reconciliation pass — loses the CAS and
// returns published == false without touching the recorded status.
func (e *Entry) reportExit(status int, reason string) (published bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.exitReported || e.phase == PhaseDeleted || e.phase == PhaseStopped {
		return false
	}

	e.exitReported = true
	e.exitStatus = status
	e.exitReason = reason
	e.phase = PhaseStopped
	return true
}

// Registry is the concurrency-safe task table. The mutex makes "look up then
// act" races benign: a task deleted while a scan is in progress simply fails
// the lookup and is skipped instead of causing a nil dereference or a
// spurious event.
type Registry struct {
	mu      sync.RWMutex
	entries map[string]*Entry
}

// NewRegistry creates an empty task table.
func NewRegistry() *Registry {
	return &Registry{entries: make(map[string]*Entry)}
}

// Add inserts a task entry. It returns an error if the id is already taken.
func (r *Registry) Add(e *Entry) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.entries[e.id]; ok {
		return errors.New("task " + e.id + " already exists in task table")
	}
	r.entries[e.id] = e
	return nil
}

// Get returns the entry for id, or ErrTaskNotFound if it has been deleted.
func (r *Registry) Get(id string) (*Entry, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	e, ok := r.entries[id]
	if !ok {
		return nil, ErrTaskNotFound
	}
	return e, nil
}

// Delete removes a task. It first marks the entry deleted so that an
// in-flight reconciliation that already holds the pointer can never publish
// for it, then drops it from the table. Deleting an unknown id is a no-op.
func (r *Registry) Delete(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if e, ok := r.entries[id]; ok {
		e.markDeleted()
		delete(r.entries, id)
	}
}

// Snapshot returns a stable slice of the entries currently in the table.
//
// The lock is held only for the copy; reconciliation then works off the
// snapshot without blocking concurrent task creation or deletion for the
// whole duration of the scan. Entries deleted afterwards remain reachable
// through the snapshot but their own phase/marker state rejects publishing.
func (r *Registry) Snapshot() []*Entry {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]*Entry, 0, len(r.entries))
	for _, e := range r.entries {
		out = append(out, e)
	}
	return out
}

// Len returns the number of registered tasks.
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.entries)
}
