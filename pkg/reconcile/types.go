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
	"errors"
	"time"
)

// EventKind distinguishes an exit observed through the normal event path from
// one synthesized by reconciliation.
type EventKind int

const (
	// EventNormal is an exit delivered by the pidfd/BPF event path with the
	// real exit status.
	EventNormal EventKind = iota

	// EventReconciled is an exit synthesized because the process was found
	// gone during a scan without any exit event having been reported. Its
	// status is ExitStatusUnknown.
	EventReconciled

	// EventOrphan is emitted (never an exit for a tracked task) when a
	// process carrying an ownership mark is still alive but its task record
	// has disappeared. Such processes are only alerted on, never killed.
	EventOrphan
)

func (k EventKind) String() string {
	switch k {
	case EventNormal:
		return "normal"
	case EventReconciled:
		return "reconciled"
	case EventOrphan:
		return "orphan"
	default:
		return "unknown"
	}
}

// ExitEvent is the single shape published through the serial exit point.
type ExitEvent struct {
	// TaskID is the task the exit belongs to.
	TaskID string
	// Pid is the process id that exited.
	Pid int
	// Status is the real exit status for normal events, or
	// ExitStatusUnknown for reconciled events.
	Status int
	// Reason explains why Status is unknown; empty for normal events.
	Reason string
	// At is the time the exit was recorded. Injected through the Clock so
	// tests stay deterministic.
	At time.Time
	// Kind identifies which path produced the event.
	Kind EventKind
}

// OrphanEvent describes a process that is alive but no longer has a task
// record. The reconciler never acts on these beyond raising the event: the
// cost of mistakenly killing an unrelated process is far higher than the cost
// of leaving the process around for an operator to inspect.
type OrphanEvent struct {
	// OwnerID is the ownership mark found on the process (e.g. its trace /
	// container id). It may be empty when the process cannot be attributed.
	OwnerID string
	// Pid is the orphaned process id.
	Pid int
	// At is the observation time.
	At time.Time
}

// ProcessInfo describes one process observed on the machine. It is the
// neutral representation used by the injected ProcessProbe instead of exposing
// /proc directly.
type ProcessInfo struct {
	// Pid is the process id.
	Pid int
	// Alive is the liveness verdict for the batch check.
	Alive bool
	// OwnerID carries an explicit ownership mark when one is available
	// (for example a trace id annotated in the process environment or
	// cgroup). Empty for unattributed processes.
	OwnerID string
}

// ProcessProbe is the batch liveness interface. A production implementation
// enumerates /proc once per scan and returns a map keyed by pid, rather than
// opening and parsing /proc/<pid>/* files per task: a machine may run several
// thousand containers, so one directory scan plus map lookups keeps the
// per-scan cost roughly proportional to the number of system processes
// instead of to (tasks * per-file reads).
type ProcessProbe interface {
	// SnapshotProcesses returns the current set of processes keyed by pid.
	SnapshotProcesses() (map[int]ProcessInfo, error)
}

// ErrProbeUnavailable is returned when the process snapshot cannot be taken.
// A failed scan must not be interpreted as "every process has exited": the
// reconciler logs the error and retries on the next interval.
var ErrProbeUnavailable = errors.New("process probe snapshot unavailable")

// Clock abstracts time so tests never depend on wall-clock sleeps.
type Clock interface {
	Now() time.Time
}

// systemClock is the production Clock.
type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// SystemClock returns the clock backed by time.Now.
func SystemClock() Clock { return systemClock{} }
