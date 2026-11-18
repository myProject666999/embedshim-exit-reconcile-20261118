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

package memevents

import (
	"errors"
	"time"
)

// Kind identifies one event of the container's lifecycle that is published
// through the single outlet. The causal order enforced by the outlet is
//
//	KindOOM*  ->  KindExit  ->  KindDelete
//
// where KindOOM may appear zero or more times before KindExit.
type Kind int

const (
	// KindOOM is a memory event: one or more memory counter increases that
	// were observed while the container had not yet been reported as exited.
	KindOOM Kind = iota

	// KindExit is the process exit. It is published at most once per task.
	KindExit

	// KindDelete is the last event of the task. The outlet is closed when it
	// is published; memory notifications that arrive afterwards are logged
	// only and never published.
	KindDelete
)

func (k Kind) String() string {
	switch k {
	case KindOOM:
		return "oom"
	case KindExit:
		return "exit"
	case KindDelete:
		return "delete"
	default:
		return "unknown"
	}
}

// Counters are the monotonic counters read from the cgroup memory event
// source. Their values are compared with the previously observed values; only
// an increase produces an event.
//
// The two cgroup generations do not expose the same counters:
//
//   - cgroup v2 (memory.events):   oom and oom_kill are both available on
//     kernels >= 4.19.
//   - cgroup v1 (memory.oom_control): only oom_kill is a counter; the number
//     of OOM triggers is not exposed (under_oom is a boolean state, not a
//     count). Counters.OOM therefore stays 0 and FieldSet.OOM is false.
type Counters struct {
	// OOM is the number of times the cgroup hit its memory limit (the "oom"
	// field of memory.events on cgroup v2).
	OOM uint64
	// OOMKill is the number of processes killed by the OOM killer (the
	// "oom_kill" field of memory.events on cgroup v2, the "oom_kill" field
	// of memory.oom_control on cgroup v1).
	OOMKill uint64
}

// Any reports whether at least one counter is non-zero.
func (c Counters) Any() bool {
	return c.OOM != 0 || c.OOMKill != 0
}

// Add returns c + o.
func (c Counters) Add(o Counters) Counters {
	return Counters{OOM: c.OOM + o.OOM, OOMKill: c.OOMKill + o.OOMKill}
}

// Sub returns the delta c - prev. Individual fields are clamped at zero so
// that a counter reset can never produce a negative delta. The second return
// value reports whether the counters went backwards, which means the source
// was recreated (or the host rebooted) and the previous values are stale; the
// caller must re-baseline instead of emitting an event.
func (c Counters) Sub(prev Counters) (Counters, bool) {
	var (
		reset bool
		d     Counters
	)
	if c.OOM < prev.OOM || c.OOMKill < prev.OOMKill {
		reset = true
	}
	if c.OOM > prev.OOM {
		d.OOM = c.OOM - prev.OOM
	}
	if c.OOMKill > prev.OOMKill {
		d.OOMKill = c.OOMKill - prev.OOMKill
	}
	return d, reset
}

// FieldSet records which counters the kernel actually exposed. A field that is
// absent is treated as zero *and* marked unsupported: it never produces an
// event. This distinction matters when a kernel lacks a field versus when the
// field is present and happens to be zero.
type FieldSet struct {
	// OOM is true when the source exposes a trigger counter.
	OOM bool
	// OOMKill is true when the source exposes a kill counter.
	OOMKill bool
}

// Supported reports whether at least one counter is available.
func (f FieldSet) Supported() bool {
	return f.OOM || f.OOMKill
}

// Sample is one reading of the memory event source.
type Sample struct {
	Counters Counters
	Fields   FieldSet
}

// ExitInfo describes the process exit published by the outlet.
type ExitInfo struct {
	// Pid is the process id of the exited init process.
	Pid int
	// Status is the exit status.
	Status int
	// ExitedAt is the time the exit was observed. It is filled in by the
	// outlet when zero.
	ExitedAt time.Time
	// Reason explains an incomplete exit report, for example when the
	// cgroup was already removed and the last memory counter read could not
	// be performed. Empty in the normal case.
	Reason string
}

// DeleteInfo describes the task deletion published by the outlet.
type DeleteInfo struct {
	// Pid is the process id of the init process.
	Pid int
	// Status is the last known exit status.
	Status int
	// ExitedAt is the last known exit time.
	ExitedAt time.Time
}

// Event is the single shape published by the outlet. Exactly one of Deltas,
// Exit or Delete is meaningful, selected by Kind.
type Event struct {
	// Kind selects the payload.
	Kind Kind
	// TaskID is the task the event belongs to.
	TaskID string
	// At is the publication time.
	At time.Time

	// Deltas carries the counter increases represented by a KindOOM event.
	// When the outlet coalesces several increases into one event these are
	// the summed deltas.
	Deltas Counters
	// Merged is the number of source readings folded into a KindOOM event.
	// It is 1 when the event was published without coalescing.
	Merged int
	// OOMFields records which counters were available when the KindOOM event
	// was produced.
	OOMFields FieldSet
	// Reason on a KindOOM event explains a coalescing or recovery decision.

	Exit   ExitInfo
	Delete DeleteInfo

	// Reason on a KindExit event explains why the synchronous counter read
	// could not be performed (cgroup already gone, outlet degraded, ...).
	// It is empty for a clean exit.
	Reason string
}

// Clock abstracts time so tests do not sleep.
type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// SystemClock returns a Clock backed by time.Now.
func SystemClock() Clock { return systemClock{} }

// Logger is the minimal logging surface used by the outlet. The default
// implementation discards everything.
type Logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
}

type nopLogger struct{}

func (nopLogger) Debugf(string, ...interface{}) {}
func (nopLogger) Infof(string, ...interface{})  {}
func (nopLogger) Warnf(string, ...interface{})  {}

// Publisher receives the outlet's events in publication order. Publish is
// called synchronously while the outlet is serializing events; an
// implementation that blocks delays the exit and delete events behind it, so
// it should only marshal the event and hand it to an in-process event bus.
//
// Publish is never called concurrently for a single outlet.
type Publisher interface {
	Publish(Event) error
}

// ErrSourceGone reports that the cgroup (or the file that carries the
// counters) no longer exists. It is the mapping of every "cgroup deleted"
// notification of both cgroup generations and it must never be interpreted as
// a memory event.
var ErrSourceGone = errors.New("memory event source is gone")

// ErrSourceUnsupported reports that the counter file exists but exposes
// neither counter this package understands.
var ErrSourceUnsupported = errors.New("memory event source exposes no supported counter")
