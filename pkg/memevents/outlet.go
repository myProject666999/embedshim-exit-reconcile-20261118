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

// Package memevents turns cgroup memory counters into orderly task events.
//
// The cgroup memory counters (memory.events on cgroup v2, memory.oom_control
// on cgroup v1) increase when a container hits its memory limit or when the
// kernel kills a process for it. Those increases and the process exit come
// from two independent sources, so their arrival order is not fixed: the exit
// may be observed before, after, or without the counter notification, and the
// cgroup may be removed before the notification is delivered.
//
// An Outlet serializes every event of one task through a single outlet:
//
//	OOM (zero or more) -> Exit (at most once) -> Delete (exactly once, last)
//
// Every exit and delete path performs a synchronous counter read before it
// publishes, so a memory event that the asynchronous watch has not delivered
// yet is still published ahead of the exit. Ordering is a property of the
// outlet, never of arrival time.
//
// This package deliberately depends only on the standard library for its core
// logic so that all ordering rules can be exercised with fake sources, fake
// clocks and fake publishers, without a kernel or a live cgroup. The cgroup
// v1/v2 adapters live in source.go and are the only generation-aware code.
package memevents

import (
	"errors"
	"os"
	"sync"
	"time"
)

// Mode describes how much of the memory event path is available for a task.
type Mode int

const (
	// ModeFull is a readable counter file with a working asynchronous watch.
	ModeFull Mode = iota

	// ModeExitOnly means the counters are readable but the watch could not
	// be established. No asynchronous events are produced; the synchronous
	// read before exit and delete still reports increases. This is a
	// partial degradation and is recorded on the task like a full one.
	ModeExitOnly

	// ModeDisabled means no memory event can be produced at all (missing
	// file, permission denied, no supported counter). The task still starts;
	// Degraded records why.
	ModeDisabled
)

func (m Mode) String() string {
	switch m {
	case ModeFull:
		return "full"
	case ModeExitOnly:
		return "exit-only"
	case ModeDisabled:
		return "disabled"
	default:
		return "unknown"
	}
}

// Config configures an Outlet.
type Config struct {
	// TaskID is the task the events belong to.
	TaskID string
	// Source is the cgroup memory event source. When nil the outlet is
	// degraded from the start.
	Source Source
	// Store persists the baseline. Nil keeps the baseline in memory only.
	Store *BaselineStore
	// Publisher receives the events. Required.
	Publisher Publisher
	// Clock defaults to SystemClock.
	Clock Clock
	// Logger defaults to a discarding logger.
	Logger Logger
	// MergeWindow enables coalescing of asynchronous counter increases.
	//
	// Zero publishes every observed increase as its own event, preserving
	// arrival time. A positive window accumulates increases and publishes
	// one event carrying the summed deltas and the number of folded
	// readings. Coalescing is friendlier to consumers but discards the time
	// precision of each individual increase; see docs/memory-events.md for
	// the tradeoff.
	//
	// The window never delays the exit or the delete: both flush pending
	// increases synchronously first.
	MergeWindow time.Duration
	// AfterFunc schedules the merge flush. It exists for tests and defaults
	// to time.AfterFunc.
	AfterFunc func(time.Duration, func()) *time.Timer
	// ExitReported starts the outlet in the "exit already published" state.
	// It is used by the restart path for a task that is found stopped: its
	// exit was published by the previous run and must not be published
	// again, but the task still needs its delete event.
	ExitReported bool
}

// Recovery describes what the first sample of a run saw relative to the
// persisted baseline.
type Recovery struct {
	// Prev is the persisted baseline, zero when the task had none.
	Prev Counters
	// Curr is the sample taken at attach time.
	Curr Counters
	// Growth is the increase between Prev and Curr. It is recorded for
	// observability only and is never published: see Attach.
	Growth Counters
	// Reset is true when the counters went backwards, which means the
	// source was recreated (typically a host reboot).
	Reset bool
	// HadBaseline is false on the first run of a task.
	HadBaseline bool
}

// Outlet is the single, ordered publication point of one task's memory,
// exit and delete events. It is safe for concurrent use; all entry points
// serialize on one mutex, which is what makes the causal order hold no matter
// which goroutine observes an event first.
type Outlet struct {
	cfg Config

	mu sync.Mutex

	attached bool
	mode     Mode
	source   Source
	watch    Watch
	degrade  *Degraded

	baseline Counters
	fields   FieldSet
	gen      Generation
	recovery Recovery

	// sourceGone is set when the watch reports the cgroup deleted or the
	// counter file disappears. It only suppresses further reads; it is
	// never turned into a memory event.
	sourceGone bool

	// pending accumulates increases when MergeWindow > 0.
	pending      Counters
	pendingCount int
	pendingSince time.Time
	timer        *time.Timer

	exitPublished  bool
	deleted        bool
	closed         bool
	deletePrepared bool

	// watchStopped is set when the outlet itself stopped the watch (exit or
	// delete). It distinguishes "the channel closed because we closed it"
	// from "the channel closed because the cgroup went away".
	watchStopped bool

	// suppressed counts notes dropped after the outlet closed, so logging
	// stays bounded.
	suppressed int
}

// New creates an outlet. Attach must be called before events are handled.
func New(cfg Config) *Outlet {
	if cfg.Clock == nil {
		cfg.Clock = SystemClock()
	}
	if cfg.Logger == nil {
		cfg.Logger = nopLogger{}
	}
	if cfg.AfterFunc == nil {
		cfg.AfterFunc = time.AfterFunc
	}
	return &Outlet{cfg: cfg}
}

// Attach probes the source, establishes the baseline and starts the watch.
//
// Attach NEVER fails because of the cgroup: a missing or unreadable counter
// file, an unsupported kernel and a failed watch all degrade the outlet and
// are recorded in Degraded(). Starting the container must not depend on
// observability. It returns the mode actually achieved.
//
// Baseline rules, in the order they apply:
//
//  1. Counters that had already accumulated before the first read of this
//     run are never reported. Without a persisted baseline the current
//     reading becomes the baseline.
//  2. With a persisted baseline, growth between it and the first read is not
//     reported either. Those increases happened while nobody was watching,
//     so their position relative to the exit event — which may already have
//     happened — cannot be established. The persisted value defines what
//     "already reported" means across restarts, including across a host
//     reboot in which the shim was down.
//  3. Counters that went backwards indicate a recreated source (usually a
//     host reboot). The baseline is reset to the current reading and no
//     delta is emitted.
//
// In all cases the resulting baseline is the current reading.
func (o *Outlet) Attach() Mode {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.attached {
		return o.mode
	}
	o.attached = true
	o.exitPublished = o.cfg.ExitReported

	if o.cfg.Source == nil {
		o.mode = ModeDisabled
		o.setDegradedLocked(&Degraded{
			Reason: DegradedUnsupported,
			Detail: "no memory event source configured",
			At:     o.cfg.Clock.Now(),
		})
		return o.mode
	}

	o.source = o.cfg.Source
	o.gen = o.source.Generation()

	sample, err := o.source.Sample()
	if err != nil {
		o.source = nil
		reason := classifyDegraded(err)
		if errors.Is(err, ErrSourceGone) {
			o.sourceGone = true
		}
		o.mode = ModeDisabled
		o.setDegradedLocked(&Degraded{
			Reason:     reason,
			Detail:     err.Error(),
			Generation: o.gen,
			At:         o.cfg.Clock.Now(),
			Err:        err,
		})
		return o.mode
	}
	o.absorbInitialSampleLocked(sample)

	// The counters are readable from here on. A failed watch degrades only
	// the asynchronous notification, not the synchronous reads.
	watch, err := o.source.Watch()
	if err != nil {
		reason := DegradedWatchFailed
		if errors.Is(err, ErrSourceGone) {
			o.sourceGone = true
			reason = DegradedSourceGone
		}
		o.mode = ModeExitOnly
		o.setDegradedLocked(&Degraded{
			Reason:     reason,
			Detail:     err.Error(),
			Generation: o.gen,
			At:         o.cfg.Clock.Now(),
			Err:        err,
		})
		return o.mode
	}

	o.watch = watch
	o.mode = ModeFull
	go o.watchLoop(watch)
	return o.mode
}

// absorbInitialSampleLocked applies the baseline rules of Attach.
func (o *Outlet) absorbInitialSampleLocked(sample Sample) {
	curr := sample.Counters
	recovery := Recovery{Curr: curr}

	var prev *Baseline
	if o.cfg.Store != nil {
		p, err := o.cfg.Store.Load()
		if err != nil {
			// A corrupt baseline is treated as absent: the conservative
			// choice is to re-baseline, never to invent events from it.
			o.cfg.Logger.Warnf("memevents: task %s: %v; re-baselining", o.cfg.TaskID, err)
		}
		prev = p
	}

	if prev != nil {
		recovery.HadBaseline = true
		recovery.Prev = prev.Counters
		delta, reset := curr.Sub(prev.Counters)
		switch {
		case reset:
			recovery.Reset = true
			o.cfg.Logger.Warnf(
				"memevents: task %s: counters went backwards (%+v -> %+v), the cgroup was recreated or the host rebooted; resetting baseline without emitting events",
				o.cfg.TaskID, prev.Counters, curr,
			)
		case delta.Any():
			recovery.Growth = delta
			o.cfg.Logger.Infof(
				"memevents: task %s: counters grew by %+v while the task was not watched; not reported (persisted baseline %+v, current %+v)",
				o.cfg.TaskID, delta, prev.Counters, curr,
			)
		}
		if prev.Fields != sample.Fields {
			o.cfg.Logger.Infof(
				"memevents: task %s: counter fields changed (%+v -> %+v); re-baselining",
				o.cfg.TaskID, prev.Fields, sample.Fields,
			)
		}
	}

	o.baseline = curr
	o.fields = sample.Fields
	o.recovery = recovery
	o.persistLocked()
}

// setDegradedLocked records why memory events are partly or fully
// unavailable. The mode is set by the caller: a readable counter file stays
// ModeExitOnly, anything else becomes ModeDisabled.
func (o *Outlet) setDegradedLocked(d *Degraded) {
	o.degrade = d
	o.cfg.Logger.Warnf("memevents: task %s: %s", o.cfg.TaskID, d.Marker())
}

// Degraded returns the reason memory events are unavailable, or nil.
func (o *Outlet) Degraded() *Degraded {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.degrade
}

// Mode returns the current mode.
func (o *Outlet) Mode() Mode {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.mode
}

// Recovery returns what the first sample of this run observed.
func (o *Outlet) Recovery() Recovery {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.recovery
}

// Baseline returns a copy of the current baseline.
func (o *Outlet) Baseline() Baseline {
	o.mu.Lock()
	defer o.mu.Unlock()
	return Baseline{
		Counters:   o.baseline,
		Fields:     o.fields,
		Generation: o.gen,
		UpdatedAt:  o.cfg.Clock.Now(),
	}
}

// watchLoop consumes the watch channel. A closed channel means the source is
// gone; the loop exits and leaves the outlet readable but silent.
func (o *Outlet) watchLoop(w Watch) {
	for note := range w.C() {
		o.handleNote(note)
		o.mu.Lock()
		closed := o.closed
		o.mu.Unlock()
		if closed {
			return
		}
	}
	o.mu.Lock()
	if !o.watchStopped {
		o.sourceGone = true
	}
	o.mu.Unlock()
}

func (o *Outlet) handleNote(note Note) {
	o.mu.Lock()
	closed := o.closed
	o.mu.Unlock()
	if closed {
		o.mu.Lock()
		o.logSuppressedLocked("memory event notification after delete")
		o.mu.Unlock()
		return
	}

	if note.Gone {
		o.mu.Lock()
		o.sourceGone = true
		o.mu.Unlock()
		// The cgroup-deleted notification is not a memory event. Never
		// sample it into an event, and never treat the vanished counters
		// as a drop to zero.
		o.cfg.Logger.Debugf("memevents: task %s: cgroup deleted notification; stopping memory watch", o.cfg.TaskID)
		return
	}
	if note.Err != nil {
		if errors.Is(note.Err, ErrSourceGone) {
			o.mu.Lock()
			o.sourceGone = true
			o.mu.Unlock()
		}
		o.cfg.Logger.Warnf("memevents: task %s: watch error: %v", o.cfg.TaskID, note.Err)
		return
	}
	if err := o.observe(observeAsync); err != nil && !errors.Is(err, ErrSourceGone) {
		o.cfg.Logger.Warnf("memevents: task %s: sample after notification: %v", o.cfg.TaskID, err)
	}
}

// observeMode selects how a synchronous read is flushed.
type observeMode int

const (
	// observeAsync is a read triggered by a watch notification. It may be
	// coalesced in the merge window.
	observeAsync observeMode = iota
	// observeExit is the read done by OnExit. It flushes immediately.
	observeExit
	// observeDelete is the read done by OnDelete. It flushes immediately.
	observeDelete
)

// observe samples the counters right now and folds any increase into the
// pending memory event. It is the only place that maps counters to events.
func (o *Outlet) observe(mode observeMode) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.observeLocked(mode)
}

func (o *Outlet) observeLocked(mode observeMode) error {
	if o.sourceGone || o.deleted || o.source == nil {
		return nil
	}

	if o.exitPublished {
		// The exit is already published. Emitting a memory event now would
		// put OOM after Exit and break the causal order, so the increase is
		// folded into the baseline only. It is not lost from the baseline's
		// point of view: a restart compares against the advanced value.
		if sample, err := o.source.Sample(); err == nil {
			o.advanceLocked(sample)
		}
		o.logSuppressedLocked("memory event observed after exit")
		return nil
	}

	sample, err := o.source.Sample()
	if err != nil {
		if errors.Is(err, ErrSourceGone) {
			o.sourceGone = true
		}
		return err
	}

	delta, ok, reset := o.advanceLocked(sample)
	if !ok {
		return nil
	}
	o.foldLocked(delta, sample.Fields, mode)
	_ = reset
	return nil
}

// advanceLocked updates the baseline from a sample and returns the increase
// since the previous baseline. It returns ok == false when there is nothing
// to report: no growth, a counter reset, or a change in the set of exposed
// fields. In those cases the baseline has already been re-anchored.
func (o *Outlet) advanceLocked(sample Sample) (delta Counters, ok bool, reset bool) {
	if o.fields != sample.Fields && o.fields != (FieldSet{}) {
		// A field appearing or disappearing makes the old value
		// incomparable (a newly exposed counter with a non-zero value is
		// indistinguishable from growth). Re-anchor silently.
		o.cfg.Logger.Infof(
			"memevents: task %s: counter fields changed (%+v -> %+v); re-baselining",
			o.cfg.TaskID, o.fields, sample.Fields,
		)
		o.baseline = sample.Counters
		o.fields = sample.Fields
		return Counters{}, false, false
	}

	delta, reset = sample.Counters.Sub(o.baseline)
	o.fields = sample.Fields
	o.baseline = sample.Counters
	if reset {
		o.cfg.Logger.Warnf(
			"memevents: task %s: counters went backwards; the cgroup was recreated; re-baselining without events",
			o.cfg.TaskID,
		)
		return Counters{}, false, true
	}
	delta = maskDelta(delta, sample.Fields)
	return delta, delta.Any(), false
}

// foldLocked adds an increase to the pending memory event and decides when
// to publish it.
func (o *Outlet) foldLocked(delta Counters, fields FieldSet, mode observeMode) {
	delta = maskDelta(delta, fields)
	if !delta.Any() {
		return
	}

	if o.pendingCount == 0 {
		o.pendingSince = o.cfg.Clock.Now()
	}
	o.pending = o.pending.Add(delta)
	o.pendingCount++
	o.fields = fields

	if mode != observeAsync || o.cfg.MergeWindow <= 0 {
		// The exit and delete paths never wait for a merge window: their
		// own synchronous read must be published immediately before them,
		// while the counters (and the cgroup) still exist.
		o.flushLocked("")
		return
	}
	if o.timer == nil {
		o.timer = o.cfg.AfterFunc(o.cfg.MergeWindow, o.flushPending)
	}
}

// flushPending is the merge-window timer callback.
func (o *Outlet) flushPending() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.timer = nil
	o.flushLocked("")
}

// Flush publishes any increases accumulated in the merge window. It is
// exposed for tests and for callers that want deterministic timing; the exit
// and delete paths flush on their own.
func (o *Outlet) Flush() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.flushLocked("")
}

// flushLocked publishes the pending memory event, if any. It is called with
// the outlet mutex held, so the publication order is the lock acquisition
// order: whatever the ordering of the callers, OOM events always come before
// the exit event of the same critical section.
func (o *Outlet) flushLocked(reason string) {
	if !o.pending.Any() {
		return
	}
	ev := Event{
		Kind:      KindOOM,
		TaskID:    o.cfg.TaskID,
		At:        o.pendingSince,
		Deltas:    o.pending,
		Merged:    o.pendingCount,
		OOMFields: o.fields,
		Reason:    reason,
	}
	o.pending = Counters{}
	o.pendingCount = 0
	o.pendingSince = time.Time{}
	if o.timer != nil {
		o.timer.Stop()
		o.timer = nil
	}

	o.publishLocked(ev)
	o.persistLocked()
}

func maskDelta(d Counters, f FieldSet) Counters {
	if !f.OOM {
		d.OOM = 0
	}
	if !f.OOMKill {
		d.OOMKill = 0
	}
	return d
}

// OnExit publishes, in order, any memory event revealed by a synchronous
// counter read and then the exit event.
//
// The synchronous read is what makes ordering independent of arrival order:
// if the kill notification has not been delivered yet, the counters already
// contain the kill, and reading them here publishes OOM before Exit.
//
// Cost: one open+read of a small cgroupfs file (a few microseconds on a warm
// page cache, tens of microseconds on a cold one) plus, only when the
// counters grew, one atomic baseline write. It runs on the goroutine that
// observed the exit, before the exit event is published and before any cgroup
// cleanup, so the counter file is guaranteed to still exist. OnExit is
// idempotent: a second call publishes nothing.
func (o *Outlet) OnExit(info ExitInfo) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.deleted || o.closed {
		o.logSuppressedLocked("exit event after delete")
		return
	}
	if o.exitPublished {
		return
	}

	var reason string
	switch {
	case o.mode == ModeDisabled:
		reason = "memory events unavailable: " + string(o.degrade.Reason)
	case o.sourceGone:
		reason = "memory counters unavailable at exit: cgroup already removed"
	default:
		if err := o.observeLocked(observeExit); err != nil {
			if errors.Is(err, ErrSourceGone) {
				reason = "memory counters unavailable at exit: cgroup already removed"
			} else {
				reason = "memory counters unavailable at exit: " + err.Error()
				o.cfg.Logger.Warnf("memevents: task %s: exit-time sample: %v", o.cfg.TaskID, err)
			}
		}
	}
	// Flush whatever the asynchronous path had accumulated: it is still a
	// memory event and must precede the exit.
	o.flushLocked(reason)

	if info.ExitedAt.IsZero() {
		info.ExitedAt = o.cfg.Clock.Now()
	}
	o.exitPublished = true
	o.stopWatchLocked()

	o.publishLocked(Event{
		Kind:   KindExit,
		TaskID: o.cfg.TaskID,
		At:     info.ExitedAt,
		Exit:   info,
		Reason: reason,
	})
}

// PrepareDelete performs the final synchronous counter read and publishes
// any memory event it reveals. It MUST be called before the cgroup is
// removed (before the OCI runtime delete), because that is the last moment at
// which the counters still exist.
//
// It is separate from OnDelete because the actual deletion can still fail:
// the read is harmless to perform early, while publishing the delete event
// for a task that was not deleted is not. Callers should invoke
// PrepareDelete, then the runtime deletion, then OnDelete.
//
// PrepareDelete is idempotent. OnDelete calls it implicitly when a caller
// uses the single-call form; in that case the read may already fail because
// the cgroup is gone, which only costs the final memory event.
func (o *Outlet) PrepareDelete() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.prepareDeleteLocked()
}

func (o *Outlet) prepareDeleteLocked() {
	if o.deletePrepared || o.deleted || o.closed {
		return
	}
	o.deletePrepared = true

	// Final read. The exit may never have happened (a created task deleted
	// directly), in which case this is the only read that can observe an
	// increase.
	if err := o.observeLocked(observeDelete); err != nil && !errors.Is(err, ErrSourceGone) {
		o.cfg.Logger.Warnf("memevents: task %s: delete-time sample: %v", o.cfg.TaskID, err)
	}
	o.flushLocked("")
}

// OnDelete publishes the delete event and closes the outlet. It is
// idempotent.
//
// After OnDelete, any counter increase that the watch still delivers is
// logged and dropped: the delete is the last event of the task.
func (o *Outlet) OnDelete(info DeleteInfo) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.deleted || o.closed {
		return
	}

	o.prepareDeleteLocked()

	// No synthetic exit is invented here: a task that is created and
	// deleted without ever running legitimately has no exit event, and a
	// fabricated one would corrupt the lifecycle counts that consumers rely
	// on. The ordering guarantee only says that a delete never precedes an
	// exit that does exist.
	o.deleted = true
	o.closed = true
	o.stopWatchLocked()

	if info.ExitedAt.IsZero() {
		info.ExitedAt = o.cfg.Clock.Now()
	}
	o.publishLocked(Event{
		Kind:   KindDelete,
		TaskID: o.cfg.TaskID,
		At:     info.ExitedAt,
		Delete: info,
	})
	o.persistLocked()
}

// Close stops the watch without publishing anything. It is used on rollback
// paths where the task never became visible. It does not publish a delete.
func (o *Outlet) Close() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.closed = true
	o.stopWatchLocked()
}

func (o *Outlet) stopWatchLocked() {
	o.watchStopped = true
	if o.timer != nil {
		o.timer.Stop()
		o.timer = nil
	}
	if o.watch != nil {
		// Close is idempotent and only signals the pump goroutines; the
		// watch loop exits on the closed channel without needing the outlet
		// mutex, so this cannot deadlock.
		_ = o.watch.Close()
		o.watch = nil
	}
}

// publishLocked hands the event to the publisher while the outlet mutex is
// held. That is what serializes publication: the publisher is never called
// concurrently, and its call order is exactly the causal order. A publisher
// that blocks therefore delays the following exit/delete; implementations
// must only marshal and enqueue.
func (o *Outlet) publishLocked(ev Event) {
	if o.cfg.Publisher == nil {
		return
	}
	if err := o.cfg.Publisher.Publish(ev); err != nil {
		o.cfg.Logger.Warnf("memevents: task %s: publish %s event: %v", o.cfg.TaskID, ev.Kind, err)
	}
}

// persistLocked records the baseline after every publication so that a
// restart can tell "already reported" from "not yet reported".
//
// The write is synchronous because its only purpose is to survive a crash or
// a host reboot: an asynchronous write could be lost exactly when it
// matters. It costs one small write (tens of bytes) per observed counter
// increase, not per sample. A write error is logged and ignored: losing the
// baseline only makes the next run more conservative (it can suppress a real
// increase), while failing the task over it would be worse.
func (o *Outlet) persistLocked() {
	if o.cfg.Store == nil {
		return
	}
	b := Baseline{
		Counters:   o.baseline,
		Fields:     o.fields,
		Generation: o.gen,
		UpdatedAt:  o.cfg.Clock.Now(),
	}
	if err := o.cfg.Store.Save(b); err != nil {
		o.cfg.Logger.Warnf("memevents: task %s: persist baseline: %v", o.cfg.TaskID, err)
	}
}

// logSuppressedLocked records a dropped notification. The caller must hold
// o.mu.
func (o *Outlet) logSuppressedLocked(what string) {
	o.suppressed++
	n := o.suppressed
	state := o.stateNameLocked()

	// Log the first occurrence and then occasionally, so a noisy source that
	// keeps notifying after the outlet closed cannot flood the log.
	if n == 1 || n%64 == 0 {
		o.cfg.Logger.Infof("memevents: task %s: %s; event dropped (outlet %s)", o.cfg.TaskID, what, state)
	}
}

func (o *Outlet) stateNameLocked() string {
	switch {
	case o.deleted:
		return "deleted"
	case o.exitPublished:
		return "exited"
	default:
		return "open"
	}
}

// State is an introspection snapshot for tests and diagnostics.
type State struct {
	Mode          Mode
	Degraded      *Degraded
	Baseline      Counters
	Fields        FieldSet
	ExitPublished bool
	Deleted       bool
	SourceGone    bool
	Pending       Counters
	PendingReads  int
	Recovery      Recovery
}

// State returns a snapshot.
func (o *Outlet) State() State {
	o.mu.Lock()
	defer o.mu.Unlock()
	return State{
		Mode:          o.mode,
		Degraded:      o.degrade,
		Baseline:      o.baseline,
		Fields:        o.fields,
		ExitPublished: o.exitPublished,
		Deleted:       o.deleted,
		SourceGone:    o.sourceGone,
		Pending:       o.pending,
		PendingReads:  o.pendingCount,
		Recovery:      o.recovery,
	}
}

func classifyDegraded(err error) DegradedReason {
	switch {
	case errors.Is(err, ErrSourceGone):
		return DegradedSourceGone
	case errors.Is(err, ErrSourceUnsupported):
		return DegradedUnsupported
	case errors.Is(err, os.ErrPermission):
		return DegradedPermission
	default:
		return DegradedUnsupported
	}
}
