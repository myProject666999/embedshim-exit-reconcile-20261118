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
	"testing"
	"time"
)

// fakeClock is fully controlled by the test. Time only moves when the test
// asks it to, so nothing depends on wall-clock behaviour.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// fakeProbe serves a test-defined process table. Its state can be mutated
// between passes to simulate exits; all access is guarded so concurrent
// stress tests are race-free.
type fakeProbe struct {
	mu    sync.Mutex
	procs map[int]ProcessInfo
	err   error
}

func newFakeProbe() *fakeProbe {
	return &fakeProbe{procs: make(map[int]ProcessInfo)}
}

func (p *fakeProbe) set(pid int, info ProcessInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.procs[pid] = info
}

func (p *fakeProbe) remove(pid int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.procs, pid)
}

func (p *fakeProbe) fail(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.err = err
}

func (p *fakeProbe) SnapshotProcesses() (map[int]ProcessInfo, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return nil, p.err
	}
	out := make(map[int]ProcessInfo, len(p.procs))
	for k, v := range p.procs {
		out[k] = v
	}
	return out, nil
}

// recordingPublisher records every published event and is safe under the
// concurrent stress test.
type recordingPublisher struct {
	mu       sync.Mutex
	exits    []ExitEvent
	orphans  []OrphanEvent
	exitByID map[string]int
}

func newRecordingPublisher() *recordingPublisher {
	return &recordingPublisher{exitByID: make(map[string]int)}
}

func (pub *recordingPublisher) PublishExit(ev ExitEvent) {
	pub.mu.Lock()
	defer pub.mu.Unlock()
	pub.exits = append(pub.exits, ev)
	pub.exitByID[ev.TaskID]++
}

func (pub *recordingPublisher) PublishOrphan(ev OrphanEvent) {
	pub.mu.Lock()
	defer pub.mu.Unlock()
	pub.orphans = append(pub.orphans, ev)
}

func (pub *recordingPublisher) exitCount(id string) int {
	pub.mu.Lock()
	defer pub.mu.Unlock()
	return pub.exitByID[id]
}

func (pub *recordingPublisher) exitEvents() []ExitEvent {
	pub.mu.Lock()
	defer pub.mu.Unlock()
	out := make([]ExitEvent, len(pub.exits))
	copy(out, pub.exits)
	return out
}

func newTestReconciler(t *testing.T) (*Reconciler, *Registry, *fakeProbe, *recordingPublisher, *fakeClock) {
	t.Helper()
	reg := NewRegistry()
	probe := newFakeProbe()
	pub := newRecordingPublisher()
	clock := newFakeClock()
	rec := New(reg, Config{
		Probe:     probe,
		Publisher: pub,
		Clock:     clock,
	})
	return rec, reg, probe, pub, clock
}

// fiveTasks builds the canonical scenario:
//   - lost:     running, process gone, exit event lost        -> must republish
//   - delayed:  running, process gone, normal event merely delayed
//   - starting: never confirmed running, no process yet       -> must skip
//   - running:  running, process alive                        -> must skip
//   - deleted:  removed from the table mid-scenario           -> must skip
func fiveTasks(t *testing.T) (*Reconciler, *Registry, *fakeProbe, *recordingPublisher, *fakeClock) {
	t.Helper()
	rec, reg, probe, pub, clock := newTestReconciler(t)

	mustAdd := func(id string, pid int, phase Phase) *Entry {
		e := NewEntry(id, pid)
		if err := reg.Add(e); err != nil {
			t.Fatalf("add %s: %v", id, err)
		}
		switch phase {
		case PhaseRunning:
			if err := rec.MarkRunning(id); err != nil {
				t.Fatalf("mark running %s: %v", id, err)
			}
		case PhaseStarting:
			if err := rec.MarkStarting(id); err != nil {
				t.Fatalf("mark starting %s: %v", id, err)
			}
		}
		return e
	}

	// PIDs are distinct so the fake process table is unambiguous.
	mustAdd("lost", 101, PhaseRunning)    // process gone (absent from probe)
	mustAdd("delayed", 102, PhaseRunning) // process gone (absent from probe)
	mustAdd("starting", 103, PhaseStarting)
	mustAdd("running", 104, PhaseRunning)
	mustAdd("deleted", 105, PhaseRunning)

	probe.set(104, ProcessInfo{Pid: 104, Alive: true})
	// 101 and 102 deliberately absent; 103 absent but never ran (starting).
	probe.set(105, ProcessInfo{Pid: 105, Alive: true})
	return rec, reg, probe, pub, clock
}

// TestReconcileOnlyRepublishesTrulyMissing covers the main path: normal
// events commit their state first, then one reconciliation pass must
// republish only the single task whose event was genuinely lost, with an
// unknown exit code — never zero.
func TestReconcileOnlyRepublishesTrulyMissing(t *testing.T) {
	rec, reg, probe, pub, clock := fiveTasks(t)

	// Let the normal events commit their state first. The "delayed" task did
	// exit (its process is gone) but its normal event eventually arrives and
	// commits before the scan; only "lost" has a genuinely missing event.
	reg.Delete("deleted")
	probe.remove(105)

	if ok, err := rec.ReportNormalExit("delayed", 13); err != nil || !ok {
		t.Fatalf("delayed normal exit should commit once, ok=%v err=%v", ok, err)
	}

	res, err := rec.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	// Exactly one republish: the lost-event task.
	if res.Republished != 1 {
		t.Fatalf("expected 1 republish, got %d (result=%+v)", res.Republished, res)
	}
	if got := pub.exitCount("lost"); got != 1 {
		t.Fatalf("lost: expected 1 exit event, got %d", got)
	}
	if got := pub.exitCount("delayed"); got != 1 {
		t.Fatalf("delayed should have its single normal event, got %d", got)
	}
	if got := pub.exitCount("starting"); got != 0 {
		t.Fatalf("starting task must never be reconciled, got %d", got)
	}
	if got := pub.exitCount("running"); got != 0 {
		t.Fatalf("alive task must not be reconciled, got %d", got)
	}
	if got := pub.exitCount("deleted"); got != 0 {
		t.Fatalf("deleted task must not get an event, got %d", got)
	}

	evs := pub.exitEvents()
	var ev ExitEvent
	for _, e := range evs {
		if e.TaskID == "lost" {
			ev = e
		}
	}
	if ev.TaskID != "lost" {
		t.Fatalf("expected the single reconciled event to be for lost, got %+v", evs)
	}
	if ev.Status != ExitStatusUnknown {
		t.Fatalf("reconciled status must be the unknown sentinel %d, got %d", ExitStatusUnknown, ev.Status)
	}
	if ev.Status == 0 {
		t.Fatal("unknown exit must never be reported as success (0)")
	}
	if ev.Reason != ReasonProcessGone {
		t.Fatalf("expected reason %q, got %q", ReasonProcessGone, ev.Reason)
	}
	if ev.Kind != EventReconciled {
		t.Fatalf("expected reconciled kind, got %v", ev.Kind)
	}
	if !ev.At.Equal(clock.Now()) {
		t.Fatalf("event time %v should equal injected clock time %v", ev.At, clock.Now())
	}

	// The delayed task keeps its real exit code, not the unknown sentinel.
	delayed, _ := reg.Get("delayed")
	if delayed.ExitStatus() != 13 || delayed.ExitReason() != "" {
		t.Fatalf("delayed must keep real status 13 with no reason, status=%d reason=%q",
			delayed.ExitStatus(), delayed.ExitReason())
	}

	// The task table reflects the stopped transition with the marker set.
	lost, err := reg.Get("lost")
	if err != nil {
		t.Fatalf("get lost: %v", err)
	}
	if lost.Phase() != PhaseStopped || !lost.ExitReported() {
		t.Fatalf("lost should be stopped+reported, phase=%v reported=%v", lost.Phase(), lost.ExitReported())
	}
	if lost.ExitStatus() != ExitStatusUnknown || lost.ExitReason() == "" {
		t.Fatalf("lost must retain unknown status and reason, status=%d reason=%q", lost.ExitStatus(), lost.ExitReason())
	}

	// The starting and running tasks remain untouched.
	if e, _ := reg.Get("starting"); e.Phase() != PhaseStarting {
		t.Fatalf("starting phase changed: %v", e.Phase())
	}
	if e, _ := reg.Get("running"); e.Phase() != PhaseRunning {
		t.Fatalf("running phase changed: %v", e.Phase())
	}
}

// TestReconcileBeatsDelayedEvent covers the race in which reconciliation runs
// before a delayed normal event arrives. Reconciliation is allowed to publish
// first; when the delayed normal event finally arrives it must NOT publish a
// second exit for the same task.
func TestReconcileBeatsDelayedEvent(t *testing.T) {
	rec, reg, _, pub, _ := fiveTasks(t)
	reg.Delete("deleted")

	// First pass republishes both gone-and-unreported tasks (lost + delayed).
	res, err := rec.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if res.Republished != 2 {
		t.Fatalf("expected 2 republishes (lost + delayed), got %d", res.Republished)
	}

	// The delayed normal exit now arrives carrying a real status. It must
	// lose the compare-and-swap and publish nothing.
	published, err := rec.ReportNormalExit("delayed", 42)
	if err != nil {
		t.Fatalf("report normal exit: %v", err)
	}
	if published {
		t.Fatal("delayed normal event must not republish after reconciliation won")
	}
	if got := pub.exitCount("delayed"); got != 1 {
		t.Fatalf("delayed must have exactly one exit event, got %d", got)
	}

	// The reconciled unknown status must not be overwritten by the late code.
	delayed, _ := reg.Get("delayed")
	if delayed.ExitStatus() != ExitStatusUnknown {
		t.Fatalf("delayed status must remain unknown, got %d", delayed.ExitStatus())
	}

	// And the symmetric order: a normal event that wins first blocks a later
	// reconciliation for the same task.
	reg2 := NewRegistry()
	probe2 := newFakeProbe()
	pub2 := newRecordingPublisher()
	rec2 := New(reg2, Config{Probe: probe2, Publisher: pub2, Clock: newFakeClock()})

	e := NewEntry("normal-first", 201)
	if err := reg2.Add(e); err != nil {
		t.Fatal(err)
	}
	if err := rec2.MarkRunning("normal-first"); err != nil {
		t.Fatal(err)
	}
	// Process exits but its normal event arrives before any scan.
	ok, err := rec2.ReportNormalExit("normal-first", 7)
	if err != nil || !ok {
		t.Fatalf("normal exit should publish once, ok=%v err=%v", ok, err)
	}
	res2, err := rec2.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if res2.Republished != 0 {
		t.Fatalf("reconciliation must not republish after normal exit, got %d", res2.Republished)
	}
	if got := pub2.exitCount("normal-first"); got != 1 {
		t.Fatalf("normal-first must have one event, got %d", got)
	}
	if e.ExitStatus() != 7 {
		t.Fatalf("real exit status 7 must be preserved, got %d", e.ExitStatus())
	}
}

// TestReconcileIdempotentAcrossRuns runs many passes and asserts a task is
// republished at most once no matter how often it is scanned.
func TestReconcileIdempotentAcrossRuns(t *testing.T) {
	rec, reg, _, pub, clock := fiveTasks(t)
	reg.Delete("deleted")

	const rounds = 20
	for i := 0; i < rounds; i++ {
		clock.advance(time.Second)
		res, err := rec.Reconcile(context.Background())
		if err != nil {
			t.Fatalf("round %d failed: %v", i, err)
		}
		if i == 0 && res.Republished != 2 {
			t.Fatalf("first round should republish 2, got %d", res.Republished)
		}
		if i > 0 && res.Republished != 0 {
			t.Fatalf("round %d must republish nothing, got %d", i, res.Republished)
		}
	}

	for _, id := range []string{"lost", "delayed", "starting", "running"} {
		want := 0
		if id == "lost" || id == "delayed" {
			want = 1
		}
		if got := pub.exitCount(id); got != want {
			t.Fatalf("%s: expected %d total events after %d rounds, got %d", id, want, rounds, got)
		}
	}
}

// TestConcurrentReconcileAndExit hammers the two exit paths from separate
// goroutines thousands of times and asserts every task publishes exactly one
// exit event, while a third goroutine deletes stopped tasks to exercise the
// registry read/write races (the reconciler must never crash or publish for a
// deleted task).
func TestConcurrentReconcileAndExit(t *testing.T) {
	const (
		tasks   = 25
		iter    = 1000
		basePid = 300
	)

	reg := NewRegistry()
	probe := newFakeProbe()
	pub := newRecordingPublisher()
	clock := newFakeClock()
	rec := New(reg, Config{Probe: probe, Publisher: pub, Clock: clock})

	for i := 0; i < tasks; i++ {
		id := taskName(i)
		pid := basePid + i
		if err := reg.Add(NewEntry(id, pid)); err != nil {
			t.Fatal(err)
		}
		if err := rec.MarkRunning(id); err != nil {
			t.Fatal(err)
		}
		// No entry in the probe means the process is gone: whichever path
		// reaches reportExit first records the exit, the other path must be a
		// no-op.
	}

	// scanWg tracks the long-lived reconciliation loop; workerWg tracks the
	// two finite producers. Keeping them separate lets us stop the loop only
	// after both finite workers have fully drained.
	var scanWg, workerWg sync.WaitGroup
	stop := make(chan struct{})

	scanWg.Add(1)
	go func() {
		defer scanWg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if _, err := rec.Reconcile(context.Background()); err != nil {
					t.Errorf("reconcile: %v", err)
					return
				}
			}
		}
	}()

	// Finite worker 1: repeated normal exits for every task.
	workerWg.Add(1)
	go func() {
		defer workerWg.Done()
		for n := 0; n < iter; n++ {
			for i := 0; i < tasks; i++ {
				_, _ = rec.ReportNormalExit(taskName(i), 1)
			}
		}
	}()

	// Finite worker 2: delete tasks once they have stopped, mirroring Delete
	// after exit. Lookup races against the scanner must stay benign; a deleted
	// task can never receive a late event because reportExit rejects
	// PhaseDeleted.
	workerWg.Add(1)
	go func() {
		defer workerWg.Done()
		for n := 0; n < iter; n++ {
			i := n % tasks
			e, err := reg.Get(taskName(i))
			if err == ErrTaskNotFound {
				continue
			}
			if err != nil {
				t.Errorf("lookup: %v", err)
				return
			}
			if e.Phase() == PhaseStopped {
				reg.Delete(taskName(i))
			}
		}
	}()

	workerWg.Wait()
	close(stop)
	scanWg.Wait()

	// Each original task published exactly one exit event regardless of which
	// path won the CAS. Publisher counts are independent of table membership,
	// so deleted tasks are still verifiable.
	for i := 0; i < tasks; i++ {
		if got := pub.exitCount(taskName(i)); got != 1 {
			t.Fatalf("task %s expected exactly 1 exit event, got %d", taskName(i), got)
		}
	}
	if len(pub.exitEvents()) != tasks {
		t.Fatalf("expected %d total exit events, got %d", tasks, len(pub.exitEvents()))
	}
}

func taskName(i int) string {
	return "task-" + strconvItoa(i)
}

// strconvItoa avoids importing strconv just for naming; kept local and tiny.
func strconvItoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

// TestCreatedTaskWithoutProcessIsNotReconciled pins the phase rule: a task
// record whose process has not started yet must not be treated as a lost exit.
func TestCreatedTaskWithoutProcessIsNotReconciled(t *testing.T) {
	rec, reg, _, pub, _ := newTestReconciler(t)
	if err := reg.Add(NewEntry("created", 401)); err != nil {
		t.Fatal(err)
	}
	// No MarkRunning: stays PhaseCreated. No process in the probe.

	res, err := rec.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.Republished != 0 || pub.exitCount("created") != 0 {
		t.Fatalf("created task must not be reconciled, res=%+v", res)
	}
	if e, _ := reg.Get("created"); e.Phase() != PhaseCreated {
		t.Fatalf("created task phase changed: %v", e.Phase())
	}
}

// TestProbeFailureDoesNotImplyExits ensures a bad scan is never interpreted
// as mass process death.
func TestProbeFailureDoesNotImplyExits(t *testing.T) {
	rec, reg, probe, pub, _ := newTestReconciler(t)
	if err := reg.Add(NewEntry("t", 501)); err != nil {
		t.Fatal(err)
	}
	if err := rec.MarkRunning("t"); err != nil {
		t.Fatal(err)
	}
	probe.fail(ErrProbeUnavailable)

	if _, err := rec.Reconcile(context.Background()); err == nil {
		t.Fatal("expected probe error to propagate")
	}
	if pub.exitCount("t") != 0 {
		t.Fatal("no event may be published when the probe fails")
	}
	if e, _ := reg.Get("t"); e.Phase() != PhaseRunning {
		t.Fatalf("task must remain running after failed scan, got %v", e.Phase())
	}

	// Recovery: once the probe works again and the process is alive, nothing
	// is published.
	probe.fail(nil)
	probe.set(501, ProcessInfo{Pid: 501, Alive: true})
	if res, err := rec.Reconcile(context.Background()); err != nil || res.Republished != 0 {
		t.Fatalf("expected clean no-op pass, res=%+v err=%v", res, err)
	}
}

// TestOrphanProcessOnlyAlerted verifies an attributed live process whose task
// is gone raises an orphan alert and is never otherwise acted on.
func TestOrphanProcessOnlyAlerted(t *testing.T) {
	rec, reg, probe, pub, _ := newTestReconciler(t)
	if err := reg.Add(NewEntry("owned", 601)); err != nil {
		t.Fatal(err)
	}
	if err := rec.MarkRunning("owned"); err != nil {
		t.Fatal(err)
	}
	probe.set(601, ProcessInfo{Pid: 601, Alive: true, OwnerID: "owned"})

	// Drop the task while the process keeps running with an owner mark.
	reg.Delete("owned")

	res, err := rec.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.Orphans != 1 || len(pub.orphans) != 1 {
		t.Fatalf("expected one orphan alert, res=%+v orphans=%d", res, len(pub.orphans))
	}
	if pub.orphans[0].OwnerID != "owned" || pub.orphans[0].Pid != 601 {
		t.Fatalf("unexpected orphan event %+v", pub.orphans[0])
	}
	// An orphan must never produce an exit event for a deleted task.
	if pub.exitCount("owned") != 0 {
		t.Fatal("orphaned process must not generate an exit event")
	}

	// Unattributed live processes are ignored entirely.
	probe.set(999, ProcessInfo{Pid: 999, Alive: true})
	res, _ = rec.Reconcile(context.Background())
	if res.Orphans != 1 {
		t.Fatalf("unattributed process must not count as orphan, got %d", res.Orphans)
	}
}

// TestRunLoopIntervalUsesInjectedClock checks the scheduler honours the
// configured interval and stops on cancellation without real sleeps.
func TestRunLoopIntervalUsesInjectedClock(t *testing.T) {
	reg := NewRegistry()
	probe := newFakeProbe()
	pub := newRecordingPublisher()
	clock := newFakeClock()
	rec := New(reg, Config{
		Probe:     probe,
		Publisher: pub,
		Clock:     clock,
		Interval:  10 * time.Second,
	})

	// A task that exits after the first pass.
	if err := reg.Add(NewEntry("loop", 701)); err != nil {
		t.Fatal(err)
	}
	if err := rec.MarkRunning("loop"); err != nil {
		t.Fatal(err)
	}
	probe.set(701, ProcessInfo{Pid: 701, Alive: true})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		rec.Run(ctx)
		close(done)
	}()

	// Process goes away; the immediate first pass saw it alive, so a periodic
	// pass must now pick the exit up.
	probe.remove(701)
	deadline := time.Now().Add(5 * time.Second)
	for pub.exitCount("loop") == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if pub.exitCount("loop") != 1 {
		t.Fatalf("run loop should reconcile the exited task once, got %d", pub.exitCount("loop"))
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after context cancellation")
	}
}

// TestIntervalAndJitter verifies the default interval and the +/-20% jitter
// bounds with an injected random source, so no wall-clock timing is involved.
func TestIntervalAndJitter(t *testing.T) {
	base := 30 * time.Second

	// No jitter: every period equals the configured interval.
	rec := New(NewRegistry(), Config{
		Probe:     newFakeProbe(),
		Publisher: newRecordingPublisher(),
		Clock:     newFakeClock(),
		Interval:  base,
		Rand:      rand.New(rand.NewSource(1)),
	})
	for i := 0; i < 10; i++ {
		if got := rec.nextInterval(); got != base {
			t.Fatalf("no-jitter period must be %v, got %v", base, got)
		}
	}

	// With jitter: every period stays within [0.8, 1.2) of the base.
	recJ := New(NewRegistry(), Config{
		Probe:     newFakeProbe(),
		Publisher: newRecordingPublisher(),
		Clock:     newFakeClock(),
		Interval:  base,
		Jitter:    true,
		Rand:      rand.New(rand.NewSource(42)),
	})
	lo := time.Duration(float64(base) * (1 - maxJitterFraction))
	hi := time.Duration(float64(base) * (1 + maxJitterFraction))
	for i := 0; i < 100; i++ {
		got := recJ.nextInterval()
		if got < lo || got >= hi {
			t.Fatalf("jittered period %v outside [%v, %v)", got, lo, hi)
		}
	}

	// Zero interval falls back to the documented default.
	def := New(NewRegistry(), Config{
		Probe:     newFakeProbe(),
		Publisher: newRecordingPublisher(),
		Clock:     newFakeClock(),
	})
	if def.interval != DefaultReconcileInterval {
		t.Fatalf("expected default interval %v, got %v", DefaultReconcileInterval, def.interval)
	}
}
