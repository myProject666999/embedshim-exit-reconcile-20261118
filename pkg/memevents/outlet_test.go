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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func v2Fields() FieldSet { return FieldSet{OOM: true, OOMKill: true} }

// --- Scenario: the memory event arrives first -----------------------------

func TestScenarioMemoryFirst(t *testing.T) {
	src := newFakeSource(Counters{}, v2Fields())
	pub := &recordingPublisher{}
	o := newTestOutlet(t, src, pub)

	if mode := o.Attach(); mode != ModeFull {
		t.Fatalf("attach mode = %v, want full", mode)
	}
	waitWatch(t, src)

	// The counter increases and the notification is delivered before the
	// exit is observed.
	src.Set(Counters{OOM: 1, OOMKill: 1})
	src.watch.Notify()
	mustWaitFor(t, func() bool { return len(pub.Events()) == 1 }, "async OOM event")

	o.OnExit(ExitInfo{Pid: 42, Status: 137})
	o.OnDelete(DeleteInfo{Pid: 42, Status: 137})

	events := pub.Events()
	assertKinds(t, pub.Kinds(), KindOOM, KindExit, KindDelete)
	assertCausal(t, events)
	if events[0].Deltas != (Counters{OOM: 1, OOMKill: 1}) {
		t.Fatalf("oom deltas = %+v", events[0].Deltas)
	}
	if events[0].Merged != 1 {
		t.Fatalf("merged = %d, want 1", events[0].Merged)
	}
}

// --- Scenario: exit first, but the synchronous read sees the growth ------

func TestScenarioExitFirstButCountersGrew(t *testing.T) {
	src := newFakeSource(Counters{}, v2Fields())
	pub := &recordingPublisher{}
	o := newTestOutlet(t, src, pub)
	o.Attach()
	waitWatch(t, src)

	// The process is killed and reaped before the inotify notification is
	// delivered; the counters already contain the kill.
	src.Set(Counters{OOM: 1, OOMKill: 1})
	o.OnExit(ExitInfo{Pid: 42, Status: 137})

	events := pub.Events()
	assertKinds(t, pub.Kinds(), KindOOM, KindExit)
	assertCausal(t, events)

	// The late notification must not produce a second OOM event. The outlet
	// closed its watch when it published the exit, so drive the handler
	// directly the way a record already queued in inotify would.
	o.handleNote(Note{Changed: true})
	waitForNoGrowth(t, pub, 2)

	o.OnDelete(DeleteInfo{Pid: 42, Status: 137})
	assertKinds(t, pub.Kinds(), KindOOM, KindExit, KindDelete)
	assertCausal(t, pub.Events())
}

// --- Scenario: exit first and the cgroup is already removed --------------

func TestScenarioExitAfterCgroupRemoved(t *testing.T) {
	src := newFakeSource(Counters{}, v2Fields())
	// From now on every read reports the source gone, as after an early
	// cgroup cleanup.
	src.SetSampleFn(func() (Sample, error) { return Sample{}, ErrSourceGone })
	pub := &recordingPublisher{}
	o := newTestOutlet(t, src, pub)

	if mode := o.Attach(); mode != ModeDisabled {
		t.Fatalf("attach mode = %v, want disabled", mode)
	}
	if d := o.Degraded(); d == nil || d.Reason != DegradedSourceGone {
		t.Fatalf("degraded = %+v", d)
	}

	o.OnExit(ExitInfo{Pid: 42, Status: 137})
	o.OnDelete(DeleteInfo{Pid: 42, Status: 137})

	assertKinds(t, pub.Kinds(), KindExit, KindDelete)
	assertCausal(t, pub.Events())
	ev := pub.Events()[0]
	if !strings.Contains(ev.Reason, "unavailable") {
		t.Fatalf("exit reason = %q, want it to explain the missing read", ev.Reason)
	}
}

// TestScenarioExitAfterWatchReportedGone covers the case where the shim is
// attached (watch works) and the cgroup is removed between the watch start
// and the exit.
func TestScenarioExitAfterWatchReportedGone(t *testing.T) {
	src := newFakeSource(Counters{}, v2Fields())
	pub := &recordingPublisher{}
	o := newTestOutlet(t, src, pub)
	o.Attach()
	waitWatch(t, src)

	src.watch.Gone()
	mustWaitFor(t, func() bool { return o.State().SourceGone }, "source gone state")
	src.SetSampleFn(func() (Sample, error) { return Sample{}, ErrSourceGone })

	o.OnExit(ExitInfo{Pid: 42, Status: 137})
	o.OnDelete(DeleteInfo{Pid: 42, Status: 137})

	assertKinds(t, pub.Kinds(), KindExit, KindDelete)
	if reason := pub.Events()[0].Reason; !strings.Contains(reason, "already removed") {
		t.Fatalf("exit reason = %q", reason)
	}
}

// --- Scenario: a memory event arrives after delete ------------------------

func TestScenarioMemoryAfterDelete(t *testing.T) {
	src := newFakeSource(Counters{}, v2Fields())
	pub := &recordingPublisher{}
	o := newTestOutlet(t, src, pub)
	o.Attach()
	waitWatch(t, src)

	o.OnExit(ExitInfo{Pid: 42, Status: 137})
	o.OnDelete(DeleteInfo{Pid: 42, Status: 137})

	// A notification that races the delete: the outlet is closed and the
	// notification is only logged.
	src.Set(Counters{OOM: 3, OOMKill: 3})
	// The fake watch was closed by the outlet, so drive the handler the way
	// a late inotify record would arrive.
	o.handleNote(Note{Changed: true})

	assertKinds(t, pub.Kinds(), KindExit, KindDelete)
	assertCausal(t, pub.Events())
	if _, ok := lastEvent(pub); !ok {
		t.Fatal("no events")
	}
}

// --- Scenario: three increases in quick succession ------------------------

func TestScenarioThreeIncreasesIndividual(t *testing.T) {
	src := newFakeSource(Counters{}, v2Fields())
	pub := &recordingPublisher{}
	o := newTestOutlet(t, src, pub)
	o.Attach()
	waitWatch(t, src)

	for i := uint64(1); i <= 3; i++ {
		src.Set(Counters{OOM: i, OOMKill: i})
		src.watch.Notify()
		mustWaitFor(t, func() bool { return len(pub.Events()) == int(i) }, "individual OOM event")
	}
	o.OnExit(ExitInfo{Pid: 42, Status: 137})

	assertKinds(t, pub.Kinds(), KindOOM, KindOOM, KindOOM, KindExit)
	for i, ev := range pub.Events()[:3] {
		if ev.Merged != 1 {
			t.Fatalf("event %d merged = %d, want 1", i, ev.Merged)
		}
		if ev.Deltas != (Counters{OOM: 1, OOMKill: 1}) {
			t.Fatalf("event %d deltas = %+v", i, ev.Deltas)
		}
	}
}

func TestScenarioThreeIncreasesMerged(t *testing.T) {
	src := newFakeSource(Counters{}, v2Fields())
	pub := &recordingPublisher{}
	timers := &fakeTimer{}
	o := newTestOutlet(t, src, pub, func(c *Config) {
		c.MergeWindow = 100 * 1e6 // 100ms; only the injected timer fires it
		c.AfterFunc = timers.AfterFunc
	})
	o.Attach()
	waitWatch(t, src)

	for i := uint64(1); i <= 3; i++ {
		src.Set(Counters{OOM: i, OOMKill: i})
		src.watch.Notify()
		// Wait until the increase has been folded into the pending event,
		// i.e. the watcher has consumed the note.
		mustWaitFor(t, func() bool { return o.State().PendingReads == int(i) }, "pending read")
	}

	if got := len(pub.Events()); got != 0 {
		t.Fatalf("events before flush = %d, want 0", got)
	}
	timers.Fire()

	events := pub.Events()
	assertKinds(t, pub.Kinds(), KindOOM)
	if events[0].Merged != 3 {
		t.Fatalf("merged = %d, want 3", events[0].Merged)
	}
	if events[0].Deltas != (Counters{OOM: 3, OOMKill: 3}) {
		t.Fatalf("deltas = %+v", events[0].Deltas)
	}

	o.OnExit(ExitInfo{Pid: 42, Status: 137})
	assertKinds(t, pub.Kinds(), KindOOM, KindExit)
}

// TestMergedPendingFlushedBeforeExit checks that an unexpired merge window is
// flushed by the exit path, so nothing is published after the exit.
func TestMergedPendingFlushedBeforeExit(t *testing.T) {
	src := newFakeSource(Counters{}, v2Fields())
	pub := &recordingPublisher{}
	timers := &fakeTimer{}
	o := newTestOutlet(t, src, pub, func(c *Config) {
		c.MergeWindow = 100 * 1e6
		c.AfterFunc = timers.AfterFunc
	})
	o.Attach()
	waitWatch(t, src)

	src.Set(Counters{OOM: 1, OOMKill: 1})
	src.watch.Notify()
	mustWaitFor(t, func() bool { return o.State().PendingReads == 1 }, "pending read")

	o.OnExit(ExitInfo{Pid: 42, Status: 137})
	assertKinds(t, pub.Kinds(), KindOOM, KindExit)
	// Firing the stale timer later must not publish another OOM event.
	timers.Fire()
	assertKinds(t, pub.Kinds(), KindOOM, KindExit)
}

// --- Edge: cgroup-deleted notification is not a memory event -------------

func TestGoneNotificationIsNotAMemoryEvent(t *testing.T) {
	src := newFakeSource(Counters{OOM: 1, OOMKill: 1}, v2Fields())
	pub := &recordingPublisher{}
	o := newTestOutlet(t, src, pub)
	o.Attach()
	waitWatch(t, src)

	src.watch.Gone()
	mustWaitFor(t, func() bool { return o.State().SourceGone }, "source gone")
	// Wait until the watch loop noticed the channel close too.
	waitForNoGrowth(t, pub, 0)

	if got := len(pub.Events()); got != 0 {
		t.Fatalf("gone notification produced %d events: %v", got, kindNamesOf(pub.Events()))
	}
}

// A task reopened after a restart with its exit already reported must not
// publish a second exit, but must still publish the delete.
func TestExitReportedOutletPublishesOnlyDelete(t *testing.T) {
	src := newFakeSource(Counters{OOM: 1, OOMKill: 1}, v2Fields())
	pub := &recordingPublisher{}
	o := newTestOutlet(t, src, pub, func(c *Config) { c.ExitReported = true })
	o.Attach()
	waitWatch(t, src)

	o.OnExit(ExitInfo{Pid: 1, Status: 0})
	src.watch.Notify()
	waitForNoGrowth(t, pub, 0)

	o.OnDelete(DeleteInfo{Pid: 1, Status: 0})
	assertKinds(t, pub.Kinds(), KindDelete)
	assertCausal(t, pub.Events())
}

// Increases first observed at delete time after the exit was published must
// be folded into the baseline only: they cannot be ordered before an exit
// that is already out.
func TestGrowthAfterExitNotPublished(t *testing.T) {
	src := newFakeSource(Counters{}, v2Fields())
	pub := &recordingPublisher{}
	o := newTestOutlet(t, src, pub)
	o.Attach()
	waitWatch(t, src)

	o.OnExit(ExitInfo{Pid: 1, Status: 1})
	src.Set(Counters{OOM: 1, OOMKill: 1})

	o.PrepareDelete()
	o.OnDelete(DeleteInfo{Pid: 1, Status: 1})

	assertKinds(t, pub.Kinds(), KindExit, KindDelete)
	if b := o.Baseline().Counters; b != (Counters{OOM: 1, OOMKill: 1}) {
		t.Fatalf("baseline = %+v, want the delete-time reading", b)
	}
}

// --- Edge: pre-existing counters are not reported -----------------------

func TestPreExistingCountersNotReported(t *testing.T) {
	src := newFakeSource(Counters{OOM: 7, OOMKill: 3}, v2Fields())
	pub := &recordingPublisher{}
	o := newTestOutlet(t, src, pub)
	o.Attach()
	waitWatch(t, src)

	src.watch.Notify()
	waitForNoGrowth(t, pub, 0)

	o.OnExit(ExitInfo{Pid: 1, Status: 0})
	o.OnDelete(DeleteInfo{Pid: 1, Status: 0})
	assertKinds(t, pub.Kinds(), KindExit, KindDelete)
}

// --- Edge: restart with a persisted baseline ----------------------------

func TestRestartGrowthIsNotReported(t *testing.T) {
	dir := t.TempDir()
	store := NewBaselineStore(filepath.Join(dir, "baseline.json"))
	if err := store.Save(Baseline{
		Counters:   Counters{OOM: 1, OOMKill: 1},
		Fields:     v2Fields(),
		Generation: GenerationV2,
	}); err != nil {
		t.Fatal(err)
	}

	src := newFakeSource(Counters{OOM: 3, OOMKill: 2}, v2Fields())
	pub := &recordingPublisher{}
	o := newTestOutlet(t, src, pub, func(c *Config) { c.Store = store })
	o.Attach()
	waitWatch(t, src)

	rec := o.Recovery()
	if !rec.HadBaseline || rec.Prev != (Counters{OOM: 1, OOMKill: 1}) {
		t.Fatalf("recovery = %+v", rec)
	}
	if rec.Growth != (Counters{OOM: 2, OOMKill: 1}) {
		t.Fatalf("recovery growth = %+v", rec.Growth)
	}
	if rec.Reset {
		t.Fatal("unexpected reset")
	}
	if got := len(pub.Events()); got != 0 {
		t.Fatalf("restart published %d events: %v", got, kindNamesOf(pub.Events()))
	}
	if got := o.Baseline().Counters; got != (Counters{OOM: 3, OOMKill: 2}) {
		t.Fatalf("baseline = %+v, want the current reading", got)
	}
}

func TestRestartCounterResetIsDetected(t *testing.T) {
	dir := t.TempDir()
	store := NewBaselineStore(filepath.Join(dir, "baseline.json"))
	if err := store.Save(Baseline{
		Counters:   Counters{OOM: 5, OOMKill: 5},
		Fields:     v2Fields(),
		Generation: GenerationV2,
	}); err != nil {
		t.Fatal(err)
	}

	// The host rebooted: the cgroup was recreated and the counters restarted
	// from zero, then grew a little.
	src := newFakeSource(Counters{OOM: 1, OOMKill: 0}, v2Fields())
	pub := &recordingPublisher{}
	o := newTestOutlet(t, src, pub, func(c *Config) { c.Store = store })
	o.Attach()

	if !o.Recovery().Reset {
		t.Fatalf("recovery = %+v, want reset", o.Recovery())
	}
	if got := len(pub.Events()); got != 0 {
		t.Fatalf("reset produced %d events: %v", got, kindNamesOf(pub.Events()))
	}
	if got := o.Baseline().Counters; got != (Counters{OOM: 1}) {
		t.Fatalf("baseline = %+v", got)
	}
}

func TestBaselineSavedAfterEvent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "baseline.json")
	store := NewBaselineStore(path)

	src := newFakeSource(Counters{}, v2Fields())
	pub := &recordingPublisher{}
	o := newTestOutlet(t, src, pub, func(c *Config) { c.Store = store })
	o.Attach()
	waitWatch(t, src)

	src.Set(Counters{OOM: 2, OOMKill: 2})
	src.watch.Notify()
	mustWaitFor(t, func() bool { return len(pub.Events()) == 1 }, "OOM event")

	saved, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if saved == nil || saved.Counters != (Counters{OOM: 2, OOMKill: 2}) {
		t.Fatalf("saved baseline = %+v", saved)
	}
	if saved.Fields != v2Fields() {
		t.Fatalf("saved fields = %+v", saved.Fields)
	}
	if saved.Generation != GenerationV2 {
		t.Fatalf("saved generation = %v", saved.Generation)
	}
}

func TestCorruptBaselineIsTreatedAsAbsent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "baseline.json")
	if err := os.WriteFile(path, []byte("{not json"), 0600); err != nil {
		t.Fatal(err)
	}

	src := newFakeSource(Counters{OOM: 1, OOMKill: 1}, v2Fields())
	pub := &recordingPublisher{}
	o := newTestOutlet(t, src, pub, func(c *Config) { c.Store = NewBaselineStore(path) })
	o.Attach()

	if got := len(pub.Events()); got != 0 {
		t.Fatalf("corrupt baseline produced %d events", got)
	}
	if o.Mode() != ModeFull {
		t.Fatalf("mode = %v, want full (a corrupt baseline must not degrade)", o.Mode())
	}
}

// --- Degradation --------------------------------------------------------

func TestAttachDegradeOnMissingFile(t *testing.T) {
	src := newFakeSource(Counters{}, v2Fields())
	src.SetSampleFn(func() (Sample, error) {
		return Sample{}, ErrSourceGone
	})
	pub := &recordingPublisher{}
	o := newTestOutlet(t, src, pub)

	if mode := o.Attach(); mode != ModeDisabled {
		t.Fatalf("mode = %v", mode)
	}
	d := o.Degraded()
	if d == nil || d.Reason != DegradedSourceGone {
		t.Fatalf("degraded = %+v", d)
	}
	if !strings.HasPrefix(d.Marker(), "memory-events-degraded:source-gone") {
		t.Fatalf("marker = %q", d.Marker())
	}
}

func TestAttachDegradeOnPermission(t *testing.T) {
	src := newFakeSource(Counters{}, v2Fields())
	src.SetSampleFn(func() (Sample, error) {
		return Sample{}, &os.PathError{Op: "open", Path: "/x", Err: os.ErrPermission}
	})
	pub := &recordingPublisher{}
	o := newTestOutlet(t, src, pub)
	o.Attach()

	d := o.Degraded()
	if d == nil || d.Reason != DegradedPermission {
		t.Fatalf("degraded = %+v", d)
	}
}

func TestAttachDegradeOnUnsupportedSource(t *testing.T) {
	src := newFakeSource(Sample{}.Counters, FieldSet{})
	src.SetSampleFn(func() (Sample, error) { return Sample{}, ErrSourceUnsupported })
	pub := &recordingPublisher{}
	o := newTestOutlet(t, src, pub)
	o.Attach()

	d := o.Degraded()
	if d == nil || d.Reason != DegradedUnsupported {
		t.Fatalf("degraded = %+v", d)
	}
}

// A failed watch degrades to exit-only: the synchronous reads still work.
func TestWatchFailureDegradesToExitOnly(t *testing.T) {
	src := newFakeSource(Counters{}, v2Fields())
	src.watchErr = errors.New("inotify watch limit reached")
	pub := &recordingPublisher{}
	o := newTestOutlet(t, src, pub)

	if mode := o.Attach(); mode != ModeExitOnly {
		t.Fatalf("mode = %v, want exit-only", mode)
	}
	d := o.Degraded()
	if d == nil || d.Reason != DegradedWatchFailed {
		t.Fatalf("degraded = %+v", d)
	}

	src.Set(Counters{OOM: 1, OOMKill: 1})
	o.OnExit(ExitInfo{Pid: 3, Status: 137})
	assertKinds(t, pub.Kinds(), KindOOM, KindExit)
	assertCausal(t, pub.Events())
}

func TestDegradedMarkerRoundTrip(t *testing.T) {
	dir := t.TempDir()
	in := &Degraded{Reason: DegradedPermission, Detail: "open /sys/fs/cgroup/memory.events: permission denied", Generation: GenerationV2}
	if err := SaveDegraded(dir, in); err != nil {
		t.Fatal(err)
	}
	out, err := LoadDegraded(dir)
	if err != nil {
		t.Fatal(err)
	}
	if out == nil || out.Reason != in.Reason || out.Detail != in.Detail {
		t.Fatalf("loaded = %+v", out)
	}
	missing, err := LoadDegraded(t.TempDir())
	if err != nil || missing != nil {
		t.Fatalf("missing marker = %+v, %v", missing, err)
	}
}

// --- Counter parsing ----------------------------------------------------

func TestParseV2MemoryEvents(t *testing.T) {
	s, err := parseSample(GenerationV2, strings.NewReader("low 0\nhigh 0\nmax 4\noom 2\noom_kill 3\n"))
	if err != nil {
		t.Fatal(err)
	}
	if s.Counters != (Counters{OOM: 2, OOMKill: 3}) {
		t.Fatalf("counters = %+v", s.Counters)
	}
	if s.Fields != (FieldSet{OOM: true, OOMKill: true}) {
		t.Fatalf("fields = %+v", s.Fields)
	}
}

func TestParseV2MissingFieldIsUnsupported(t *testing.T) {
	s, err := parseSample(GenerationV2, strings.NewReader("low 0\nmax 4\n"))
	if !errors.Is(err, ErrSourceUnsupported) {
		t.Fatalf("err = %v, want ErrSourceUnsupported", err)
	}
	if s.Fields.Supported() {
		t.Fatalf("fields = %+v", s.Fields)
	}
}

func TestParseV2PartialFields(t *testing.T) {
	s, err := parseSample(GenerationV2, strings.NewReader("oom 5\n"))
	if err != nil {
		t.Fatal(err)
	}
	if s.Fields != (FieldSet{OOM: true, OOMKill: false}) {
		t.Fatalf("fields = %+v", s.Fields)
	}
	if s.Counters != (Counters{OOM: 5}) {
		t.Fatalf("counters = %+v", s.Counters)
	}
}

func TestParseV1OOMControl(t *testing.T) {
	s, err := parseSample(GenerationV1, strings.NewReader("oom_kill_disable 0\nunder_oom 0\noom_kill 4\n"))
	if err != nil {
		t.Fatal(err)
	}
	if s.Counters != (Counters{OOMKill: 4}) {
		t.Fatalf("counters = %+v", s.Counters)
	}
	if s.Fields != (FieldSet{OOM: false, OOMKill: true}) {
		t.Fatalf("fields = %+v", s.Fields)
	}
}

// On v1 the trigger count is not exposed. under_oom flipping must not be
// mistaken for a counter increase.
func TestV1UnderOOMIsNotACounter(t *testing.T) {
	s, err := parseSample(GenerationV1, strings.NewReader("under_oom 1\noom_kill 0\n"))
	if err != nil {
		t.Fatal(err)
	}
	if s.Counters.Any() {
		t.Fatalf("counters = %+v, want none", s.Counters)
	}
}

func TestV1WithoutOOMKillIsUnsupported(t *testing.T) {
	_, err := parseSample(GenerationV1, strings.NewReader("oom_kill_disable 0\nunder_oom 0\n"))
	if !errors.Is(err, ErrSourceUnsupported) {
		t.Fatalf("err = %v", err)
	}
}

// A field that disappears mid-run triggers a silent re-baseline instead of a
// bogus delta.
func TestFieldSetChangeRebaselines(t *testing.T) {
	src := newFakeSource(Counters{OOM: 1, OOMKill: 1}, v2Fields())
	pub := &recordingPublisher{}
	o := newTestOutlet(t, src, pub)
	o.Attach()
	waitWatch(t, src)

	// oom disappears; the remaining counter reads as unchanged.
	src.Set(Counters{OOM: 0, OOMKill: 1})
	src.SetFields(FieldSet{OOMKill: true})
	src.watch.Notify()
	waitForNoGrowth(t, pub, 0)

	if b := o.Baseline(); b.Fields != (FieldSet{OOMKill: true}) {
		t.Fatalf("baseline fields = %+v", b.Fields)
	}
}

// A counter the kernel does not expose is dropped even if a broken sample
// carries it.
func TestUnsupportedCounterDropped(t *testing.T) {
	src := newFakeSource(Counters{}, FieldSet{OOMKill: true})
	pub := &recordingPublisher{}
	o := newTestOutlet(t, src, pub)
	o.Attach()
	waitWatch(t, src)

	src.Set(Counters{OOM: 9, OOMKill: 0})
	src.watch.Notify()
	waitForNoGrowth(t, pub, 0)
}

// --- Counter arithmetic -------------------------------------------------

func TestCountersSub(t *testing.T) {
	d, reset := (Counters{OOM: 5, OOMKill: 3}).Sub(Counters{OOM: 2, OOMKill: 3})
	if reset {
		t.Fatal("unexpected reset")
	}
	if d != (Counters{OOM: 3}) {
		t.Fatalf("delta = %+v", d)
	}

	_, reset = (Counters{OOM: 1}).Sub(Counters{OOM: 2})
	if !reset {
		t.Fatal("want reset")
	}
}

// --- Misc ---------------------------------------------------------------

func lastEvent(p *recordingPublisher) (Event, bool) {
	es := p.Events()
	if len(es) == 0 {
		return Event{}, false
	}
	return es[len(es)-1], true
}

func waitWatch(t *testing.T, s *fakeSource) {
	t.Helper()
	mustWaitFor(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.watch != nil
	}, "watch attached")
}

// waitForNoGrowth waits until the event count is stable at n for a short
// interval. It is used to assert that no event is published asynchronously.
func waitForNoGrowth(t *testing.T, p *recordingPublisher, n int) {
	t.Helper()
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		if len(p.Events()) != n {
			t.Fatalf("events = %v, want %d", kindNamesOf(p.Events()), n)
		}
		time.Sleep(2 * time.Millisecond)
	}
	if len(p.Events()) != n {
		t.Fatalf("events = %v, want %d", kindNamesOf(p.Events()), n)
	}
}
