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
	"sync"
	"testing"
	"time"
)

// fakeClock is a manually advanced clock.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Unix(1700000000, 0)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// fakeSource is a Source whose counters are set by the test. A nil watch
// factory means Watch fails with watchErr.
type fakeSource struct {
	mu       sync.Mutex
	sample   Sample
	sampleFn func() (Sample, error)
	sampleN  int

	gen      Generation
	watchErr error

	watch *fakeWatch
	// onWatch lets a test observe when the watch was established.
	onWatch func(*fakeWatch)
}

func newFakeSource(c Counters, f FieldSet) *fakeSource {
	return &fakeSource{
		sample: Sample{Counters: c, Fields: f},
		gen:    GenerationV2,
	}
}

func (s *fakeSource) Generation() Generation { return s.gen }

func (s *fakeSource) Sample() (Sample, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sampleN++
	if s.sampleFn != nil {
		return s.sampleFn()
	}
	return s.sample, nil
}

func (s *fakeSource) SampleCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sampleN
}

func (s *fakeSource) Set(c Counters) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sample.Counters = c
}

func (s *fakeSource) SetFields(f FieldSet) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sample.Fields = f
}

func (s *fakeSource) SetSampleFn(fn func() (Sample, error)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sampleFn = fn
}

func (s *fakeSource) Watch() (Watch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.watchErr != nil {
		return nil, s.watchErr
	}
	w := &fakeWatch{notes: make(chan Note, 16), done: make(chan struct{})}
	s.watch = w
	if s.onWatch != nil {
		s.onWatch(w)
	}
	return w, nil
}

// fakeWatch lets the test drive notifications in any order. Senders and the
// closer are serialized by a mutex: the outlet closes the watch from the exit
// goroutine while the test may be delivering a record, so the fake must not
// close the channel under a concurrent send.
type fakeWatch struct {
	mu       sync.Mutex
	notes    chan Note
	done     chan struct{}
	closed   bool
	stopOnce sync.Once
}

func (w *fakeWatch) C() <-chan Note { return w.notes }

func (w *fakeWatch) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closeLocked()
	return nil
}

func (w *fakeWatch) closeLocked() {
	w.stopOnce.Do(func() {
		w.closed = true
		close(w.done)
		close(w.notes)
	})
}

// Closed reports whether the watch has been closed.
func (w *fakeWatch) Closed() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closed
}

// Notify delivers a modification hint. It reports false when the watch has
// been closed, which lets a racing test goroutine model the kernel dropping a
// record because nobody holds the fd any more.
func (w *fakeWatch) Notify() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return false
	}
	w.notes <- Note{Changed: true}
	return true
}

// Gone delivers the cgroup-deleted notification.
func (w *fakeWatch) Gone() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	w.notes <- Note{Gone: true}
}

// recordingPublisher records every event and verifies that Publish is never
// called concurrently.
type recordingPublisher struct {
	mu       sync.Mutex
	events   []Event
	inFlight bool
	// err, when set, is returned for every call.
	err error
}

func (p *recordingPublisher) Publish(ev Event) error {
	p.mu.Lock()
	if p.inFlight {
		p.mu.Unlock()
		panic("concurrent Publish")
	}
	p.inFlight = true
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		p.inFlight = false
		p.mu.Unlock()
	}()

	p.mu.Lock()
	p.events = append(p.events, ev)
	p.mu.Unlock()
	return p.err
}

func (p *recordingPublisher) Events() []Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]Event, len(p.events))
	copy(out, p.events)
	return out
}

func (p *recordingPublisher) Kinds() []Kind {
	var out []Kind
	for _, ev := range p.Events() {
		out = append(out, ev.Kind)
	}
	return out
}

// fakeTimer records AfterFunc schedules and lets the test fire them.
type fakeTimer struct {
	mu      sync.Mutex
	fns     []func()
	stopped bool
}

func (t *fakeTimer) AfterFunc(d time.Duration, fn func()) *time.Timer {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.fns = append(t.fns, fn)
	// A real *time.Timer is returned so Outlet.Stop has something to call;
	// it is stopped immediately so it never fires on its own.
	tm := time.AfterFunc(time.Hour, func() {})
	tm.Stop()
	return tm
}

func (t *fakeTimer) Fire() int {
	t.mu.Lock()
	fns := t.fns
	t.fns = nil
	t.mu.Unlock()
	for _, fn := range fns {
		fn()
	}
	return len(fns)
}

func (t *fakeTimer) Pending() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.fns)
}

func mustWaitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func newTestOutlet(t *testing.T, src Source, pub Publisher, opts ...func(*Config)) *Outlet {
	t.Helper()
	cfg := Config{
		TaskID:    "task-1",
		Source:    src,
		Publisher: pub,
		Clock:     newFakeClock(),
	}
	for _, o := range opts {
		o(&cfg)
	}
	return New(cfg)
}

func assertKinds(t *testing.T, got []Kind, want ...Kind) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("event kinds = %v, want %v", kindNames(got), kindNames(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("event kinds = %v, want %v", kindNames(got), kindNames(want))
		}
	}
}

func kindNames(kinds []Kind) []string {
	out := make([]string, len(kinds))
	for i, k := range kinds {
		out[i] = k.String()
	}
	return out
}

func assertCausal(t *testing.T, events []Event) {
	t.Helper()
	seenExit := false
	seenDelete := false
	for i, ev := range events {
		switch ev.Kind {
		case KindOOM:
			if seenExit || seenDelete {
				t.Fatalf("event %d: OOM after %s: %v", i, ifElse(seenDelete, "delete", "exit"), kindNamesOf(events))
			}
		case KindExit:
			if seenExit {
				t.Fatalf("duplicate exit event: %v", kindNamesOf(events))
			}
			if seenDelete {
				t.Fatalf("event %d: exit after delete: %v", i, kindNamesOf(events))
			}
			seenExit = true
		case KindDelete:
			if seenDelete {
				t.Fatalf("duplicate delete event: %v", kindNamesOf(events))
			}
			seenDelete = true
		}
	}
	// A delete without an exit is legitimate for a task that was created
	// and removed without ever running; it is not an ordering violation.
}

func kindNamesOf(events []Event) []string {
	kinds := make([]Kind, len(events))
	for i, ev := range events {
		kinds[i] = ev.Kind
	}
	return kindNames(kinds)
}

func ifElse(cond bool, a, b string) string {
	if cond {
		return a
	}
	return b
}

var errBoom = errors.New("boom")
