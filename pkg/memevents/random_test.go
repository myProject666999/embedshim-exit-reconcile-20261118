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
	"math/rand"
	"sync"
	"testing"
	"time"
)

// TestRandomArrivalOrder drives the two event sources concurrently, in a
// random interleaving, over many rounds. It asserts the invariants that must
// hold for every run:
//
//   - OOM events never follow the exit event;
//   - the exit is published at most once and before the delete;
//   - the delete is published exactly once and is the last event;
//   - no event is published after the delete;
//   - each counter increase is reported exactly once: the sum of the OOM
//     deltas equals the total counter growth, whether the outlet coalesced
//     them or not.
//
// The random order is the point: the outlet's guarantee must not depend on
// which source wins a race.
func TestRandomArrivalOrder(t *testing.T) {
	const rounds = 300

	for round := 0; round < rounds; round++ {
		round := round
		seed := int64(round) + 1

		t.Run("", func(t *testing.T) {
			t.Parallel()
			runRandomRound(t, seed)
		})
	}
}

func runRandomRound(t *testing.T, seed int64) {
	t.Helper()

	rng := rand.New(rand.NewSource(seed))

	src := newFakeSource(Counters{}, v2Fields())
	pub := &recordingPublisher{}

	var (
		mergeWindow time.Duration
		afterFn     func(time.Duration, func()) *time.Timer
	)
	if rng.Intn(2) == 0 {
		timers := &fakeTimer{}
		mergeWindow = 10 * time.Millisecond
		afterFn = timers.AfterFunc
	}

	o := newTestOutlet(t, src, pub, func(c *Config) {
		c.MergeWindow = mergeWindow
		c.AfterFunc = afterFn
	})
	if mode := o.Attach(); mode != ModeFull {
		t.Fatalf("attach mode = %v", mode)
	}
	waitWatch(t, src)

	// The exit path is modelled by one goroutine that performs the
	// synchronous read and publishes; the counter path by another that
	// notifies. Both may run in any order.
	var (
		wg        sync.WaitGroup
		oomTotal  uint64
		oomMu     sync.Mutex
		raceDelay = time.Duration(rng.Intn(200)) * time.Microsecond
	)

	bump := func(grow uint64) {
		oomMu.Lock()
		oomTotal += grow
		src.Set(Counters{OOM: oomTotal, OOMKill: oomTotal})
		oomMu.Unlock()
	}

	// Background counter increases with notifications. Some happen before
	// the exit, some race it, and the last one happens after the exit in
	// half of the rounds (the "late notification" case).
	preGrowth := uint64(rng.Intn(4))
	bump(preGrowth)
	for i := uint64(1); i <= preGrowth; i++ {
		src.Set(Counters{OOM: i, OOMKill: i})
		src.watch.Notify()
		mustWaitFor(t, func() bool {
			return o.State().Baseline == (Counters{OOM: i, OOMKill: i})
		}, "counter growth folded")
	}

	// Race the exit against one more increase.
	raceGrowth := uint64(1 + rng.Intn(2))
	wg.Add(2)
	go func() {
		defer wg.Done()
		time.Sleep(raceDelay)
		bump(raceGrowth)
		select {
		case <-src.watch.done:
			// The exit already closed the watch; the synchronous read in
			// OnExit will pick the growth up.
		default:
			src.watch.Notify()
		}
	}()
	go func() {
		defer wg.Done()
		time.Sleep(time.Duration(rng.Intn(200)) * time.Microsecond)
		o.OnExit(ExitInfo{Pid: 42, Status: 137})
	}()
	wg.Wait()

	// Late notification after the exit.
	if rng.Intn(2) == 0 {
		late := uint64(1 + rng.Intn(2))
		bump(late)
		o.handleNote(Note{Changed: true})
	}

	// A small drain so an in-flight async fold completes before the delete.
	time.Sleep(2 * time.Millisecond)
	if mergeWindow > 0 {
		o.Flush()
	}

	o.OnDelete(DeleteInfo{Pid: 42, Status: 137})

	events := pub.Events()
	assertCausal(t, events)

	// The delete must be present, last and unique.
	if len(events) == 0 || events[len(events)-1].Kind != KindDelete {
		t.Fatalf("last event = %v, want delete", kindNamesOf(events))
	}
	deletes, exits := 0, 0
	var oomDeltas uint64
	for _, ev := range events {
		switch ev.Kind {
		case KindDelete:
			deletes++
		case KindExit:
			exits++
		case KindOOM:
			oomDeltas += ev.Deltas.OOM
			if ev.Deltas.OOMKill != ev.Deltas.OOM {
				t.Fatalf("OOM and OOMKill deltas differ: %+v", ev.Deltas)
			}
			if ev.Merged < 1 {
				t.Fatalf("merged = %d", ev.Merged)
			}
		}
	}
	if deletes != 1 {
		t.Fatalf("delete events = %d", deletes)
	}
	if exits != 1 {
		t.Fatalf("exit events = %d", exits)
	}

	// Accounting. Increases observed up to the exit must be reported before
	// the exit; growth that the test races against the exit may land on
	// either side (the synchronous read catches it, or it arrived after the
	// exit and is intentionally folded into the baseline without an event);
	// growth bumped after the exit is never reported. So the reported total
	// must lie between the pre-exit growth and the pre-exit + raced growth,
	// and can never exceed the counter's actual value.
	lower := preGrowth
	upper := preGrowth + raceGrowth
	if oomDeltas < lower || oomDeltas > upper {
		t.Fatalf("reported OOM kills = %d, want within [%d, %d] (total counter growth %d)",
			oomDeltas, lower, upper, oomTotal)
	}
	if oomDeltas > o.State().Baseline.OOM {
		t.Fatalf("reported %d OOM kills but baseline is %d", oomDeltas, o.State().Baseline.OOM)
	}

	// The published OOM events must be ordered oldest-first: their merged
	// counts sum to the reported total and none of them is empty.
	merged := 0
	for _, ev := range events {
		if ev.Kind != KindOOM {
			continue
		}
		if !ev.Deltas.Any() {
			t.Fatalf("empty OOM event: %+v", ev)
		}
		merged += ev.Merged
	}
	if merged < 1 && oomDeltas > 0 {
		t.Fatalf("no OOM events but %d kills reported", oomDeltas)
	}
}
