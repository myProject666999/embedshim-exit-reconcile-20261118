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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Baseline is the counter reference used to decide whether a sample contains
// new events. Only growth relative to the baseline produces an event.
//
// The baseline is persisted with the task so that, after a shim restart, the
// restart recovery path can tell three situations apart:
//
//  1. counters grew while the shim was down (current >= persisted). Those
//     increases happened before the first watch of this run; they are NOT
//     reported, because their position relative to the exit event cannot be
//     established any more. The baseline is advanced to the current reading.
//  2. counters went backwards (current < persisted). The source was recreated
//     — usually a host reboot, which resets the cgroup counters. This is
//     detected and logged, the baseline is reset to the current reading, and
//     no delta is reported. A negative delta is never turned into an event.
//  3. counters unchanged. Nothing to do.
//
// In all three cases the effective baseline after restart is the current
// reading. The persisted value is the task's record of where the previous run
// stopped; it defines what "already reported" means across restarts.
type Baseline struct {
	// Counters is the last observed counter reading.
	Counters Counters `json:"counters"`
	// Fields records which counters the source exposed.
	Fields FieldSet `json:"fields"`
	// Generation is the cgroup interface the reading came from.
	Generation Generation `json:"generation"`
	// UpdatedAt is when the reading was taken.
	UpdatedAt time.Time `json:"updated_at"`
}

// BaselineStore persists one task's baseline under a caller-chosen file path,
// typically inside the task bundle. Writes are atomic (write + rename) and
// synchronous: the file is small (a few dozen bytes) and must be durable
// before a possible reboot, so a fire-and-forget write would defeat its only
// purpose. Callers on the event path should therefore expect one small
// filesystem write per observed counter increase, not per sample.
type BaselineStore struct {
	path string
}

// NewBaselineStore returns a baseline store for path.
func NewBaselineStore(path string) *BaselineStore {
	return &BaselineStore{path: path}
}

// Load reads the persisted baseline. A missing file yields (nil, nil): the
// task simply has no recorded baseline, which is the normal first-boot case.
func (s *BaselineStore) Load() (*Baseline, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read memory baseline %s: %w", s.path, err)
	}
	var b Baseline
	if err := json.Unmarshal(data, &b); err != nil {
		// A corrupt baseline must never prevent the container from starting
		// or the task from being deleted. Treat it as absent; the caller
		// starts from the current reading, which cannot emit phantom events.
		return nil, fmt.Errorf("parse memory baseline %s: %w", s.path, err)
	}
	return &b, nil
}

// Save atomically replaces the persisted baseline.
func (s *BaselineStore) Save(b Baseline) error {
	data, err := json.Marshal(b)
	if err != nil {
		return fmt.Errorf("encode memory baseline: %w", err)
	}
	return atomicWriteFile(s.path, data)
}

func atomicWriteFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after a successful rename

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

// DegradedReason classifies why memory events are unavailable for a task.
type DegradedReason string

const (
	// DegradedSourceGone: the cgroup or its counter file does not exist
	// (already removed, or the process was never put into a memory cgroup).
	DegradedSourceGone DegradedReason = "source-gone"
	// DegradedPermission: the counter file exists but cannot be read or
	// watched by this process.
	DegradedPermission DegradedReason = "permission-denied"
	// DegradedUnsupported: the counter file exposes no counter this package
	// understands (for example a v1 kernel without oom_kill).
	DegradedUnsupported DegradedReason = "unsupported-source"
	// DegradedWatchFailed: the counters were readable but the watch could
	// not be established (inotify unavailable, watch limit reached, ...).
	DegradedWatchFailed DegradedReason = "watch-failed"
)

// Degraded describes why a task runs without memory events. It is not an
// error: container startup must proceed. Callers persist Marker on the task
// (for example as a file in the bundle or a task label) so that operators and
// the recovery path can tell "this task will never emit OOM" from "this task
// simply had no OOM".
type Degraded struct {
	// Reason is the coarse classification.
	Reason DegradedReason `json:"reason"`
	// Detail carries the underlying error text, empty when not applicable.
	Detail string `json:"detail,omitempty"`
	// Generation records which cgroup interface was probed.
	Generation Generation `json:"generation"`
	// At is when the delegation was decided.
	At time.Time `json:"at"`
	// Err is the underlying error. It is not serialized.
	Err error `json:"-"`
}

// Marker returns a stable, one-line marker suitable for a task label or a
// marker file in the bundle.
func (d *Degraded) Marker() string {
	if d == nil {
		return ""
	}
	if d.Detail != "" {
		return fmt.Sprintf("memory-events-degraded:%s:%s", d.Reason, d.Detail)
	}
	return fmt.Sprintf("memory-events-degraded:%s", d.Reason)
}

func (d *Degraded) Error() string { return d.Marker() }

// Unwrap exposes the underlying error.
func (d *Degraded) Unwrap() error { return d.Err }

// DegradedMarkerFile is the bundle file name a caller can use to persist
// Degraded alongside the other task state.
const DegradedMarkerFile = "memory-events.degraded.json"

// SaveDegraded persists the marker into dir under DegradedMarkerFile.
func SaveDegraded(dir string, d *Degraded) error {
	if d == nil {
		return nil
	}
	data, err := json.Marshal(d)
	if err != nil {
		return err
	}
	return atomicWriteFile(filepath.Join(dir, DegradedMarkerFile), data)
}

// LoadDegraded reads the marker previously written by SaveDegraded. A missing
// file yields (nil, nil).
func LoadDegraded(dir string) (*Degraded, error) {
	data, err := os.ReadFile(filepath.Join(dir, DegradedMarkerFile))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var d Degraded
	if err := json.Unmarshal(data, &d); err != nil {
		return nil, err
	}
	return &d, nil
}
