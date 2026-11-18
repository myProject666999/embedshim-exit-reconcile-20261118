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
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// The file source must read a real file, report a modification and report the
// deletion of the file as Gone.
func TestFileSourceWatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "memory.events")
	if err := os.WriteFile(path, []byte("oom 0\noom_kill 0\n"), 0600); err != nil {
		t.Fatal(err)
	}

	src := NewFileSource(FileSourceConfig{Path: path, Generation: GenerationV2})

	s, err := src.Sample()
	if err != nil {
		t.Fatal(err)
	}
	if s.Counters.Any() || s.Fields != v2Fields() {
		t.Fatalf("initial sample = %+v", s)
	}

	w, err := src.Watch()
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err := os.WriteFile(path, []byte("oom 2\noom_kill 1\n"), 0600); err != nil {
		t.Fatal(err)
	}

	select {
	case note := <-w.C():
		if !note.Changed {
			t.Fatalf("note = %+v, want Changed", note)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no modification note")
	}

	s, err = src.Sample()
	if err != nil {
		t.Fatal(err)
	}
	if s.Counters != (Counters{OOM: 2, OOMKill: 1}) {
		t.Fatalf("sample = %+v", s.Counters)
	}

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	// Deletion may be preceded by a modify/attrib record for the removal
	// itself; only the delete-self record means the source is gone.
	deadline := time.After(2 * time.Second)
waitGone:
	for {
		select {
		case note, ok := <-w.C():
			if !ok {
				// The watch channel may close directly once the watch is
				// removed, which also means gone.
				break waitGone
			}
			if note.Gone {
				break waitGone
			}
			if note.Err != nil {
				t.Fatalf("unexpected note %+v", note)
			}
		case <-deadline:
			t.Fatal("no deletion note")
		}
	}

	// After deletion the sample must report the source gone, never a zero
	// sample that would look like a counter reset.
	if _, err := src.Sample(); !errors.Is(err, ErrSourceGone) {
		t.Fatalf("sample after delete: %v, want ErrSourceGone", err)
	}
}

func TestFileSourceMissingFile(t *testing.T) {
	src := NewFileSource(FileSourceConfig{
		Path:       filepath.Join(t.TempDir(), "nope"),
		Generation: GenerationV2,
	})
	if _, err := src.Sample(); !errors.Is(err, ErrSourceGone) {
		t.Fatalf("err = %v", err)
	}
	if _, err := src.Watch(); !errors.Is(err, ErrSourceGone) {
		t.Fatalf("watch err = %v", err)
	}
}

// decodeInotify must classify deletion records as Gone and modification
// records as Changed, including a batch that contains both.
func TestDecodeInotify(t *testing.T) {
	record := func(mask uint32, name string) []byte {
		buf := make([]byte, unix.SizeofInotifyEvent+len(name))
		binary.LittleEndian.PutUint32(buf[4:], mask)
		binary.LittleEndian.PutUint32(buf[12:], uint32(len(name)))
		copy(buf[unix.SizeofInotifyEvent:], name)
		return buf
	}

	gone, changed := decodeInotify(record(unix.IN_MODIFY, ""))
	if gone || !changed {
		t.Fatalf("modify: gone=%v changed=%v", gone, changed)
	}

	for _, mask := range []uint32{unix.IN_DELETE_SELF, unix.IN_MOVE_SELF, unix.IN_IGNORED} {
		gone, changed = decodeInotify(record(mask, ""))
		if !gone || changed {
			t.Fatalf("mask %#x: gone=%v changed=%v", mask, gone, changed)
		}
	}

	batch := append(record(unix.IN_MODIFY, ""), record(unix.IN_DELETE_SELF, "")...)
	gone, _ = decodeInotify(batch)
	if !gone {
		t.Fatal("batch with delete_self not reported as gone")
	}

	// A short buffer must be ignored rather than panicking.
	if gone, changed := decodeInotify(make([]byte, 4)); gone || changed {
		t.Fatal("short buffer decoded")
	}
}

func TestSourcePathForPIDMatchesProc(t *testing.T) {
	path, gen, err := SourcePathForPID(os.Getpid())
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if path == "" {
		t.Fatal("empty path")
	}
	if gen != GenerationV1 && gen != GenerationV2 {
		t.Fatalf("generation = %v", gen)
	}
	// The file need not be readable in every environment (a container may
	// not be in a memory cgroup), but its parent directory must exist.
	if _, err := os.Stat(filepath.Dir(path)); err != nil {
		t.Fatalf("parent of %s: %v", path, err)
	}
}

func TestParseCgroupFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cgroup")
	content := "12:cpu,cpuacct:/a/b\n4:memory:/docker/abc\n0::/system.slice/x.scope\n"
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}

	got, err := parseCgroupFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got[""] != "/system.slice/x.scope" {
		t.Fatalf("unified entry = %q", got[""])
	}
	if got["memory"] != "/docker/abc" {
		t.Fatalf("memory entry = %q", got["memory"])
	}
	if got["cpu"] != "/a/b" || got["cpuacct"] != "/a/b" {
		t.Fatalf("cpu entries = %v", got)
	}
}
