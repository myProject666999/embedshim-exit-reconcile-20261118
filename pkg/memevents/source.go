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
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/containerd/cgroups"
	"golang.org/x/sys/unix"
)

// Generation identifies which cgroup interface a source reads.
type Generation int

const (
	// GenerationV1 reads the "memory.oom_control" counter file of cgroup v1.
	GenerationV1 Generation = iota

	// GenerationV2 reads the "memory.events" counter file of cgroup v2.
	GenerationV2
)

func (g Generation) String() string {
	switch g {
	case GenerationV1:
		return "cgroup-v1"
	case GenerationV2:
		return "cgroup-v2"
	default:
		return "unknown"
	}
}

// Note is one notification from a Watch.
//
// Gone carries the cgroup-deleted notification. It is deliberately a
// distinct note rather than a Changed note with a zero sample: a deleted
// cgroup must never be interpreted as "the counters dropped to zero", which
// would look like a counter increase after re-baselining. A Watch channel is
// closed right after a Gone note.
type Note struct {
	// Changed means the counter file may have been modified; the receiver
	// must re-sample.
	Changed bool
	// Gone means the cgroup (or the counter file) has been removed. No
	// further notes are delivered.
	Gone bool
	// Err is a watch failure other than deletion. It is advisory: the
	// receiver keeps the current baseline and logs.
	Err error
}

// Watch delivers Notes from a Source. C returns the note channel; it is
// closed when the watch ends because the source is gone or Close was called.
// Notes are delivered from one goroutine at a time, so C may be buffered.
type Watch interface {
	C() <-chan Note
	// Close stops the watch and releases its descriptor. It is idempotent.
	Close() error
}

// Source is the generation-neutral memory event source. Implementations are
// the only place that knows about cgroup v1/v2 differences.
//
// Sample must be non-blocking for practical purposes: a single open+read of a
// small procfs/cgroupfs file. It is called synchronously from the exit path,
// so it must not wait for anything else.
type Source interface {
	// Sample reads the counters right now.
	Sample() (Sample, error)
	// Watch establishes a watch. Returning an error means no watch could be
	// established (missing file, permission denied, inotify unavailable);
	// the caller degrades to "no memory events" instead of failing the task.
	Watch() (Watch, error)
	// Generation reports which cgroup interface backs the source.
	Generation() Generation
}

// FileSourceConfig configures NewFileSource.
type FileSourceConfig struct {
	// Path is the counter file: memory.events (v2) or memory.oom_control
	// (v1).
	Path string
	// Generation selects the parser.
	Generation Generation
	// RescanInterval, when > 0, adds a periodic re-sample in addition to
	// inotify events. It is a safety net for kernels and filesystems where
	// the modify notification is not delivered for cgroup files. Zero
	// disables it.
	RescanInterval time.Duration
}

// NewFileSource returns a Source backed by a single cgroup counter file.
func NewFileSource(cfg FileSourceConfig) Source {
	return &fileSource{cfg: cfg}
}

type fileSource struct {
	cfg FileSourceConfig
}

func (s *fileSource) Generation() Generation { return s.cfg.Generation }

func (s *fileSource) Sample() (Sample, error) {
	f, err := os.Open(s.cfg.Path)
	if err != nil {
		return Sample{}, classifyOpenError(s.cfg.Path, err)
	}
	defer f.Close()

	sample, err := parseSample(s.cfg.Generation, f)
	if err != nil {
		return Sample{}, err
	}
	return sample, nil
}

func (s *fileSource) Watch() (Watch, error) {
	fd, err := unix.InotifyInit1(unix.IN_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("inotify init: %w", err)
	}

	mask := uint32(unix.IN_MODIFY | unix.IN_ATTRIB | unix.IN_CLOSE_WRITE | unix.IN_DELETE_SELF | unix.IN_MOVE_SELF)
	wd, err := unix.InotifyAddWatch(fd, s.cfg.Path, mask)
	if err != nil {
		unix.Close(fd)
		return nil, classifyWatchError(s.cfg.Path, err)
	}

	w := &fileWatch{
		fd:     fd,
		notes:  make(chan Note, 16),
		done:   make(chan struct{}),
		rescan: s.cfg.RescanInterval,
		path:   s.cfg.Path,
	}
	// The watch descriptor is only used for removal; keep it in the struct so
	// Close can drop it before closing the fd.
	w.wd = wd

	w.start()
	return w, nil
}

// fileWatch is the inotify-backed Watch of a fileSource.
type fileWatch struct {
	fd     int
	wd     int
	notes  chan Note
	done   chan struct{}
	rescan time.Duration
	path   string

	stopOnce sync.Once
	wg       sync.WaitGroup
}

func (w *fileWatch) C() <-chan Note { return w.notes }

func (w *fileWatch) start() {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.readLoop()
	}()
	if w.rescan > 0 {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			w.pollLoop()
		}()
	}
	// Close notes when both pump goroutines have exited, so the receiver can
	// distinguish "watch ended" from "watch still quiet".
	go func() {
		w.wg.Wait()
		close(w.notes)
	}()
}

func (w *fileWatch) Close() error {
	w.stopOnce.Do(func() {
		close(w.done)
		unix.InotifyRmWatch(w.fd, uint32(w.wd)) //nolint:errcheck
		unix.Close(w.fd)
	})
	return nil
}

// emit delivers a note unless the watch has been closed. It never blocks on a
// full channel: dropping a Changed hint is safe because the next hint or the
// exit-path sample re-reads the counters, whereas blocking here would keep
// the inotify fd from being drained.
func (w *fileWatch) emit(n Note) {
	select {
	case <-w.done:
	case w.notes <- n:
	default:
	}
}

// readLoop translates inotify records into Notes.
//
// Every kernel record is inspected because the deletion records
// (IN_DELETE_SELF, IN_MOVE_SELF, IN_IGNORED) are the "cgroup deleted"
// notification and must be reported as Gone, never as Changed. A plain
// IN_MODIFY is only a hint: the receiver re-reads the counters and decides
// whether anything actually grew, so a modify caused by an unrelated field
// (for example on v1 when only under_oom flips) produces no event.
func (w *fileWatch) readLoop() {
	buf := make([]byte, unix.SizeofInotifyEvent*8)
	for {
		n, err := unix.Read(w.fd, buf)
		if err != nil {
			if err == unix.EINTR {
				continue
			}
			select {
			case <-w.done:
				return
			default:
			}
			if err == unix.EBADF {
				return
			}
			w.emit(Note{Err: fmt.Errorf("inotify read: %w", err)})
			return
		}
		gone, changed := decodeInotify(buf[:n])
		switch {
		case gone:
			w.emit(Note{Gone: true})
			return
		case changed:
			w.emit(Note{Changed: true})
		}
	}
}

// decodeInotify returns whether a batch of inotify records contains a
// deletion record and whether it contains a modification record. The record
// layout is
//
//	int32 wd; uint32 mask; uint32 cookie; uint32 len; char name[len];
//
// decoded without unsafe so it can be exercised with synthetic bytes.
func decodeInotify(buf []byte) (gone, changed bool) {
	for off := 0; off+unix.SizeofInotifyEvent <= len(buf); {
		mask := binary.LittleEndian.Uint32(buf[off+4:])
		nameLen := int(binary.LittleEndian.Uint32(buf[off+12:]))
		switch {
		case mask&(unix.IN_DELETE_SELF|unix.IN_MOVE_SELF|unix.IN_IGNORED) != 0:
			gone = true
		case mask&(unix.IN_MODIFY|unix.IN_CLOSE_WRITE|unix.IN_ATTRIB) != 0:
			changed = true
		}
		off += unix.SizeofInotifyEvent + nameLen
	}
	return gone, changed
}

func (w *fileWatch) pollLoop() {
	t := time.NewTicker(w.rescan)
	defer t.Stop()
	for {
		select {
		case <-w.done:
			return
		case <-t.C:
			if _, err := os.Stat(w.path); err != nil {
				if os.IsNotExist(err) {
					w.emit(Note{Gone: true})
				} else {
					w.emit(Note{Err: err})
				}
				return
			}
			w.emit(Note{Changed: true})
		}
	}
}

// DefaultRescanInterval is used for cgroup v1. cgroup v1's native OOM
// notification is an eventfd registered through cgroup.event_control, not a
// modification of memory.oom_control itself, so an inotify watch on the file
// is not a reliable trigger there. A periodic re-sample closes that gap; the
// exit and delete paths read synchronously in any case, so the poll only
// affects how promptly a long-running OOM is reported.
const DefaultRescanInterval = time.Second

// NewSourceForPID resolves the counter file of pid's cgroup and returns a
// ready Source. cgroup v1 sources get a periodic rescan; cgroup v2 sources
// rely on inotify on memory.events, which the kernel does deliver.
func NewSourceForPID(pid int) (Source, error) {
	path, gen, err := SourcePathForPID(pid)
	if err != nil {
		return nil, err
	}
	cfg := FileSourceConfig{Path: path, Generation: gen}
	if gen == GenerationV1 {
		cfg.RescanInterval = DefaultRescanInterval
	}
	return NewFileSource(cfg), nil
}

// SourcePathForPID resolves the counter file that belongs to the cgroup of
// pid. The generation is selected by the mode of the running kernel:
//
//   - cgroup v2 (Unified): <unified-mount><group>/memory.events
//   - cgroup v1 (Legacy/Hybrid): <memory-mount><group>/memory.oom_control
//
// Both the relative group path and the mount point are read from /proc, so
// the result is correct for nested cgroups, systemd cgroups and non-standard
// cgroup mount points alike.
func SourcePathForPID(pid int) (path string, gen Generation, err error) {
	groups, err := parseCgroupFile(fmt.Sprintf("/proc/%d/cgroup", pid))
	if err != nil {
		return "", 0, err
	}

	if cgroups.Mode() == cgroups.Unified {
		group, ok := groups[""]
		if !ok {
			return "", GenerationV2, fmt.Errorf("no cgroup2 entry for pid %d", pid)
		}
		return filepath.Join(unifiedMountpoint, group, "memory.events"), GenerationV2, nil
	}

	group, ok := groups["memory"]
	if !ok {
		return "", GenerationV1, fmt.Errorf("pid %d is not in a memory cgroup", pid)
	}
	root, err := findMountPoint("memory")
	if err != nil {
		return "", GenerationV1, err
	}
	return filepath.Join(root, group, "memory.oom_control"), GenerationV1, nil
}

const unifiedMountpoint = "/sys/fs/cgroup"

// parseCgroupFile maps each cgroup hierarchy to the relative group path.
// cgroup v2 entries (the "0::" line) are keyed by the empty string.
func parseCgroupFile(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	out := make(map[string]string)
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		// hierarchy-ID:controller-list:cgroup-path
		parts := strings.SplitN(sc.Text(), ":", 3)
		if len(parts) != 3 {
			continue
		}
		if parts[0] == "0" && parts[1] == "" {
			out[""] = parts[2]
			continue
		}
		for _, ctl := range strings.Split(parts[1], ",") {
			out[ctl] = parts[2]
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// findMountPoint returns the mount point of the cgroup1 hierarchy that has
// the named controller attached.
func findMountPoint(controller string) (string, error) {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return "", err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 10 || fields[len(fields)-3] != "cgroup" {
			continue
		}
		for _, opt := range strings.Split(fields[len(fields)-1], ",") {
			if opt == controller {
				return fields[4], nil
			}
		}
	}
	if err := sc.Err(); err != nil {
		return "", err
	}
	return "", fmt.Errorf("no cgroup v1 mount for controller %q", controller)
}

// parseSample dispatches to the parser of the cgroup generation.
func parseSample(gen Generation, r io.Reader) (Sample, error) {
	switch gen {
	case GenerationV2:
		return parseV2MemoryEvents(r)
	case GenerationV1:
		return parseV1OOMControl(r)
	default:
		return Sample{}, fmt.Errorf("unknown cgroup generation %d", gen)
	}
}

// parseV2MemoryEvents parses cgroup v2 memory.events.
//
// Recognised fields: oom (trigger count) and oom_kill (kill count). A field
// that is absent is marked unsupported and reads as zero, so it can never
// produce an event. Other fields (low, high, max) are counted by the kernel
// for threshold crossings, not OOM kills, and are ignored.
func parseV2MemoryEvents(r io.Reader) (Sample, error) {
	values, err := parseKeyValues(r)
	if err != nil {
		return Sample{}, err
	}
	var s Sample
	if v, ok := values["oom"]; ok {
		s.Counters.OOM = v
		s.Fields.OOM = true
	}
	if v, ok := values["oom_kill"]; ok {
		s.Counters.OOMKill = v
		s.Fields.OOMKill = true
	}
	if !s.Fields.Supported() {
		return s, ErrSourceUnsupported
	}
	return s, nil
}

// parseV1OOMControl parses cgroup v1 memory.oom_control.
//
// The file exposes oom_kill (a counter) but no trigger counter: under_oom is
// a 0/1 state. Fields.OOM is therefore always false on v1 and the trigger
// count cannot be reported. If a kernel omits oom_kill entirely the sample is
// still valid but supports no counter, which surfaces as
// ErrSourceUnsupported at attach time (degraded, no OOM events).
func parseV1OOMControl(r io.Reader) (Sample, error) {
	values, err := parseKeyValues(r)
	if err != nil {
		return Sample{}, err
	}
	var s Sample
	if v, ok := values["oom_kill"]; ok {
		s.Counters.OOMKill = v
		s.Fields.OOMKill = true
	}
	if !s.Fields.Supported() {
		return s, ErrSourceUnsupported
	}
	return s, nil
}

func parseKeyValues(r io.Reader) (map[string]uint64, error) {
	out := make(map[string]uint64)
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		v, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}
		out[fields[0]] = v
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read counter file: %w", err)
	}
	return out, nil
}

func classifyOpenError(path string, err error) error {
	if os.IsNotExist(err) || isGoneErr(err) {
		return fmt.Errorf("%w: %s", ErrSourceGone, path)
	}
	return fmt.Errorf("open %s: %w", path, err)
}

func classifyWatchError(path string, err error) error {
	if err == unix.ENOENT || err == unix.ENOTDIR {
		return fmt.Errorf("%w: %s", ErrSourceGone, path)
	}
	return fmt.Errorf("inotify watch %s: %w", path, err)
}

func isGoneErr(err error) bool {
	return err == unix.ENODEV || err == unix.ENOENT || err == unix.ENOTDIR
}
