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
	"os"
	"strconv"
	"strings"
	"syscall"
)

// ProcProbe is the production ProcessProbe.
//
// Scanning cost is controlled in two ways:
//
//  1. /proc is readdir'd exactly once per pass to enumerate pids, instead of
//     opening and parsing per-task files for every one of potentially several
//     thousand tasks.
//  2. Liveness is then decided with a single signal-0 check per pid rather
//     than reading /proc/<pid>/status or other files. A directory entry under
//     /proc can linger briefly for a zombie or a process being torn down, so
//     the signal check is the authoritative verdict: ESRCH means gone.
//
// Ownership marks are read lazily — only from processes whose cgroup path
// contains a container id — and are used solely for orphan detection.
type ProcProbe struct{}

// NewProcProbe returns the production /proc-backed probe.
func NewProcProbe() *ProcProbe { return &ProcProbe{} }

// SnapshotProcesses implements ProcessProbe.
func (p *ProcProbe) SnapshotProcesses() (map[int]ProcessInfo, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, ErrProbeUnavailable
	}

	out := make(map[int]ProcessInfo, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			// Non-numeric entries (sys, self, ...) are not processes.
			continue
		}

		info := ProcessInfo{Pid: pid}
		switch err := syscall.Kill(pid, 0); err {
		case nil:
			info.Alive = true
		case syscall.EPERM:
			// The process exists but we lack permission to signal it: it is
			// still alive.
			info.Alive = true
		case syscall.ESRCH:
			info.Alive = false
		default:
			// Ambiguous; treat as alive rather than risk a false republish.
			info.Alive = true
		}

		// Best-effort ownership attribution for orphan detection. A failure to
		// read the cgroup file just leaves OwnerID empty.
		if owner := readOwnerFromCgroup(pid); owner != "" {
			info.OwnerID = owner
		}
		out[pid] = info
	}
	return out, nil
}

// readOwnerFromCgroup extracts a container/task id from a process's cgroup
// path when one is present. It is deliberately tolerant: any read or parse
// failure yields an empty owner, which simply disables orphan reporting for
// that process.
func readOwnerFromCgroup(pid int) string {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/cgroup")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		// cgroup v2: "0::/kubepods/.../<containerID>"
		// cgroup v1: "9:devices:/kubepods/.../<containerID>"
		idx := strings.LastIndex(line, "/")
		if idx < 0 || idx == len(line)-1 {
			continue
		}
		tail := line[idx+1:]
		// A 64-hex-char segment is the typical container id.
		if len(tail) == 64 && isHex(tail) {
			return tail
		}
	}
	return ""
}

func isHex(s string) bool {
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'f':
		default:
			return false
		}
	}
	return true
}
