package gateway

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

// parkStore remembers which exits the free tier parked for the rest of its
// quota window.
//
// The anonymous allowance is counted per egress IP and the upstream states the
// reset in Retry-After as "seconds until 00:00 UTC". The value stored here is
// therefore an absolute instant, not a duration: a runtime rebuilt hours later
// must still keep a spent exit out instead of handing it back to the cursor.
type parkStore struct {
	path  string
	mu    sync.Mutex
	parks map[string]int64
}

// newParkStore loads the parks recorded by an earlier runtime. A missing or
// unreadable file simply means no parks, never an error the caller must handle.
func newParkStore(path string) *parkStore {
	store := &parkStore{path: path, parks: map[string]int64{}}
	store.load()
	return store
}

func (s *parkStore) load() {
	if s == nil || s.path == "" {
		return
	}
	data, err := os.ReadFile(s.path)
	if err != nil {
		return
	}
	var parks map[string]int64
	if err := json.Unmarshal(data, &parks); err != nil {
		return
	}
	now := time.Now().UnixNano()
	for address, until := range parks {
		if until > now {
			s.parks[address] = until
		}
	}
}

// restore applies every unexpired park to the node pool of a fresh runtime.
func (s *parkStore) restore(nodes *anonymousNodes) {
	if s == nil || nodes == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UnixNano()
	for _, node := range nodes.nodes {
		if node.proxy == nil {
			continue
		}
		until, ok := s.parks[node.proxy.name]
		if !ok || until <= now {
			continue
		}
		node.cooldownUntil.Store(until)
	}
}

// record stores one park and rewrites the file. A park happens at most once per
// spent exit per quota window, so writing the whole map is cheap enough and
// keeps the file self-describing.
func (s *parkStore) record(address string, until time.Time) {
	if s == nil || s.path == "" || address == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UnixNano()
	for key, value := range s.parks {
		if value <= now {
			delete(s.parks, key)
		}
	}
	s.parks[address] = until.UnixNano()
	data, err := json.Marshal(s.parks)
	if err != nil {
		return
	}
	temporary := s.path + ".tmp"
	if err := os.WriteFile(temporary, data, 0o600); err != nil {
		return
	}
	_ = os.Rename(temporary, s.path)
}

// parkPath names the file that survives a restart. It sits beside the config so
// the container's mounted state volume already covers it.
func parkPath(configPath string) string {
	if configPath == "" {
		return ""
	}
	return configPath + ".proxy-park.json"
}
