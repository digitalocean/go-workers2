package workers

import (
	"encoding/json"
	"net/http"
	"sync"
)

type apiServer struct {
	lock     sync.Mutex
	managers map[string]*Manager
}

var globalApiServer = &apiServer{
	managers: map[string]*Manager{},
}

func (s *apiServer) registerManager(m *Manager) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.managers[m.uuid] = m
}

func (s *apiServer) deregisterManager(m *Manager) {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.managers, m.uuid)
}

func (s *apiServer) Stats(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	allStats := []Stats{}
	for _, m := range s.managers {
		s, err := m.GetStats()
		if err != nil {
			Logger.Println("couldn't retrieve stats for manager:", err)
		} else {
			allStats = append(allStats, s)
		}
	}

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(allStats)
}

type Stats struct {
	Name       string                 `json:"manager_name"`
	Processed  int64                  `json:"processed"`
	Failed     int64                  `json:"failed"`
	Jobs       map[string][]JobStatus `json:"jobs"`
	Enqueued   map[string]int64       `json:"enqueued"`
	RetryCount int64                  `json:"retry_count"`
}

type JobStatus struct {
	Message   *Msg  `json:"message"`
	StartedAt int64 `json:"started_at"`
}
