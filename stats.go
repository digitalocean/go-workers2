package workers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
)

type statsServer struct {
	lock     sync.Mutex
	managers map[string]*Manager
}

var globalStatsServer = &statsServer{
	managers: map[string]*Manager{},
}

func (ss *statsServer) registerManager(m *Manager) {
	ss.lock.Lock()
	defer ss.lock.Unlock()
	ss.managers[m.uuid] = m
}

func (ss *statsServer) deregisterManager(m *Manager) {
	ss.lock.Lock()
	defer ss.lock.Unlock()
	delete(ss.managers, m.uuid)
}

func (ss *statsServer) Stats(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	var allStats []Stats
	for _, m := range ss.managers {
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
	Name      string                 `json:"manager_name"`
	Processed int64                  `json:"processed"`
	Failed    int64                  `json:"failed"`
	Jobs      map[string][]JobStatus `json:"jobs"`
	Enqueued  map[string]int64       `json:"enqueued"`
	Retries   int64                  `json:"retries"`
}

type JobStatus struct {
	Message   *Msg  `json:"message"`
	StartedAt int64 `json:"started_at"`
}

func StartStatsServer(port int) {
	http.HandleFunc("/stats", globalStatsServer.Stats)

	Logger.Println("Stats are available at", fmt.Sprint("http://localhost:", port, "/stats"))

	if err := http.ListenAndServe(fmt.Sprint(":", port), nil); err != nil {
		Logger.Println(err)
	}
}
