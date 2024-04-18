package workers

import (
	"encoding/json"
	"net/http"
	"time"
)

func (s *apiServer) Stats(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	allStats := []Stats{}
	for _, m := range s.managers {
		stats, err := m.GetStats()
		if err != nil {
			s.logger.Println("couldn't retrieve stats for manager:", err)
		} else {
			allStats = append(allStats, stats)
		}
	}

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(allStats)
}

// Stats contains current stats for a manager
type Stats struct {
	Name                  string                 `json:"manager_name"`
	Processed             int64                  `json:"processed"`
	Failed                int64                  `json:"failed"`
	Jobs                  map[string][]JobStatus `json:"jobs"`
	Enqueued              map[string]int64       `json:"enqueued"`
	RetryCount            int64                  `json:"retry_count"`
	HeartbeatLastPushedAt time.Time              `json:"heartbeat_last_pushed_at"`
}

// JobStatus contains the status and data for active jobs of a manager
type JobStatus struct {
	Message   *Msg  `json:"message"`
	StartedAt int64 `json:"started_at"`
}
