package workers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"

	"github.com/go-redis/redis"
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

	var allStats []stats
	for _, m := range ss.managers {
		s, err := statsForManager(m)
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

type stats struct {
	Name      string                 `json:"manager_name"`
	Processed int64                  `json:"processed"`
	Failed    int64                  `json:"failed"`
	Jobs      map[string][]jobStatus `json:"jobs"`
	Enqueued  map[string]int64       `json:"enqueued"`
	Retries   int64                  `json:"retries"`
}

type jobStatus struct {
	Message   *Msg  `json:"message"`
	StartedAt int64 `json:"started_at"`
}

func statsForManager(m *Manager) (stats, error) {
	stats := stats{
		Jobs:     map[string][]jobStatus{},
		Enqueued: map[string]int64{},
		Name:     m.opts.ManagerDisplayName,
	}
	pipe := m.opts.client.Pipeline()
	inProgress := m.inProgressMessages()
	ns := m.opts.Namespace

	pGet := pipe.Get(ns + "stat:processed")
	fGet := pipe.Get(ns + "stat:failed")
	rGet := pipe.ZCard(m.RetryQueue())
	qLen := map[string]*redis.IntCmd{}

	for queue, msgs := range inProgress {
		var jobs []jobStatus
		for _, m := range msgs {
			jobs = append(jobs, jobStatus{
				Message:   m,
				StartedAt: m.startedAt,
			})
		}
		stats.Jobs[ns+queue] = jobs
		qLen[ns+queue] = pipe.LLen(fmt.Sprintf("%squeue:%s", ns, queue))
	}

	_, err := pipe.Exec()

	if err != nil && err != redis.Nil {
		return stats, err
	}
	stats.Processed, _ = strconv.ParseInt(pGet.Val(), 10, 64)
	stats.Failed, _ = strconv.ParseInt(fGet.Val(), 10, 64)
	stats.Retries = rGet.Val()

	for q, l := range qLen {
		stats.Enqueued[q] = l.Val()
	}
	return stats, nil

}

func StartStatsServer(port int) {
	http.HandleFunc("/stats", globalStatsServer.Stats)

	Logger.Println("Stats are available at", fmt.Sprint("http://localhost:", port, "/stats"))

	if err := http.ListenAndServe(fmt.Sprint(":", port), nil); err != nil {
		Logger.Println(err)
	}
}
