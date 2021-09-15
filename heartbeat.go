package workers

import (
	"time"
	"encoding/json"
	"log"
	"os"
)

type HeartbeatInfo struct {
	Hostname string 			`json:"hostname"`
	StartedAt int64   		`json:"started_at"`
	Pid int 							`json:"pid"`
	Tag string 						`json:"tag"`
	Concurrency int 			`json:"concurrency"`
	Queues []string 			`json:"queues"`
	Labels []string  			`json:"labels"`
	Identity string 			`json:"identity"`
}

type Heartbeat struct {
	Beat time.Time
	Quiet bool
	Busy int
	RttUS int
	RSS int
	Info string
}

func (s *apiServer) StartHeartbeat() {
	heartbeatTicker := time.NewTicker(5 * time.Second)
	for {
	    select {
	    case <-heartbeatTicker.C:
	    	for _, m := range s.managers {
	    		log.Println("sending heartbeat")
	    		m.SendHeartbeat()
	    	}
	    }
	}
}

func BuildHeartbeat(m *Manager) *Heartbeat {
	queues := []string{}
	concurrency := 0
	busy := 0
	for _, w := range m.workers {
		queues = append(queues, w.queue)
		concurrency += w.concurrency // add up all concurrency here because it can be specified on a per-worker basis.
	}

	h1 := &HeartbeatInfo{
	  Hostname:   "john.bolliger-1-go",
	  StartedAt: m.startedAt.UTC().Unix(),
	  Pid: os.Getpid(),
	  Tag: "sometag",
	  Concurrency: concurrency,
	  Queues: queues,
	  Labels: []string{},
	  Identity: "john.bolliger-1-go:44179:somehash",
	}
	h1m, _ := json.Marshal(h1)

	// inProgress := m.inProgressMessages()
	// ns := m.opts.Namespace

	// for queue, msgs := range inProgress {
	// 	var jobs []JobStatus
	// 	for _, m := range msgs {
	// 		jobs = append(jobs, JobStatus{
	// 			Message:   m,
	// 			StartedAt: m.startedAt,
	// 		})
	// 	}
	// 	stats.Jobs[ns+queue] = jobs
	// 	q = append(q, queue)
	// }


	h := &Heartbeat{
		Beat: time.Now(),
		Quiet: false,
		Busy: busy,
		RttUS: -1,
		RSS: -1,
		Info: string(h1m),
	}

	return h
}

