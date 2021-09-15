package workers

import (
	"fmt"
	"time"
)

func startHeartbeat() {
	heartbeatTicker := time.NewTicker(5 * time.Second)

	for {
	    select {
	    case <-heartbeatTicker.C:
	    	sendHeartbeat()
	    }
	}
}

type Heartbeat struct {

}

// 12) "{\"hostname\":\"19883-JBolliger\",\"started_at\":1631662759.862796,\"pid\":44179,\"tag\":\"kirby\",\"concurrency\":10,\"queues\":[\"default\"],\"labels\":[],\"identity\":\"19883-JBolliger:44179:72231e7bea4f\"}"

type HeartbeatInfo struct {
	Hostname string 			`json:"hostname"`
	StartedAt time.time   `json:"started_at"`
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

func sendHeartbeat() {
	// encoded json
	//
	// conn.hmget(key, "info", "busy", "beat", "quiet", "rss", "rtt_us")
	// {\"hostname\":\"19883-JBolliger\",\"started_at\":1631662759.862796,\"pid\":44179,\"tag\":\"kirby\",\"concurrency\":10,\"queues\":[\"default\"],\"labels\":[],\"identity\":\"19883-JBolliger:44179:72231e7bea4f\"}
	//


	//  1) "beat"
	//  2) "1631664057.681488"
	//  3) "quiet"
	//  4) "false"
	//  5) "busy"
	//  6) "0"
	//  7) "rtt_us"
	//  8) "184"
	//  9) "rss"
	// 10) "0"
	// 11) "info"
	// 12) "{\"hostname\":\"19883-JBolliger\",\"started_at\":1631662759.862796,\"pid\":44179,\"tag\":\"kirby\",\"concurrency\":10,\"queues\":[\"default\"],\"labels\":[],\"identity\":\"19883-JBolliger:44179:72231e7bea4f\"}"

	h1 := &HeartbeatInfo{
      Hostname:   "john.bolliger-1-go",
      StartedAt: time.Unix(1631662759),
      Pid: 44179,
      Tag: "kirby",
      Concurrency: 5,
      Queues: []string{"default","myqueue1","myqueue2"},
      Labels: []string{},
      Identity: "john.bolliger-1-go:44179:somehash",
  h1m, _ := json.Marshal(h1)

  beat := fmt.Sprintf("%d",time.Now().Unix())
  startedAt := "1631662759.862796"
  quiet := false
  busy := 1
  rtt_us := 100 // note
  rss := -1
  info := h1m


}
