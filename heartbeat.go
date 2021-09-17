package workers

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	// "log"
	"github.com/digitalocean/go-workers2/storage"
	"os"
	"strings"
	"time"
)

type HeartbeatInfo struct {
	Hostname    string   `json:"hostname"`
	StartedAt   int64    `json:"started_at"`
	Pid         int      `json:"pid"`
	Tag         string   `json:"tag"`
	Concurrency int      `json:"concurrency"`
	Queues      []string `json:"queues"`
	Labels      []string `json:"labels"`
	Identity    string   `json:"identity"`
}

//  => {"retry"=>9, "queue"=>"sleeprb", "backtrace"=>true, "class"=>"SleepWorker", "args"=>[60], "jid"=>"348adede638ab7d4c2e547e7", "created_at"=>1631905645.1018732, "Trace-Context"=>{"uber-trace-id"=>"8e55bdaf3409cbbb:8e55bdaf3409cbbb:0:1"}, "enqueued_at"=>1631905645.1061718}

type HeartbeatWorkerMsgWrapper struct {
	Queue   string `json:"queue"`
	Payload string `json:"payload"`
	RunAt   int64  `json:"run_at"`
	Tid     string `json:"tid"`
}

type HeartbeatWorkerMsg struct {
	Retry      int    `json:"retry"`
	Queue      string `json:"queue"`
	Backtrace  bool   `json:"backtrace"`
	Class      string `json:"class"`
	Args       *Args  `json:"args"`
	Jid        string `json:"jid"`
	CreatedAt  int64  `json:"created_at"`
	EnqueuedAt int64  `json:"enqueued_at"`
}

func GenerateProcessNonce() (string, error) {
	bytes := make([]byte, 12)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func BuildHeartbeat(m *Manager) *storage.Heartbeat {

	fmt.Println(m.inProgressMessages())

	queues := []string{}

	// tid -> wrapper(payload)
	msgs := map[string]string{}

	concurrency := 0
	busy := 0

	for _, w := range m.workers {
		queues = append(queues, w.queue)
		concurrency += w.concurrency // add up all concurrency here because it can be specified on a per-worker basis.
		busy += len(w.inProgressMessages())

		w.runnersLock.Lock()

		for _, r := range w.runners {
			msg := r.inProgressMessage()
			if msg == nil {
				continue
			}

			workerMsg := &HeartbeatWorkerMsg{
				Retry:      1,
				Queue:      w.queue,
				Backtrace:  false,
				Class:      msg.Class(),
				Args:       msg.Args(),
				Jid:        msg.Jid(),
				CreatedAt:  msg.startedAt, // not actually started at
				EnqueuedAt: time.Now().UTC().Unix(),
			}
			jsonMsg, _ := json.Marshal(workerMsg)

			wrapper := &HeartbeatWorkerMsgWrapper{
				Queue:   w.queue,
				Payload: string(jsonMsg),
				RunAt:   msg.startedAt,
			}

			jsonWrapper, _ := json.Marshal(wrapper)

			msgs[r.tid] = string(jsonWrapper)

		}

		w.runnersLock.Unlock()

		// 	fmt.Println("found msg", msg)
		// 	// found msg &{0xc00033a058 {"queue":"sleepgo","class":"Add","args":[10],"jid":"f4914398ea383d1a0611e884","enqueued_at":1631906124.4731379,"at":1631906124.473137} true 1631906139}

		// 	//  => {"retry"=>9, "queue"=>"sleeprb", "backtrace"=>true, "class"=>"SleepWorker", "args"=>[60], "jid"=>"348adede638ab7d4c2e547e7", "created_at"=>1631905645.1018732, "Trace-Context"=>{"uber-trace-id"=>"8e55bdaf3409cbbb:8e55bdaf3409cbbb:0:1"}, "enqueued_at"=>1631905645.1061718}

		// 	// 2) "{\"queue\":\"sleeprb\",\"payload\":\"{\\\"retry\\\":9,\\\"queue\\\":\\\"sleeprb\\\",\\\"backtrace\\\":true,\\\"class\\\":\\\"SleepWorker\\\",\\\"args\\\":[60],\\\"jid\\\":\\\"d722863bc0092f44d23f655e\\\",\\\"created_at\\\":1631910445.881293,\\\"Trace-Context\\\":{\\\"uber-trace-id\\\":\\\"8aa4890c1585e9f3:8aa4890c1585e9f3:0:1\\\"},\\\"enqueued_at\\\":1631910445.8897479}\",\"run_at\":1631910445}"

		// 	// 2) "{\"queue\":\"sleepgo\",\"payload\":\"{\\\"retry\\\":1,\\\"queue\\\":\\\"sleepgo\\\",\\\"backtrace\\\":false,\\\"class\\\":\\\"Add\\\",\\\"args\\\":[],\\\"jid\\\":\\\"0db564597153e031848c85d9\\\",\\\"created_at\\\":1631910751,\\\"enqueued_at\\\":1631910756,\\\"run_at\\\":1631910751}\"}"
	}

	hostname, _ := os.Hostname()
	pid := os.Getpid()

	if m.opts.ManagerDisplayName != "" {
		hostname = hostname + ":" + m.opts.ManagerDisplayName
	}

	tag := "default"

	if m.opts.Namespace != "" {
		tag = strings.ReplaceAll(m.opts.Namespace, ":", "")
	}

	identity := fmt.Sprintf("%s:%d:%s", hostname, pid, m.processNonce)

	h1 := &HeartbeatInfo{
		Hostname:    hostname,
		StartedAt:   m.startedAt.UTC().Unix(),
		Pid:         pid,
		Tag:         tag,
		Concurrency: concurrency,
		Queues:      queues,
		Labels:      []string{},
		Identity:    identity,
	}
	h1m, _ := json.Marshal(h1)

	h := &storage.Heartbeat{
		Identity:       identity,
		Beat:           time.Now(),
		Quiet:          false,
		Busy:           busy,
		RSS:            0, // rss is not currently supported
		Info:           string(h1m),
		Pid:            pid,
		WorkerMessages: msgs,
	}

	return h
}
