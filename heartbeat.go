package workers

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
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

func (m *Manager) buildHeartbeat() (*storage.Heartbeat, error) {
	queues := []string{}

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

			jsonMsg, err := json.Marshal(workerMsg)
			if err != nil {
				return nil, err
			}

			wrapper := &HeartbeatWorkerMsgWrapper{
				Queue:   w.queue,
				Payload: string(jsonMsg),
				RunAt:   msg.startedAt,
			}

			jsonWrapper, err := json.Marshal(wrapper)
			if err != nil {
				return nil, err
			}

			msgs[r.tid] = string(jsonWrapper)
		}

		w.runnersLock.Unlock()
	}

	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	pid := os.Getpid()

	if m.opts.ManagerDisplayName != "" {
		hostname = hostname + ":" + m.opts.ManagerDisplayName
	}

	tag := "default"

	if m.opts.Namespace != "" {
		tag = strings.ReplaceAll(m.opts.Namespace, ":", "")
	}

	identity := fmt.Sprintf("%s:%d:%s", hostname, pid, m.processNonce)

	heartbeatInfo := &HeartbeatInfo{
		Hostname:    hostname,
		StartedAt:   m.startedAt.UTC().Unix(),
		Pid:         pid,
		Tag:         tag,
		Concurrency: concurrency,
		Queues:      queues,
		Labels:      []string{},
		Identity:    identity,
	}
	heartbeatInfoJson, err := json.Marshal(heartbeatInfo)

	if err != nil {
		return nil, err
	}

	heartbeat := &storage.Heartbeat{
		Identity:       identity,
		Beat:           time.Now(),
		Quiet:          false,
		Busy:           busy,
		RSS:            0, // rss is not currently supported
		Info:           string(heartbeatInfoJson),
		Pid:            pid,
		WorkerMessages: msgs,
	}

	return heartbeat, nil
}
