package workers

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/digitalocean/go-workers2/storage"
)

type HeartbeatInfo struct {
	Hostname    string   `json:"hostname"`
	StartedAt   int64    `json:"started_at"`
	Pid         int      `json:"Pid"`
	Tag         string   `json:"tag"`
	Concurrency int      `json:"concurrency"`
	Queues      []string `json:"queues"`
	Labels      []string `json:"labels"`
	Identity    string   `json:"identity"`
}

type HeartbeatWorkerMsgWrapper struct {
	Queue   string `json:"Queue"`
	Payload string `json:"payload"`
	RunAt   int64  `json:"run_at"`
	Tid     string `json:"Tid"`
}

type HeartbeatWorkerMsg struct {
	Retry      int    `json:"retry"`
	Queue      string `json:"Queue"`
	Backtrace  bool   `json:"backtrace"`
	Class      string `json:"class"`
	Args       *Args  `json:"args"`
	Jid        string `json:"jid"`
	CreatedAt  int64  `json:"created_at"`
	EnqueuedAt int64  `json:"enqueued_at"`
}

type afterHeartbeatFunc func(heartbeat *storage.Heartbeat, manager *Manager, staleMessageUpdates []*staleMessageUpdate) error

func GenerateProcessNonce() (string, error) {
	bytes := make([]byte, 12)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func (m *Manager) buildHeartbeat(heartbeatTime time.Time, ttl time.Duration) (*storage.Heartbeat, error) {
	queues := []string{}

	concurrency := 0
	busy := 0
	pid := os.Getpid()

	var workerHeartbeats []storage.WorkerHeartbeat

	for _, w := range m.workers {
		queues = append(queues, w.queue)
		concurrency += w.concurrency // add up all concurrency here because it can be specified on a per-worker basis.
		busy += len(w.inProgressMessages())

		w.runnersLock.Lock()
		for _, r := range w.runners {
			workerHeartbeat := storage.WorkerHeartbeat{
				Pid:             pid,
				Tid:             r.tid,
				Queue:           w.queue,
				InProgressQueue: w.inProgressQueue,
			}
			workerHeartbeats = append(workerHeartbeats, workerHeartbeat)
		}
		w.runnersLock.Unlock()
	}

	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	if m.opts.ManagerDisplayName != "" {
		hostname = hostname + ":" + m.opts.ManagerDisplayName
	}

	tag := "default"

	if m.opts.Namespace != "" {
		tag = strings.ReplaceAll(m.opts.Namespace, ":", "")
	}

	heartbeatID, err := m.getHeartbeatID()
	if err != nil {
		return nil, err
	}

	heartbeatInfo := &HeartbeatInfo{
		Hostname:    hostname,
		StartedAt:   m.startedAt.UTC().Unix(),
		Pid:         pid,
		Tag:         tag,
		Concurrency: concurrency,
		Queues:      queues,
		Labels:      []string{},
		Identity:    heartbeatID,
	}
	heartbeatInfoJson, err := json.Marshal(heartbeatInfo)

	if err != nil {
		return nil, err
	}

	heartbeat := &storage.Heartbeat{
		Identity:         heartbeatID,
		Beat:             heartbeatTime.UTC().Unix(),
		Quiet:            false,
		Busy:             busy,
		RSS:              0, // rss is not currently supported
		Info:             string(heartbeatInfoJson),
		Pid:              pid,
		ActiveManager:    m.IsActive(),
		WorkerHeartbeats: workerHeartbeats,
		Ttl:              ttl,
	}
	if m.opts.Heartbeat != nil && m.opts.Heartbeat.PrioritizedManager != nil {
		heartbeat.ManagerPriority = m.opts.Heartbeat.PrioritizedManager.ManagerPriority
	}

	return heartbeat, nil
}

func (m *Manager) getHeartbeatID() (string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return "", err
	}
	pid := os.Getpid()
	return fmt.Sprintf("%s:%d:%s", hostname, pid, m.processNonce), nil
}
