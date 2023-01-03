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

type afterHeartbeatFunc func(heartbeat *storage.Heartbeat, updateActiveCluster *updateActiveClusterStatus, requeuedTaskRunnersStatus []requeuedTaskRunnerStatus)

func GenerateProcessNonce() (string, error) {
	bytes := make([]byte, 12)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func (m *Manager) buildHeartbeat(heartbeatTime time.Time) (*storage.Heartbeat, error) {
	queues := []string{}

	concurrency := 0
	busy := 0
	pid := os.Getpid()

	var taskRunnersInfo []storage.TaskRunnerInfo

	for _, w := range m.workers {
		queues = append(queues, w.queue)
		concurrency += w.concurrency // add up all concurrency here because it can be specified on a per-worker basis.
		busy += len(w.inProgressMessages())

		w.runnersLock.Lock()
		for _, r := range w.runners {
			taskRunnerInfo := storage.TaskRunnerInfo{
				Pid:             pid,
				Tid:             r.tid,
				Queue:           w.queue,
				InProgressQueue: w.inProgressQueue,
			}
			msg := r.inProgressMessage()
			if msg != nil {
				workerMsg := &HeartbeatWorkerMsg{
					Retry:      1,
					Queue:      w.queue,
					Backtrace:  false,
					Class:      msg.Class(),
					Args:       msg.Args(),
					Jid:        msg.Jid(),
					CreatedAt:  msg.startedAt, // not actually started at
					EnqueuedAt: heartbeatTime.Unix(),
				}

				jsonMsg, err := json.Marshal(workerMsg)
				if err != nil {
					return nil, err
				}

				msgWrapper := &HeartbeatWorkerMsgWrapper{
					Tid:     r.tid,
					Queue:   w.queue,
					Payload: string(jsonMsg),
					RunAt:   msg.startedAt,
				}

				jsonMsgWrapper, err := json.Marshal(msgWrapper)
				if err != nil {
					return nil, err
				}

				taskRunnerInfo.WorkerMsg = string(jsonMsgWrapper)
			}
			taskRunnersInfo = append(taskRunnersInfo, taskRunnerInfo)
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
		Identity:        heartbeatID,
		Beat:            heartbeatTime,
		Quiet:           false,
		Busy:            busy,
		RSS:             0, // rss is not currently supported
		Info:            string(heartbeatInfoJson),
		Pid:             pid,
		ActiveManager:   m.IsActive(),
		TaskRunnersInfo: taskRunnersInfo,
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
