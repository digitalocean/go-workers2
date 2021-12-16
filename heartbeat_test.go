package workers

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
	"time"
)

func TestBuildHeartbeat(t *testing.T) {
	namespace := "prod"
	opts := testOptionsWithNamespace(namespace)
	mgr, err := newTestManager(opts)
	assert.NoError(t, err)

	mgr.AddWorker("somequeue", 5, func(m *Msg) error {
		return nil
	})

	mgr.AddWorker("second_queue", 10, func(m *Msg) error {
		return nil
	})

	heartbeat := mgr.buildHeartbeat()

	hostname, _ := os.Hostname()

	info := &HeartbeatInfo{}

	err = json.Unmarshal([]byte(heartbeat.Info), info)
	assert.Nil(t, err)

	assert.Equal(t, hostname, info.Hostname)
	assert.Equal(t, "prod", info.Tag)
	assert.ElementsMatch(t, []string{"somequeue", "second_queue"}, info.Queues)
	assert.Equal(t, 15, info.Concurrency)
	assert.Equal(t, []string{}, info.Labels)

	assert.Equal(t, false, heartbeat.Quiet)
}

func TestBuildHeartbeatMsg(t *testing.T) {
	namespace := "prod"
	opts := testOptionsWithNamespace(namespace)
	mgr, err := newTestManager(opts)
	assert.NoError(t, err)

	mgr.AddWorker("somequeue", 1, func(m *Msg) error {
		return nil
	})

	startedAt := time.Now().UTC().Unix()

	workerMsg := &HeartbeatWorkerMsg{
		Retry:     1,
		Queue:     "somequeue",
		Backtrace: false,
		Class:     "",
		// Args:       "1",
		Jid:        "a",
		CreatedAt:  startedAt, // not actually started at
		EnqueuedAt: time.Now().UTC().Unix(),
	}
	jsonMsg, _ := json.Marshal(workerMsg)

	wrapper := &HeartbeatWorkerMsgWrapper{
		Queue:   "somequeue",
		Payload: string(jsonMsg),
		RunAt:   startedAt,
	}
	jsonWrapper, _ := json.Marshal(wrapper)
	log.Println(string(jsonWrapper))

	msg, err := NewMsg("{\"class\":\"MyWorker\",\"jid\":\"jid-123\"}")

	testLogger := log.New(os.Stdout, "test-go-workers2: ", log.Ldate|log.Lmicroseconds)

	tr := newTaskRunner(testLogger, func(m *Msg) error {
		return nil
	})

	tr.currentMsg = msg

	firstWorker := mgr.workers[0]
	firstWorker.runners = []*taskRunner{tr}

	heartbeat := mgr.buildHeartbeat()

	info := &HeartbeatInfo{}

	err = json.Unmarshal([]byte(heartbeat.Info), info)

	log.Println(heartbeat)

	// assert.Nil(t, err)

	// assert.Equal(t, hostname, info.Hostname)
	// assert.Equal(t, "prod", info.Tag)
	// assert.ElementsMatch(t, []string{"somequeue", "second_queue"}, info.Queues)
	// assert.Equal(t, 15, info.Concurrency)
	// assert.Equal(t, []string{}, info.Labels)

	// assert.Equal(t, false, heartbeat.Quiet)
	// assert.Equal(heartbeat)
}
