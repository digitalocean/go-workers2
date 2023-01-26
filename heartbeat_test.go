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
	mgr, err := newTestManager(opts, true)
	assert.NoError(t, err)

	mgr.AddWorker("somequeue", 5, func(m *Msg) error {
		return nil
	})

	mgr.AddWorker("second_queue", 10, func(m *Msg) error {
		return nil
	})

	heartbeat, err := mgr.buildHeartbeat(time.Now().UTC(), time.Second)
	assert.Nil(t, err)

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

func TestBuildHeartbeatWorkerMessage(t *testing.T) {
	namespace := "prod"
	opts := testOptionsWithNamespace(namespace)
	mgr, err := newTestManager(opts, true)
	assert.NoError(t, err)

	mgr.AddWorker("somequeue", 1, func(m *Msg) error {
		return nil
	})

	msg, err := NewMsg("{\"class\":\"MyWorker\",\"jid\":\"jid-123\"}")

	testLogger := log.New(os.Stdout, "test-go-workers2: ", log.Ldate|log.Lmicroseconds)

	tr := newTaskRunner(testLogger, func(m *Msg) error {
		return nil
	})

	tr.currentMsg = msg

	firstWorker := mgr.workers[0]
	firstWorker.runners = []*taskRunner{tr}

	heartbeat, err := mgr.buildHeartbeat(time.Now().UTC(), time.Second)
	assert.Nil(t, err)

	assert.Equal(t, 1, len(heartbeat.WorkerHeartbeats))

	var decodedWorkerMsgWrapper map[string]interface{}
	for _, v := range heartbeat.WorkerHeartbeats {
		err = json.Unmarshal([]byte(v.WorkerMsg), &decodedWorkerMsgWrapper)
		assert.Nil(t, err)
	}

	var decodedWorkerMsgPayload map[string]interface{}

	err = json.Unmarshal([]byte(decodedWorkerMsgWrapper["payload"].(string)), &decodedWorkerMsgPayload)

	assert.Equal(t, "somequeue", decodedWorkerMsgWrapper["Queue"])
	assert.Equal(t, "MyWorker", decodedWorkerMsgPayload["class"])
}
