package workers

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
)

func TestBuildHeartbeat(t *testing.T) {

	namespace := "prod"
	opts := testOptionsWithNamespace(namespace)
	mgr, err := newTestManager(opts)
	assert.NoError(t, err)

	mgr.AddWorker("somequeue", 5, func(m *Msg) error {
		// handlerCalled = true
		return nil
	})

	heartbeat := mgr.buildHeartbeat()

	hostname, _ := os.Hostname()

	info := &HeartbeatInfo{}

	err = json.Unmarshal([]byte(heartbeat.Info), info)
	assert.Nil(t, err)

	assert.Equal(t, hostname, info.Hostname)
	assert.Equal(t, "prod", info.Tag)
	assert.ElementsMatch(t, []string{"somequeue"}, info.Queues)
	assert.Equal(t, 5, info.Concurrency)
	assert.Equal(t, []string{}, info.Labels)

	assert.Equal(t, false, heartbeat.Quiet)
	assert.Equal(t)

	log.Println(heartbeat)

	// assert.Equal(heartbeat)

}
