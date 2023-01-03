package workers

import (
	"context"
	"time"
)

const (
	testServerAddr = "localhost:6379"
	testDatabase   = 15
)

func setupTestOptions() (Options, error) {
	return setupTestOptionsWithNamespace("")
}

func setupTestOptionsWithNamespace(namespace string) (Options, error) {
	opts, err := processOptions(testOptionsWithNamespace(namespace))
	if opts.client != nil {
		opts.client.FlushDB(context.Background()).Result()
	}
	return opts, err
}

func testOptionsWithNamespace(namespace string) Options {
	return Options{
		ServerAddr: testServerAddr,
		ProcessID:  "1",
		Database:   testDatabase,
		PoolSize:   1,
		Namespace:  namespace,
	}
}

func testHeartbeatOptionsWithProcess(namespace, processID string) Options {
	return Options{
		ServerAddr:   testServerAddr,
		ProcessID:    processID,
		Database:     testDatabase,
		PoolSize:     1,
		Namespace:    namespace,
		PollInterval: time.Second,
		Heartbeat: &HeartbeatOptions{
			Interval:                2 * time.Second,
			TaskRunnerEvictInterval: 8 * time.Second,
		},
	}
}

func testHeartbeatOptionsWithCluster(namespace, processID, clusterID string, clusterPriority float64) Options {
	return Options{
		ServerAddr: testServerAddr,
		ProcessID:  processID,
		Database:   testDatabase,
		PoolSize:   1,
		Namespace:  namespace,
		Heartbeat: &HeartbeatOptions{
			Interval:                2 * time.Second,
			TaskRunnerEvictInterval: 6 * time.Second,
			ClusterEvictInterval:    6 * time.Second,
		},
		ActivePassiveFailover: &ActivePassFailoverOptions{
			ClusterID:       clusterID,
			ClusterPriority: clusterPriority,
		},
	}
}

type callCounter struct {
	count     int
	syncCh    chan *Msg
	ackSyncCh chan bool
}

func newCallCounter() *callCounter {
	return &callCounter{
		syncCh:    make(chan *Msg),
		ackSyncCh: make(chan bool),
	}
}

func newCallCounterWithMessageProcessTime(messageProcessTime time.Duration) *callCounter {
	return &callCounter{
		syncCh:    make(chan *Msg),
		ackSyncCh: make(chan bool),
	}
}

func (j *callCounter) getOpt(m *Msg, opt string) bool {
	if m == nil {
		return false
	}
	return m.Args().GetIndex(0).Get(opt).MustBool()
}

func (j *callCounter) F(m *Msg) error {
	j.count++
	if m != nil {
		if j.getOpt(m, "sync") {
			j.syncCh <- m
			<-j.ackSyncCh
		}
		m.ack = !j.getOpt(m, "noack")
	}
	return nil
}

func (j *callCounter) syncMsg() *Msg {
	m, _ := NewMsg(`{"args": [{"sync": true}]}`)
	return m
}

func (j *callCounter) msg() *Msg {
	m, _ := NewMsg(`{"args": []}`)
	return m
}

func (j *callCounter) noAckMsg() *Msg {
	m, _ := NewMsg(`{"args": [{"noack": true}]}`)
	return m
}
