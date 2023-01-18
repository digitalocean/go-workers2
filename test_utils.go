package workers

import (
	"context"
	"time"
)

const (
	testServerAddr = "localhost:6379"
	testDatabase   = 15
)

// SetupDefaultTestOptions will setup processed default test options without a namespace
func SetupDefaultTestOptions() (Options, error) {
	return SetupDefaultTestOptionsWithNamespace("")
}

// SetupDefaultTestOptionsWithNamespace sets up processed default test options with namespace and flushes redis
// if client is configured
func SetupDefaultTestOptionsWithNamespace(namespace string) (Options, error) {
	opts, err := processOptions(testOptionsWithNamespace(namespace))
	if opts.client != nil {
		_, err = opts.client.FlushDB(context.Background()).Result()
		if err != nil {
			return Options{}, err
		}
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

// SetupDefaultTestOptionsWithHeartbeat creates default options for testing heartbeat related features
func SetupDefaultTestOptionsWithHeartbeat(namespace, processID string) Options {
	return Options{
		ServerAddr: testServerAddr,
		ProcessID:  processID,
		Database:   testDatabase,
		PoolSize:   1,
		Namespace:  namespace,
		Heartbeat: &HeartbeatOptions{
			Interval:     2 * time.Second,
			HeartbeatTTL: 6 * time.Second,
		},
	}
}

// CallCounter counts and synchronizes calls
type CallCounter struct {
	count     int
	syncCh    chan *Msg
	ackSyncCh chan bool
}

// NewCallCounter returns a new CallCounter
func NewCallCounter() *CallCounter {
	return &CallCounter{
		syncCh:    make(chan *Msg),
		ackSyncCh: make(chan bool),
	}
}

func (j *CallCounter) getOpt(m *Msg, opt string) bool {
	if m == nil {
		return false
	}
	return m.Args().GetIndex(0).Get(opt).MustBool()
}

func (j *CallCounter) F(m *Msg) error {
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

func (j *CallCounter) syncMsg() *Msg {
	m, _ := NewMsg(`{"args": [{"sync": true}]}`)
	return m
}

func (j *CallCounter) msg() *Msg {
	m, _ := NewMsg(`{"args": []}`)
	return m
}

func (j *CallCounter) noAckMsg() *Msg {
	m, _ := NewMsg(`{"args": [{"noack": true}]}`)
	return m
}
