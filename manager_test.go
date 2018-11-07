package workers

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func newTestManager(opts Options) (*Manager, error) {
	mgr, err := NewManager(opts)
	if mgr != nil {
		mgr.opts.client.FlushDB().Result()
	}
	return mgr, err
}

func TestNewManager(t *testing.T) {
	namespace := "prod"
	opts := testOptionsWithNamespace(namespace)
	mgr, err := NewManager(opts)
	assert.NoError(t, err)
	assert.NotEmpty(t, mgr.uuid)
	assert.Equal(t, namespace+":", mgr.opts.Namespace)
}

func TestManager_AddBeforeStartHooks(t *testing.T) {
	namespace := "prod"
	opts := testOptionsWithNamespace(namespace)
	opts.PollInterval = 1
	mgr, err := newTestManager(opts)
	assert.NoError(t, err)
	var beforeStartCalled int
	mgr.AddBeforeStartHooks(func() {
		beforeStartCalled++
	})
	ch := make(chan bool)
	go func() {
		mgr.Run()
		ch <- true
		mgr.Run()
		ch <- true
	}()
	time.Sleep(time.Second)
	assert.Equal(t, 1, beforeStartCalled)
	mgr.Stop()
	<-ch
	time.Sleep(time.Second)
	assert.Equal(t, 2, beforeStartCalled)
	mgr.Stop()
	<-ch
}

func TestManager_AddDuringDrainHooks(t *testing.T) {
	namespace := "prod"
	opts := testOptionsWithNamespace(namespace)
	opts.PollInterval = 1
	mgr, err := newTestManager(opts)
	assert.NoError(t, err)
	var duringDrainCalled int
	mgr.AddDuringDrainHooks(func() {
		duringDrainCalled++
	})
	ch := make(chan bool)
	go func() {
		mgr.Run()
		ch <- true
		mgr.Run()
		ch <- true
	}()
	time.Sleep(time.Second)
	mgr.Stop()
	assert.Equal(t, 1, duringDrainCalled)
	<-ch
	time.Sleep(time.Second)
	mgr.Stop()
	assert.Equal(t, 2, duringDrainCalled)
	<-ch
}

func TestManager_AddWorker(t *testing.T) {
	namespace := "prod"
	opts := testOptionsWithNamespace(namespace)
	opts.PollInterval = 1
	mgr, err := NewManager(opts)
	assert.NoError(t, err)

	var handlerCalled bool
	var defaultMidCalled bool

	baseMids := defaultMiddlewares
	defaultMiddlewares = NewMiddlewares(
		func(queue string, mgr *Manager, next JobFunc) JobFunc {
			return func(message *Msg) (result error) {
				defaultMidCalled = true
				result = next(message)
				return
			}
		},
	)
	mgr.AddWorker("someq", 1, func(m *Msg) error {
		handlerCalled = true
		return nil
	})
	assert.Len(t, mgr.workers, 1)
	assert.Equal(t, "someq", mgr.workers[0].queue)

	msg, _ := NewMsg("{}")

	mgr.workers[0].handler(msg)
	assert.True(t, defaultMidCalled)
	assert.True(t, handlerCalled)

	var midCalled bool

	mgr.workers = nil
	mgr.AddWorker("someq", 1, func(m *Msg) error {
		handlerCalled = true
		return nil
	}, func(queue string, mgr *Manager, next JobFunc) JobFunc {
		return func(message *Msg) (result error) {
			midCalled = true
			result = next(message)
			return
		}
	})

	defaultMidCalled = false
	handlerCalled = false

	mgr.workers[0].handler(msg)
	assert.False(t, defaultMidCalled)
	assert.True(t, midCalled)
	assert.True(t, handlerCalled)

	defaultMiddlewares = baseMids
}

func TestManager_RetryQueue(t *testing.T) {
	namespace := "prod"
	opts := testOptionsWithNamespace(namespace)
	mgr, err := newTestManager(opts)
	assert.NoError(t, err)
	assert.Equal(t, "prod:goretry", mgr.RetryQueue())
}

func TestManager_Run(t *testing.T) {
	namespace := "mgrruntest"
	opts := testOptionsWithNamespace(namespace)
	opts.PollInterval = 1
	mgr, err := newTestManager(opts)
	assert.NoError(t, err)
	prod := mgr.Producer()

	q1cc := newCallCounter()
	q2cc := newCallCounter()
	mgr.AddWorker("queue1", 1, q1cc.F, NopMiddleware)
	mgr.AddWorker("queue2", 2, q2cc.F, NopMiddleware)

	var wg sync.WaitGroup
	go func() {
		wg.Add(1)
		mgr.Run()
		wg.Done()
	}()

	// Test that messages process
	_, err = prod.Enqueue("queue1", "any", q1cc.syncMsg().Args().Interface())
	assert.NoError(t, err)
	// This channel read will timeout the test if messages don't process
	<-q1cc.syncCh
	q1cc.ackSyncCh <- true

	// Test that the manager is registered in the stats server
	assert.Contains(t, globalStatsServer.managers, mgr.uuid)

	// Test that it runs a scheduledWorker
	_, err = prod.EnqueueIn("queue1", "any", 2, q1cc.syncMsg().Args().Interface())
	assert.NoError(t, err)
	// This channel read will timeout the test if the scheduled message doesn't process
	<-q1cc.syncCh
	q1cc.ackSyncCh <- true

	mgr.Stop()
	wg.Wait()

	// Test that the manager is deregistered from the stats server
	assert.NotContains(t, globalStatsServer.managers, mgr.uuid)

	// Test that we can restart the manager
	go func() {
		wg.Add(1)
		mgr.Run()
		wg.Done()
	}()

	// Test that messages process
	_, err = prod.Enqueue("queue1", "any", q1cc.syncMsg().Args().Interface())
	assert.NoError(t, err)
	// This channel read will timeout the test if messages don't process, which
	// means the manager didn't restart
	<-q1cc.syncCh
	q1cc.ackSyncCh <- true

	// Test that we're back in the global stats server
	assert.Contains(t, globalStatsServer.managers, mgr.uuid)

	mgr.Stop()
	wg.Wait()

}

func TestManager_inProgressMessages(t *testing.T) {
	namespace := "mgrruntest"
	opts := testOptionsWithNamespace(namespace)
	opts.PollInterval = 1
	mgr, err := newTestManager(opts)
	assert.NoError(t, err)
	prod, err := NewProducer(opts)
	assert.NoError(t, err)

	q1cc := newCallCounter()
	q2cc := newCallCounter()
	mgr.AddWorker("queue1", 1, q1cc.F, NopMiddleware)
	mgr.AddWorker("queue2", 2, q2cc.F, NopMiddleware)

	var wg sync.WaitGroup
	go func() {
		wg.Add(1)
		mgr.Run()
		wg.Done()
	}()

	// None
	ipm := mgr.inProgressMessages()
	assert.Len(t, ipm, 2)
	assert.Contains(t, ipm, "queue1")
	assert.Contains(t, ipm, "queue2")
	assert.Empty(t, ipm["queue1"])
	assert.Empty(t, ipm["queue2"])

	// One in Queue1
	_, err = prod.Enqueue("queue1", "any", q1cc.syncMsg().Args().Interface())
	assert.NoError(t, err)
	<-q1cc.syncCh
	ipm = mgr.inProgressMessages()
	assert.Len(t, ipm, 2)
	assert.Contains(t, ipm, "queue1")
	assert.Contains(t, ipm, "queue2")
	assert.Len(t, ipm["queue1"], 1)
	assert.Empty(t, ipm["queue2"])

	// One in Queue2
	_, err = prod.Enqueue("queue2", "any", q2cc.syncMsg().Args().Interface())
	assert.NoError(t, err)
	<-q2cc.syncCh
	ipm = mgr.inProgressMessages()
	assert.Len(t, ipm, 2)
	assert.Contains(t, ipm, "queue1")
	assert.Contains(t, ipm, "queue2")
	assert.Len(t, ipm["queue1"], 1)
	assert.Len(t, ipm["queue2"], 1)

	// Another in Queue2
	_, err = prod.Enqueue("queue2", "any", q2cc.syncMsg().Args().Interface())
	assert.NoError(t, err)
	<-q2cc.syncCh
	ipm = mgr.inProgressMessages()
	assert.Len(t, ipm, 2)
	assert.Contains(t, ipm, "queue1")
	assert.Contains(t, ipm, "queue2")
	assert.Len(t, ipm["queue1"], 1)
	assert.Len(t, ipm["queue2"], 2)

	// Release two from Queue2
	q2cc.ackSyncCh <- true
	q2cc.ackSyncCh <- true

	time.Sleep(2 * time.Second)
	ipm = mgr.inProgressMessages()
	assert.Len(t, ipm, 2)
	assert.Contains(t, ipm, "queue1")
	assert.Contains(t, ipm, "queue2")
	assert.Len(t, ipm["queue1"], 1)
	assert.Len(t, ipm["queue2"], 0)

	// Release last from Queue1 - should have one left in queue2
	q1cc.ackSyncCh <- true
	time.Sleep(2 * time.Second)
	ipm = mgr.inProgressMessages()
	assert.Len(t, ipm, 2)
	assert.Contains(t, ipm, "queue1")
	assert.Contains(t, ipm, "queue2")
	assert.Len(t, ipm["queue1"], 0)
	assert.Len(t, ipm["queue2"], 0)

	mgr.Stop()
	wg.Wait()
}
