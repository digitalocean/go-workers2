package workers

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/digitalocean/go-workers2/storage"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func newTestManager(opts Options, flushDB bool) (*Manager, error) {
	mgr, err := NewManager(opts)
	if mgr != nil && flushDB {
		mgr.opts.client.FlushDB(context.Background()).Result()
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

func TestNewManagerWithRedisClient(t *testing.T) {
	namespace := "prod"
	opts := Options{
		ProcessID: "1",
		Namespace: namespace,
	}

	client := redis.NewClient(&redis.Options{
		IdleTimeout: 1,
		Password:    "ab",
		DB:          2,
		TLSConfig:   &tls.Config{ServerName: "test_tls2"},
	})

	mgr, err := NewManagerWithRedisClient(opts, client)

	assert.NoError(t, err)
	assert.NotEmpty(t, mgr.uuid)
	assert.Equal(t, namespace+":", mgr.opts.Namespace)

	assert.NotNil(t, mgr.GetRedisClient())
	assert.NotNil(t, mgr.GetRedisClient().Options().TLSConfig)
	assert.Equal(t, "test_tls2", mgr.GetRedisClient().Options().TLSConfig.ServerName)
}

func TestNewManagerWithRedisClientNoProcessID(t *testing.T) {
	namespace := "prod"
	opts := Options{
		Namespace: namespace,
	}

	client := redis.NewClient(&redis.Options{
		IdleTimeout: 1,
		Password:    "ab",
		DB:          2,
		TLSConfig:   &tls.Config{ServerName: "test_tls2"},
	})

	mgr, err := NewManagerWithRedisClient(opts, client)

	assert.Error(t, err)
	assert.Nil(t, mgr)
}

func TestManager_AddBeforeStartHooks(t *testing.T) {
	namespace := "prod"
	opts := testOptionsWithNamespace(namespace)
	opts.PollInterval = time.Second
	mgr, err := newTestManager(opts, true)
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
	opts.PollInterval = time.Second
	mgr, err := newTestManager(opts, true)
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
	opts.PollInterval = time.Second
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

func TestManager_Run(t *testing.T) {
	namespace := "mgrruntest"
	opts := testOptionsWithNamespace(namespace)
	opts.PollInterval = time.Second
	mgr, err := newTestManager(opts, true)
	assert.NoError(t, err)
	prod := mgr.Producer()

	q1cc := NewCallCounter()
	q2cc := NewCallCounter()
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
	assert.Contains(t, globalAPIServer.managers, mgr.uuid)

	// Test that it runs a scheduledWorker
	_, err = prod.EnqueueIn("queue1", "any", 2, q1cc.syncMsg().Args().Interface())
	assert.NoError(t, err)
	// This channel read will timeout the test if the scheduled message doesn't process
	<-q1cc.syncCh
	q1cc.ackSyncCh <- true

	mgr.Stop()
	wg.Wait()

	// Test that the manager is deregistered from the stats server
	assert.NotContains(t, globalAPIServer.managers, mgr.uuid)

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
	assert.Contains(t, globalAPIServer.managers, mgr.uuid)

	mgr.Stop()
	wg.Wait()

}

func TestManager_inProgressMessages(t *testing.T) {
	namespace := "mgrruntest"
	opts := testOptionsWithNamespace(namespace)
	opts.PollInterval = time.Second
	mgr, err := newTestManager(opts, true)
	assert.NoError(t, err)
	prod, err := NewProducer(opts)
	assert.NoError(t, err)

	q1cc := NewCallCounter()
	q2cc := NewCallCounter()
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

func TestManager_InactiveManagerNoMessageProcessing(t *testing.T) {
	namespace := "mgrruntest"
	opts := SetupDefaultTestOptionsWithHeartbeat(namespace, "1")
	mgr, err := newTestManager(opts, true)
	assert.NoError(t, err)
	mgr.active = false
	q1cc := NewCallCounter()
	mgr.AddWorker("queue1", 1, q1cc.F, NopMiddleware)

	var wg sync.WaitGroup
	go func() {
		wg.Add(1)
		mgr.Run()
		wg.Done()
	}()
	prod, err := NewProducer(opts)
	assert.NoError(t, err)
	_, err = prod.Enqueue("queue1", "any", q1cc.syncMsg().Args().Interface())
	assert.NoError(t, err)
	time.Sleep(2 * time.Second)
	ipm := mgr.inProgressMessages()
	assert.Len(t, ipm, 1)
	assert.Contains(t, ipm, "queue1")
	// message does not get to inprogress queue since it is never picked up by the inactive manager
	assert.Len(t, ipm["queue1"], 0)
	mgr.Stop()
	wg.Wait()
}

func TestManager_Run_HeartbeatHandlesStaleInProgressMessages(t *testing.T) {
	namespace := "mgrruntest"
	opts1 := SetupDefaultTestOptionsWithHeartbeat(namespace, "1")
	mgr1, err := newTestManager(opts1, true)
	assert.NoError(t, err)
	prod1 := mgr1.Producer()

	mgr1qcc := NewCallCounter()
	mgr1.AddWorker("testqueue", 3, mgr1qcc.F, NopMiddleware)

	assertMgr1HeartbeatTimeoutDuration := mgr1.opts.Heartbeat.Interval * 3
	pollMgr1StartTime, err := mgr1.opts.store.GetTime(context.Background())
	assert.NoError(t, err)
	assertMgr1Heartbeat := false
	mgr1.AddAfterHeartbeatHooks(func(heartbeat *storage.Heartbeat, manager *Manager, staleMessageUpdates []*storage.StaleMessageUpdate) error {
		heartbeatTime := time.Unix(heartbeat.Beat, 0)
		if !assertMgr1Heartbeat && heartbeatTime.Sub(pollMgr1StartTime) > assertMgr1HeartbeatTimeoutDuration {
			assert.Fail(t, "mgr1 heartbeat timed out")
		}
		if len(heartbeat.WorkerHeartbeats) > 0 {
			assertMgr1Heartbeat = true
		}
		if len(staleMessageUpdates) > 0 {
			assert.Fail(t, "expiring in manager 1")
		}
		return nil
	})

	var wg1 sync.WaitGroup
	go func() {
		wg1.Add(1)
		mgr1.Run()
		wg1.Done()
	}()

	// put 3 messages in queue1 and into in-progress queues
	_, err = prod1.Enqueue("testqueue", "any", mgr1qcc.syncMsg().Args().Interface())
	assert.NoError(t, err)
	<-mgr1qcc.syncCh
	_, err = prod1.Enqueue("testqueue", "any", mgr1qcc.syncMsg().Args().Interface())
	assert.NoError(t, err)
	<-mgr1qcc.syncCh
	_, err = prod1.Enqueue("testqueue", "any", mgr1qcc.syncMsg().Args().Interface())
	assert.NoError(t, err)
	<-mgr1qcc.syncCh

	// release 2 of the messages in-progress queues
	mgr1qcc.ackSyncCh <- true
	mgr1qcc.ackSyncCh <- true

	// 1 left in in-progress Queue, wait for heartbeat and stop manager
	for !assertMgr1Heartbeat {
		time.Sleep(time.Millisecond)
	}
	mgr1.Stop()

	// we update processID to guarantee using different in-progress queues from mgr1
	opts2 := SetupDefaultTestOptionsWithHeartbeat(namespace, "2")
	mgr2, err := newTestManager(opts2, false)
	assert.NoError(t, err)
	mgr2qcc := NewCallCounter()
	// demonstrate implementation does not care for different concurrency levels by changing concurrency from 3 to 2
	mgr2.AddWorker("testqueue", 2, mgr2qcc.F, NopMiddleware)
	pollMgr2StartTime, err := mgr2.opts.store.GetTime(context.Background())
	assert.NoError(t, err)
	assertMessageRequeued := make(chan bool)
	mgr2.AddAfterHeartbeatHooks(func(heartbeat *storage.Heartbeat, manager *Manager, staleMessageUpdates []*storage.StaleMessageUpdate) error {
		heartbeatTime := time.Unix(heartbeat.Beat, 0)
		if heartbeatTime.Sub(pollMgr2StartTime) > mgr2.opts.Heartbeat.HeartbeatTTL*2 {
			assert.Fail(t, "mgr2 timed out polling for requeued stale task runner")
			assertMessageRequeued <- false
		}
		if len(staleMessageUpdates) > 0 {
			for _, staleMessageUpdate := range staleMessageUpdates {
				assert.Equal(t, "testqueue", staleMessageUpdate.Queue)
				assert.Contains(t, staleMessageUpdate.InprogressQueue, "testqueue")
				assert.Contains(t, staleMessageUpdate.InprogressQueue, "inprogress")
				if len(staleMessageUpdate.RequeuedMsgs) > 0 {
					// check if it has requeued messages, as heartbeat may have expired the other 2 task runners
					// without messages instead
					assert.Equal(t, 1, len(staleMessageUpdates[0].RequeuedMsgs))
					<-mgr2qcc.syncCh
					ipm := mgr2.inProgressMessages()
					assert.Contains(t, ipm, "testqueue")
					requeuedMsg, err := NewMsg(staleMessageUpdates[0].RequeuedMsgs[0])
					assert.NoError(t, err)
					// verify requeued message from manager 1 is now in progress of being processed by manager 2
					assert.Equal(t, requeuedMsg.Jid(), ipm["testqueue"][0].Jid())
					assert.NoError(t, err)
					assertMessageRequeued <- true
				}
			}
		}
		return nil
	})
	var wg2 sync.WaitGroup
	go func() {
		wg2.Add(1)
		mgr2.Run()
		wg2.Done()
	}()
	// process requeued message in manager2
	isRequeud := <-assertMessageRequeued
	assert.True(t, isRequeud)
	// testing complete as manager2 picked up the originally in-progress message from manager1
	// signal dummy acks for pending in-progress messages to satisfy waitgroups
	mgr1qcc.ackSyncCh <- true
	mgr2qcc.ackSyncCh <- true

	mgr2.Stop()
	wg1.Wait()
	wg2.Wait()
}

type testPrioritizedActiveManagerConfig struct {
	manager           *Manager
	callCounter       *CallCounter
	managerPriority   int64
	waitGroup         sync.WaitGroup
	assertHeartbeat   chan bool
	assertedHeartbeat bool
	assertedActivate  bool
}

func TestManager_Run_PrioritizedActiveManager(t *testing.T) {
	namespace := "mgrruntest"
	var managerConfigs []*testPrioritizedActiveManagerConfig
	totalManagers := 6
	totalActiveManagers := totalManagers / 2
	// initialize managers
	for i := 0; i < totalManagers; i++ {
		opts := SetupDefaultTestOptionsWithHeartbeat(namespace, fmt.Sprintf("process%d", i))
		opts.ManagerStartInactive = true
		// half of the managers will be active based on priority
		opts.Heartbeat.PrioritizedManager = &PrioritizedManagerOptions{
			TotalActiveManagers: totalActiveManagers,
			ManagerPriority:     i,
		}
		flushDB := i == 0
		manager, err := newTestManager(opts, flushDB)
		assert.NoError(t, err)
		mgrqcc := NewCallCounter()
		manager.AddWorker("testqueue", 3, mgrqcc.F, NopMiddleware)

		managerConfig := &testPrioritizedActiveManagerConfig{
			manager:           manager,
			callCounter:       mgrqcc,
			managerPriority:   int64(i),
			assertHeartbeat:   make(chan bool),
			assertedHeartbeat: false,
		}

		manager.AddAfterHeartbeatHooks(func(heartbeat *storage.Heartbeat, manager *Manager, requeuedTaskRunnersStatus []*storage.StaleMessageUpdate) error {
			if !managerConfig.assertedHeartbeat {
				managerConfig.assertHeartbeat <- true
			}
			return nil
		})

		go func() {
			managerConfig.waitGroup.Add(1)
			managerConfig.manager.Run()
			managerConfig.waitGroup.Done()
		}()
		managerConfigs = append(managerConfigs, managerConfig)
	}

	// synchronize all managers have had a heartbeat
	for i := 0; i < totalManagers; i++ {
		managerConfigs[i].assertedHeartbeat = <-managerConfigs[i].assertHeartbeat
		assert.True(t, managerConfigs[i].assertedHeartbeat)
	}

	time.Sleep(managerConfigs[0].manager.Opts().Heartbeat.Interval * 2)

	// verify managers 0 to 2 are inactive and 1 to 5 are active
	for i := 0; i < totalManagers; i++ {
		if i < totalManagers/2 {
			assert.False(t, managerConfigs[i].manager.IsActive())
		} else {
			// higher priority managers are activated
			assert.True(t, managerConfigs[i].manager.IsActive())
		}
	}

	// stop all the active highest priority managers
	for i := totalManagers / 2; i < totalManagers; i++ {
		managerConfigs[i].manager.Stop()
		managerConfigs[i].waitGroup.Wait()
	}

	time.Sleep(managerConfigs[0].manager.Opts().Heartbeat.HeartbeatTTL * 2)

	// the lowest priority managers will activate
	for i := 0; i < totalManagers/2; i++ {
		assert.True(t, managerConfigs[i].manager.IsActive())
	}
}
