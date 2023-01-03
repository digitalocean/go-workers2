package workers

import (
	"context"
	"crypto/tls"
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

func TestManager_InactiveManagerNoMessageProcessing(t *testing.T) {
	namespace := "mgrruntest"
	opts := testHeartbeatOptionsWithProcess(namespace, "1")
	mgr, err := newTestManager(opts, true)
	assert.NoError(t, err)
	mgr.active = false
	q1cc := newCallCounter()
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

func TestManager_Run_HeartbeatHandlesExpiredInProgressMessages(t *testing.T) {
	namespace := "mgrruntest"
	opts1 := testHeartbeatOptionsWithProcess(namespace, "1")
	mgr1, err := newTestManager(opts1, true)
	assert.NoError(t, err)
	prod1 := mgr1.Producer()

	mgr1qcc := newCallCounter()
	mgr1.AddWorker("testqueue", 3, mgr1qcc.F, NopMiddleware)

	assertMgr1HeartbeatTimeoutDuration := mgr1.opts.Heartbeat.Interval * 3
	pollMgr1StartTime, err := mgr1.opts.store.GetTime(context.Background())
	assert.NoError(t, err)
	assertMgr1Heartbeat := false
	mgr1.AddAfterHeartbeatHooks(mgr1.debugLogHeartbeat)
	mgr1.AddAfterHeartbeatHooks(func(heartbeat *storage.Heartbeat, updateActiveCluster *updateActiveClusterStatus, requeuedTaskRunnersStatus []requeuedTaskRunnerStatus) {
		if !assertMgr1Heartbeat && heartbeat.Beat.Sub(pollMgr1StartTime) > assertMgr1HeartbeatTimeoutDuration {
			assert.Fail(t, "mgr1 heartbeat timed out")
		}
		if len(heartbeat.TaskRunnersInfo) > 0 {
			assertMgr1Heartbeat = true
		}
		if len(requeuedTaskRunnersStatus) > 0 {
			assert.Fail(t, "expiring in manager 1")
		}
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

	// 1 left in in-progress queue, wait for heartbeat and stop manager
	for !assertMgr1Heartbeat {
		time.Sleep(time.Millisecond)
	}
	mgr1.Stop()

	// we update processID to guarantee using different in-progress queues from mgr1
	opts2 := testHeartbeatOptionsWithProcess(namespace, "2")
	mgr2, err := newTestManager(opts2, false)
	assert.NoError(t, err)
	mgr2qcc := newCallCounter()
	// demonstrate implementation does not care for different concurrency levels by changing concurrency from 3 to 2
	mgr2.AddWorker("testqueue", 2, mgr2qcc.F, NopMiddleware)
	pollMgr2StartTime, err := mgr2.opts.store.GetTime(context.Background())
	assert.NoError(t, err)
	assertMessageRequeued := make(chan bool)
	mgr2.AddAfterHeartbeatHooks(mgr2.debugLogHeartbeat)
	mgr2.AddAfterHeartbeatHooks(func(heartbeat *storage.Heartbeat, updateActiveCluster *updateActiveClusterStatus, requeuedTaskRunnersStatus []requeuedTaskRunnerStatus) {
		if heartbeat.Beat.Sub(pollMgr2StartTime) > mgr2.opts.Heartbeat.TaskRunnerEvictInterval*2 {
			assert.Fail(t, "mgr2 timed out polling for requeued stale task runner")
			assertMessageRequeued <- false
		}
		if len(requeuedTaskRunnersStatus) > 0 {
			for _, requeuedTaskRunnerStatus := range requeuedTaskRunnersStatus {
				assert.Equal(t, "testqueue", requeuedTaskRunnerStatus.queue)
				assert.Contains(t, requeuedTaskRunnerStatus.inprogressQueue, "testqueue")
				assert.Contains(t, requeuedTaskRunnerStatus.inprogressQueue, "inprogress")
				if len(requeuedTaskRunnerStatus.requeuedMsgs) > 0 {
					// check if it has requeued messages, as heartbeat may have expired the other 2 task runners
					// without messages instead
					assert.Equal(t, 1, len(requeuedTaskRunnersStatus[0].requeuedMsgs))
					<-mgr2qcc.syncCh
					ipm := mgr2.inProgressMessages()
					assert.Contains(t, ipm, "testqueue")
					requeuedMsg, err := NewMsg(requeuedTaskRunnersStatus[0].requeuedMsgs[0])
					assert.NoError(t, err)
					// verify requeued message from manager 1 is now in progress of being processed by manager 2
					assert.Equal(t, requeuedMsg.Jid(), ipm["testqueue"][0].Jid())
					assert.NoError(t, err)
					assertMessageRequeued <- true
				}
			}
		}
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

func TestManager_Run_HeartbeatHandlesActivatingClusterManager(t *testing.T) {
	namespace := "mgrruntest"
	// mgr1 is higher priority than mgr2
	opts1 := testHeartbeatOptionsWithCluster(namespace, "process1", "cluster1", 1)
	mgr1, err := newTestManager(opts1, true)
	assert.NoError(t, err)

	mgr1qcc := newCallCounter()
	mgr1.AddWorker("testqueue", 3, mgr1qcc.F, NopMiddleware)
	assertedMgr1Heartbeat := false
	assertMgr1Heartbeat := make(chan bool)
	mgr1.AddAfterHeartbeatHooks(mgr1.debugLogHeartbeat)
	//var mgr1HeartbeatTime time.Time
	mgr1.AddAfterHeartbeatHooks(func(heartbeat *storage.Heartbeat, updateActiveCluster *updateActiveClusterStatus, requeuedTaskRunnersStatus []requeuedTaskRunnerStatus) {
		if !assertedMgr1Heartbeat {
			assertMgr1Heartbeat <- true
		}
	})

	var wg1 sync.WaitGroup
	go func() {
		wg1.Add(1)
		mgr1.Run()
		wg1.Done()
	}()

	opts2 := testHeartbeatOptionsWithCluster(namespace, "process2", "cluster2", 99)
	mgr2, err := newTestManager(opts2, false)
	assert.NoError(t, err)

	assertedMgr1Heartbeat = <-assertMgr1Heartbeat

	mgr2qcc := newCallCounter()
	mgr2.AddWorker("testqueue", 2, mgr2qcc.F, NopMiddleware)
	assertMgr2Heartbeat := make(chan bool)
	assertCluster2Activated := make(chan bool)
	mgr2.AddAfterHeartbeatHooks(mgr2.debugLogHeartbeat)
	assertedMgr2Heartbeat := false
	mgr2.AddAfterHeartbeatHooks(func(heartbeat *storage.Heartbeat, updateActiveCluster *updateActiveClusterStatus, requeuedTaskRunnersStatus []requeuedTaskRunnerStatus) {
		if !assertedMgr2Heartbeat {
			assertMgr2Heartbeat <- true
		}
		if updateActiveCluster.activateManager {
			assertCluster2Activated <- true
		}
	})

	var wg2 sync.WaitGroup
	go func() {
		wg2.Add(1)
		mgr2.Run()
		wg2.Done()
	}()

	assertedMgr2Heartbeat = <-assertMgr2Heartbeat
	// assert manager 1, which belongs to higher priority cluster (lower priority number), is active
	assert.True(t, mgr1.IsActive())
	assert.False(t, mgr2.IsActive())

	mgr1.Stop()
	wg1.Wait()

	// cluster2 will activate once cluster1 is no longer considered active and is evicted
	<-assertCluster2Activated

	// after manager1 which belongs to cluster1 stopped for longer than cluster evict interval,
	// manager2 which belongs to cluster2 is now active
	assert.True(t, mgr2.IsActive())
	mgr2.Stop()
	wg2.Wait()
}
