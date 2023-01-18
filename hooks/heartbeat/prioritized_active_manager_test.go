package heartbeat

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	workers "github.com/digitalocean/go-workers2"
	pstorage "github.com/digitalocean/go-workers2/storage"
)

func newTestManager(opts workers.Options) (*workers.Manager, error) {
	mgr, err := workers.NewManager(opts)
	return mgr, err
}

type testManagerConfig struct {
	prioritizedActiveManager *PrioritizedActiveManager
	callCounter              *workers.CallCounter
	managerPriority          int64
	waitGroup                sync.WaitGroup
	assertHeartbeat          chan bool
	assertedHeartbeat        bool
	assertedActivate         bool
}

func TestManager_Run_PrioritizedActiveManager(t *testing.T) {
	namespace := "mgrruntest"
	var managerConfigs []*testManagerConfig
	totalManagers := 6
	// initialize managers
	for i := 0; i < totalManagers; i++ {
		opts := workers.SetupDefaultTestOptionsWithHeartbeat(namespace, fmt.Sprintf("process%d", i))
		opts.ManagerStartInactive = true
		manager, err := newTestManager(opts)
		if i == 0 {
			// flush db before initializing first manager
			manager.Opts().Client().FlushDB(context.Background())
		}
		assert.NoError(t, err)
		mgrqcc := workers.NewCallCounter()
		manager.AddWorker("testqueue", 3, mgrqcc.F, workers.NopMiddleware)
		// half of the managers will be active based on priority
		prioritizedActiveManager := NewPrioritizedActiveManager(manager, i, totalManagers/2)

		managerConfig := &testManagerConfig{
			prioritizedActiveManager: prioritizedActiveManager,
			callCounter:              mgrqcc,
			managerPriority:          int64(i),
			assertHeartbeat:          make(chan bool),
			assertedHeartbeat:        false,
		}

		prioritizedActiveManager.Manager().AddAfterHeartbeatHooks(func(heartbeat *pstorage.Heartbeat, manager *workers.Manager, requeuedTaskRunnersStatus []*pstorage.StaleMessageUpdate) error {
			if !managerConfig.assertedHeartbeat {
				managerConfig.assertHeartbeat <- true
			}
			return nil
		})

		go func() {
			managerConfig.waitGroup.Add(1)
			managerConfig.prioritizedActiveManager.Manager().Run()
			managerConfig.waitGroup.Done()
		}()
		managerConfigs = append(managerConfigs, managerConfig)
	}

	// synchronize all managers have had a heartbeat
	for i := 0; i < totalManagers; i++ {
		managerConfigs[i].assertedHeartbeat = <-managerConfigs[i].assertHeartbeat
		assert.True(t, managerConfigs[i].assertedHeartbeat)
	}

	time.Sleep(managerConfigs[0].prioritizedActiveManager.Manager().Opts().Heartbeat.Interval * 2)

	// verify managers 0 to 2 are inactive and 1 to 5 are active
	for i := 0; i < totalManagers; i++ {
		if i < totalManagers/2 {
			assert.False(t, managerConfigs[i].prioritizedActiveManager.Manager().IsActive())
		} else {
			// higher priority managers are activated
			assert.True(t, managerConfigs[i].prioritizedActiveManager.Manager().IsActive())
		}
	}

	// stop all the active highest priority managers
	for i := totalManagers / 2; i < totalManagers; i++ {
		managerConfigs[i].prioritizedActiveManager.Manager().Stop()
		managerConfigs[i].waitGroup.Wait()
	}

	time.Sleep(managerConfigs[0].prioritizedActiveManager.Manager().Opts().Heartbeat.HeartbeatTTL * 2)

	// the lowest priority managers will activate
	for i := 0; i < totalManagers/2; i++ {
		assert.True(t, managerConfigs[i].prioritizedActiveManager.Manager().IsActive())
	}
}
