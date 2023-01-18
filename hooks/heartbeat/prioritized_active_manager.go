package heartbeat

import (
	"context"
	"log"
	"sort"
	"sync"

	"github.com/go-redis/redis/v8"

	workers "github.com/digitalocean/go-workers2"
	"github.com/digitalocean/go-workers2/hooks/heartbeat/storage"
	pstorage "github.com/digitalocean/go-workers2/storage"
)

// PrioritizedActiveManager is a struct which supports the hook for limiting active managers up to totalActiveManagers
// based on manager priority.
type PrioritizedActiveManager struct {
	lock sync.Mutex

	store  storage.Store
	logger *log.Logger

	manager         *workers.Manager
	managerPriority int

	totalActiveManagers int
}

// NewPrioritizedActiveManager initializes PrioritizedActiveManager and registers the necessary hooks into manager
// to allow prioritized active manager
func NewPrioritizedActiveManager(manager *workers.Manager, managerPriority, totalActiveManagers int) *PrioritizedActiveManager {
	prioritizedActiveManager := &PrioritizedActiveManager{
		store:               storage.NewRedisStore(manager.Opts().Namespace, manager.Opts().Client(), manager.Opts().Logger),
		logger:              manager.Opts().Logger,
		manager:             manager,
		managerPriority:     managerPriority,
		totalActiveManagers: totalActiveManagers,
	}
	// register prioritized active manager hooks
	manager.AddBeforeStartHooks(prioritizedActiveManager.beforeStartHook)
	manager.AddAfterHeartbeatHooks(prioritizedActiveManager.afterHeartbeatHook)
	return prioritizedActiveManager
}

func (p *PrioritizedActiveManager) Manager() *workers.Manager {
	return p.manager
}

func (p *PrioritizedActiveManager) beforeStartHook() {
	ctx := context.Background()
	p.store.SetTotalActiveManagers(ctx, p.totalActiveManagers)
}

func (p *PrioritizedActiveManager) afterHeartbeatHook(heartbeat *pstorage.Heartbeat, manager *workers.Manager, staleMessageUpdates []*pstorage.StaleMessageUpdate) error {
	ctx := context.Background()
	err := p.store.SetHeartbeatManagerPriority(ctx, heartbeat.Identity, p.managerPriority)
	if err != nil {
		return nil
	}
	heartbeatManagerPriorities, err := p.store.GetHeartbeatManagerPriorities(ctx)
	if len(heartbeatManagerPriorities) == 0 {
		return nil
	}
	// order heartbeatManagerPriorities by manager priority descending
	sort.Slice(heartbeatManagerPriorities, func(i, j int) bool {
		return heartbeatManagerPriorities[i].ManagerPriority > heartbeatManagerPriorities[j].ManagerPriority
	})

	totalActiveManagers, err := p.store.GetTotalActiveManagers(ctx)
	if err != nil && err != redis.Nil {
		return err
	}

	// if current manager's priority is high enough to be within total active manager threshold, set manager as active
	activeManager := false
	for i := 0; i < totalActiveManagers; i++ {
		if heartbeatManagerPriorities[i].HeartbeatID == heartbeat.Identity {
			activeManager = true
			break
		}
	}
	manager.Active(activeManager)
	return nil
}
