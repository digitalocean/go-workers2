package storage

import (
	"context"
	"log"
	"strconv"

	"github.com/go-redis/redis/v8"

	pstorage "github.com/digitalocean/go-workers2/storage"
)

type redisStore struct {
	namespace string
	client    *redis.Client
	logger    *log.Logger
}

type HeartbeatManagerPriority struct {
	HeartbeatID     string
	ManagerPriority int
}

const (
	managerKeyManagerPriorityKey = "manager_priority"
)

func NewRedisStore(namespace string, client *redis.Client, logger *log.Logger) Store {
	return &redisStore{
		namespace: namespace,
		client:    client,
		logger:    logger,
	}
}

func (r *redisStore) SetHeartbeatManagerPriority(ctx context.Context, heartbeatID string, managerPriority int) error {
	managerKey := pstorage.GetManagerKey(r.namespace, heartbeatID)
	_, err := r.client.HSet(ctx, managerKey, managerKeyManagerPriorityKey, managerPriority).Result()
	return err
}

func (r *redisStore) GetHeartbeatManagerPriorities(ctx context.Context) ([]*HeartbeatManagerPriority, error) {
	heartbeatIDs, err := r.client.SMembers(ctx, pstorage.GetProcessesKey(r.namespace)).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	var heartbeatManagerPriorities []*HeartbeatManagerPriority
	for _, heartbeatID := range heartbeatIDs {
		heartbeatManagerPriority, err := r.getHeartbeatManagerPriority(ctx, heartbeatID)
		if err != nil {
			return nil, err
		}
		heartbeatManagerPriorities = append(heartbeatManagerPriorities, heartbeatManagerPriority)
	}
	return heartbeatManagerPriorities, nil
}

func (r *redisStore) getHeartbeatManagerPriority(ctx context.Context, heartbeatID string) (*HeartbeatManagerPriority, error) {
	managerKey := pstorage.GetManagerKey(r.namespace, heartbeatID)
	heartbeatManagerPriority := &HeartbeatManagerPriority{
		HeartbeatID: heartbeatID,
	}
	managerPriorityStr, err := r.client.HGet(ctx, managerKey, managerKeyManagerPriorityKey).Result()
	if err != nil {
		return nil, err
	}
	if managerPriorityStr == "" {
		return heartbeatManagerPriority, nil
	}
	managerPriority, err := strconv.Atoi(managerPriorityStr)
	if err != nil {
		return nil, err
	}
	heartbeatManagerPriority.ManagerPriority = managerPriority
	return heartbeatManagerPriority, nil
}

func (r *redisStore) SetTotalActiveManagers(ctx context.Context, totalActiveManagers int) error {
	_, err := r.client.Set(ctx, totalActiveManagersKey(r.namespace), totalActiveManagers, 0).Result()
	return err
}

func (r *redisStore) GetTotalActiveManagers(ctx context.Context) (int, error) {
	totalActiveManagersStr, err := r.client.Get(ctx, totalActiveManagersKey(r.namespace)).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(totalActiveManagersStr)
}

func totalActiveManagersKey(namespace string) string {
	return namespace + "total-active-managers"
}
