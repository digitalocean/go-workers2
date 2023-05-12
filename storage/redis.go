package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

type redisStore struct {
	namespace string

	client *redis.Client
	logger *log.Logger
}

// Compile-time check to ensure that Redis store does in fact implement the Store interface
var _ Store = &redisStore{}

// NewRedisStore returns a new Redis store with the given namespace and preconfigured client
func NewRedisStore(namespace string, client *redis.Client, logger *log.Logger) Store {
	return &redisStore{
		namespace: namespace,
		client:    client,
		logger:    logger,
	}
}

func (r *redisStore) DequeueMessage(ctx context.Context, queue string, inprogressQueue string, timeout time.Duration) (string, error) {
	message, err := r.client.BRPopLPush(ctx, r.getQueueName(queue), r.getQueueName(inprogressQueue), timeout).Result()

	if err != nil {
		// If redis returns null, the queue is empty.
		// Just ignore empty queue errors; print all other errors.
		if err != redis.Nil {
			r.logger.Println("ERR: ", queue, err)
		} else {
			err = NoMessage
		}

		time.Sleep(1 * time.Second)

		return "", err
	}

	return message, nil
}

func (r *redisStore) CheckRtt(ctx context.Context) int64 {
	start := time.Now()
	r.client.Ping(ctx)
	ellapsed := time.Since(start)

	return ellapsed.Microseconds()
}

func (r *redisStore) getHeartbeat(ctx context.Context, heartbeatID string) (*Heartbeat, error) {
	heartbeatProperties := []string{"beat", "quiet", "busy", "rtt_us", "rss", "info", "manager_priority", "active_manager"}
	booleanProperties := []string{"quiet", "active_manager"}
	managerKey := GetManagerKey(r.namespace, heartbeatID)
	heartbeatPropertyValues, err := r.client.HMGet(ctx, managerKey, heartbeatProperties...).Result()
	if err != nil {
		return nil, err
	}

	heartbeatStrMap := make(map[string]string)
	hasPropertyValue := false
	for i, heartbeatProperty := range heartbeatProperties {
		if heartbeatPropertyValues[i] != nil {
			heartbeatStrMap[heartbeatProperty] = heartbeatPropertyValues[i].(string)
			hasPropertyValue = true
		}
	}

	for _, booleanProperty := range booleanProperties {
		if heartbeatStrMap[booleanProperty] == "1" {
			heartbeatStrMap[booleanProperty] = "true"
		} else {
			heartbeatStrMap[booleanProperty] = "false"
		}
	}

	if !hasPropertyValue {
		return nil, nil
	}

	heartbeatJson, err := json.Marshal(heartbeatStrMap)
	if err != nil {
		return nil, err
	}

	heartbeat := Heartbeat{}
	err = json.Unmarshal(heartbeatJson, &heartbeat)
	if err != nil {
		return nil, err
	}
	heartbeat.Identity = heartbeatID
	return &heartbeat, nil
}

func (r *redisStore) GetAllHeartbeats(ctx context.Context) ([]*Heartbeat, error) {
	var heartbeats []*Heartbeat

	heartbeatIDs, err := r.getHeartbeatIDs(ctx)
	if len(heartbeatIDs) == 0 {
		return nil, err
	}
	for _, heartbeatID := range heartbeatIDs {
		heartbeat, err := r.getHeartbeat(ctx, heartbeatID)
		if err != nil {
			return nil, err
		}
		if heartbeat != nil {
			heartbeats = append(heartbeats, heartbeat)
		}
	}
	return heartbeats, nil
}

func (r *redisStore) getHeartbeatIDs(ctx context.Context) ([]string, error) {
	heartbeatIDs, err := r.client.SMembers(ctx, GetProcessesKey(r.namespace)).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	return heartbeatIDs, nil
}

func (r *redisStore) SendHeartbeat(ctx context.Context, heartbeat *Heartbeat) error {
	pipe := r.client.Pipeline()
	rtt := r.CheckRtt(ctx)

	managerKey := GetManagerKey(r.namespace, heartbeat.Identity)
	pipe.SAdd(ctx, GetProcessesKey(r.namespace), heartbeat.Identity) // add to the sidekiq processes set without the namespace

	workerHeartbeats, err := json.Marshal(heartbeat.WorkerHeartbeats)
	if err != nil {
		return err
	}

	pipe.HMSet(ctx, managerKey,
		"beat", heartbeat.Beat,
		"quiet", heartbeat.Quiet,
		"busy", heartbeat.Busy,
		"rtt_us", rtt,
		"rss", heartbeat.RSS,
		"info", heartbeat.Info,
		"manager_priority", heartbeat.ManagerPriority,
		"active_manager", heartbeat.ActiveManager,
		"worker_heartbeats", workerHeartbeats)

	_, err = pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return err
	}

	return nil
}

func (r *redisStore) HandleAllExpiredHeartbeats(ctx context.Context, expireTS int64) ([]*StaleMessageUpdate, error) {
	heartbeatIDs, err := r.client.SMembers(ctx, GetProcessesKey(r.namespace)).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}

	var staleMessageUpdates []*StaleMessageUpdate
	for _, heartbeatID := range heartbeatIDs {
		managerKey := GetManagerKey(r.namespace, heartbeatID)
		strHeartbeatTS, err := r.client.HGet(ctx, managerKey, "beat").Result()
		if err != nil && err != redis.Nil {
			return nil, err
		}
		heartbeatTS, err := strconv.Atoi(strHeartbeatTS)
		if err != nil {
			return nil, err
		}
		if int64(heartbeatTS) > expireTS {
			continue
		}
		_, err = r.client.SRem(ctx, GetProcessesKey(r.namespace), heartbeatID).Result()
		if err != nil {
			return nil, err
		}
		strWorkerHeartbeats, err := r.client.HGet(ctx, managerKey, "worker_heartbeats").Result()
		var workerHeartbeats []WorkerHeartbeat

		err = json.Unmarshal([]byte(strWorkerHeartbeats), &workerHeartbeats)
		if err != nil {
			return nil, err
		}

		// requeue worker in-progress queues back to the queues
		requeuedInProgressQueues := make(map[string]bool)
		for _, workerHeartbeat := range workerHeartbeats {
			var requeuedMsgs []string
			if _, exists := requeuedInProgressQueues[workerHeartbeat.InProgressQueue]; exists {
				continue
			}
			requeuedMsgs, err = r.RequeueMessagesFromInProgressQueue(ctx, workerHeartbeat.InProgressQueue, workerHeartbeat.Queue)
			if err != nil {
				return nil, err
			}
			requeuedInProgressQueues[workerHeartbeat.InProgressQueue] = true
			if len(requeuedMsgs) == 0 {
				continue
			}
			staleMessageUpdate := &StaleMessageUpdate{
				Queue:           workerHeartbeat.Queue,
				InprogressQueue: workerHeartbeat.InProgressQueue,
				RequeuedMsgs:    requeuedMsgs,
			}
			staleMessageUpdates = append(staleMessageUpdates, staleMessageUpdate)
		}

	}
	return staleMessageUpdates, nil
}

func (r *redisStore) getTaskRunnerID(pid int, tid string) string {
	return fmt.Sprintf("%d-%s", pid, tid)
}

func (r *redisStore) RequeueMessagesFromInProgressQueue(ctx context.Context, inprogressQueue, queue string) ([]string, error) {
	var requeuedMsgs []string
	for {
		msg, err := r.client.BRPopLPush(ctx, r.getQueueName(inprogressQueue), r.getQueueName(queue), 1*time.Second).Result()

		if err != nil {
			if err == redis.Nil {
				break
			}
			return requeuedMsgs, err
		}
		requeuedMsgs = append(requeuedMsgs, msg)
	}
	return requeuedMsgs, nil
}

func (r *redisStore) RemoveHeartbeat(ctx context.Context, heartbeatID string) error {
	managerKey := GetManagerKey(r.namespace, heartbeatID)

	pipe := r.client.Pipeline()
	pipe.Del(ctx, managerKey)

	workersKey := GetWorkersKey(managerKey)
	pipe.Del(ctx, workersKey)

	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return err
	}

	return nil
}

func (r *redisStore) EnqueueMessage(ctx context.Context, queue string, priority float64, message string) error {
	_, err := r.client.ZAdd(ctx, r.getQueueName(queue), &redis.Z{
		Score:  priority,
		Member: message,
	}).Result()

	return err
}

func (r *redisStore) EnqueueScheduledMessage(ctx context.Context, priority float64, message string) error {
	_, err := r.client.ZAdd(ctx, r.namespace+ScheduledJobsKey, &redis.Z{
		Score:  priority,
		Member: message,
	}).Result()

	return err
}

func (r *redisStore) DequeueScheduledMessage(ctx context.Context, priority float64) (string, error) {
	key := r.namespace + ScheduledJobsKey

	messages, err := r.client.ZRangeByScore(ctx, key, &redis.ZRangeBy{
		Min:    "-inf",
		Max:    strconv.FormatFloat(priority, 'f', -1, 64),
		Offset: 0,
		Count:  1,
	}).Result()

	if err != nil {
		return "", err
	}

	if len(messages) == 0 {
		return "", NoMessage
	}

	removed, err := r.client.ZRem(ctx, key, messages[0]).Result()
	if err != nil {
		return "", err
	}

	if removed == 0 {
		return "", NoMessage
	}

	return messages[0], nil
}

func (r *redisStore) EnqueueRetriedMessage(ctx context.Context, priority float64, message string) error {
	_, err := r.client.ZAdd(ctx, r.namespace+RetryKey, &redis.Z{
		Score:  priority,
		Member: message,
	}).Result()

	return err
}

func (r *redisStore) DequeueRetriedMessage(ctx context.Context, priority float64) (string, error) {
	key := r.namespace + RetryKey

	messages, err := r.client.ZRangeByScore(ctx, key, &redis.ZRangeBy{
		Min:    "-inf",
		Max:    strconv.FormatFloat(priority, 'f', -1, 64),
		Offset: 0,
		Count:  1,
	}).Result()

	if err != nil {
		return "", err
	}

	if len(messages) == 0 {
		return "", NoMessage
	}

	removed, err := r.client.ZRem(ctx, key, messages[0]).Result()
	if err != nil {
		return "", err
	}

	if removed == 0 {
		return "", NoMessage
	}

	return messages[0], nil
}

func (r *redisStore) EnqueueMessageNow(ctx context.Context, queue string, message string) error {
	queue = r.namespace + "queue:" + queue
	_, err := r.client.LPush(ctx, queue, message).Result()
	return err
}

func (r *redisStore) GetAllRetries(ctx context.Context) (*Retries, error) {
	pipe := r.client.Pipeline()

	retryCountGet := pipe.ZCard(ctx, r.namespace+RetryKey)
	retryJobsGet := pipe.ZRange(ctx, r.namespace+RetryKey, 0, -1)

	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return nil, err
	}

	return &Retries{
		RetryJobs:       retryJobsGet.Val(),
		TotalRetryCount: retryCountGet.Val(),
	}, nil
}

func (r *redisStore) GetAllStats(ctx context.Context, queues []string) (*Stats, error) {
	pipe := r.client.Pipeline()

	pGet := pipe.Get(ctx, r.namespace+"stat:processed")
	fGet := pipe.Get(ctx, r.namespace+"stat:failed")
	rGet := pipe.ZCard(ctx, r.namespace+RetryKey)
	qLen := map[string]*redis.IntCmd{}

	for _, queue := range queues {
		qLen[r.namespace+queue] = pipe.LLen(ctx, fmt.Sprintf("%squeue:%s", r.namespace, queue))
	}

	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return nil, err
	}

	stats := &Stats{
		Enqueued: make(map[string]int64),
	}

	stats.Processed, _ = strconv.ParseInt(pGet.Val(), 10, 64)
	stats.Failed, _ = strconv.ParseInt(fGet.Val(), 10, 64)
	stats.RetryCount = rGet.Val()

	for q, l := range qLen {
		stats.Enqueued[q] = l.Val()
	}

	return stats, nil
}

func (r *redisStore) AcknowledgeMessage(ctx context.Context, queue string, message string) error {
	_, err := r.client.LRem(ctx, r.getQueueName(queue), -1, message).Result()

	return err
}

func (r *redisStore) CreateQueue(ctx context.Context, queue string) error {
	_, err := r.client.SAdd(ctx, r.namespace+"queues", queue).Result()
	return err
}

func (r *redisStore) ListMessages(ctx context.Context, queue string) ([]string, error) {
	messages, err := r.client.LRange(ctx, r.getQueueName(queue), 0, -1).Result()
	if err != nil {
		return nil, err
	}

	return messages, nil
}

func (r *redisStore) IncrementStats(ctx context.Context, metric string) error {
	rc := r.client

	today := time.Now().UTC().Format("2006-01-02")

	pipe := rc.Pipeline()
	pipe.Incr(ctx, r.namespace+"stat:"+metric)
	pipe.Incr(ctx, r.namespace+"stat:"+metric+":"+today)

	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}

	return nil
}

func (r *redisStore) getQueueName(queue string) string {
	return r.namespace + "queue:" + queue
}

func (r *redisStore) GetTime(ctx context.Context) (time.Time, error) {
	return r.client.Time(ctx).Result()
}
