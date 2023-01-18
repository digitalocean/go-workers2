package storage

import (
	"context"
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

func (r *redisStore) SendHeartbeat(ctx context.Context, heartbeat *Heartbeat) error {
	now, err := r.GetTime(ctx)
	if err != nil {
		return err
	}

	pipe := r.client.Pipeline()
	rtt := r.CheckRtt(ctx)

	managerKey := GetManagerKey(r.namespace, heartbeat.Identity)
	pipe.SAdd(ctx, GetProcessesKey(r.namespace), heartbeat.Identity) // add to the sidekiq processes set without the namespace

	pipe.HMSet(ctx, managerKey,
		"beat", heartbeat.Beat.UTC().Unix(),
		"quiet", heartbeat.Quiet,
		"busy", heartbeat.Busy,
		"rtt_us", rtt,
		"rss", heartbeat.RSS,
		"info", heartbeat.Info,
		"active_manager", heartbeat.ActiveManager)
	pipe.Expire(ctx, managerKey, heartbeat.Ttl)

	workersKey := GetWorkersKey(managerKey)
	pipe.Del(ctx, workersKey)

	for _, workerHeatbeat := range heartbeat.WorkerHeartbeats {
		workerID := GetWorkerID(heartbeat.Pid, workerHeatbeat.Tid)
		if workerHeatbeat.WorkerMsg != "" {
			// workersKey contains in-progress work messages for a given worker as of the heartbeat
			pipe.HSet(ctx, workersKey, workerID, workerHeatbeat.WorkerMsg)
		}
		// workerKey contains worker info used for recovering un-handled in-progress work
		workerKey := GetWorkerKey(r.namespace, workerID)
		pipe.HMSet(ctx, workerKey,
			"pid", workerHeatbeat.Pid,
			"tid", workerHeatbeat.Tid,
			"queue", workerHeatbeat.Queue,
			"inprogress_queue", workerHeatbeat.InProgressQueue)
		pipe.ZAdd(ctx, GetActiveWorkersKey(r.namespace), &redis.Z{Member: workerKey, Score: float64(now.Unix())})
	}
	_, err = pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return err
	}

	return nil
}

func (r *redisStore) HandleExpiredHeartbeatIdentities(ctx context.Context) ([]string, error) {
	heartbeatIDs, err := r.client.SMembers(ctx, GetProcessesKey(r.namespace)).Result()
	var expiredHeartbeatIDs []string
	if err != nil && err != redis.Nil {
		return nil, err
	}
	for _, heartbeatID := range heartbeatIDs {
		managerKey := GetManagerKey(r.namespace, heartbeatID)
		exist, err := r.client.Exists(ctx, managerKey).Result()
		if err != nil {
			return nil, err
		}
		if exist == int64(0) {
			_, err := r.client.SRem(ctx, GetProcessesKey(r.namespace), heartbeatID).Result()
			if err != nil {
				return nil, err
			}
			expiredHeartbeatIDs = append(expiredHeartbeatIDs, heartbeatID)
		}
	}
	return expiredHeartbeatIDs, nil
}

func (r *redisStore) getExpiredWorkerHeartbeatKeys(ctx context.Context, expireTS int64) ([]string, error) {
	expiredTaskRunnerKeys, err := r.client.ZRangeByScore(ctx, GetActiveWorkersKey(r.namespace), &redis.ZRangeBy{Min: "-inf", Max: fmt.Sprintf("(%d", expireTS)}).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	return expiredTaskRunnerKeys, nil
}

func (r *redisStore) getTaskRunnerID(pid int, tid string) string {
	return fmt.Sprintf("%d-%s", pid, tid)
}

func (r *redisStore) getWorkerHeartbeat(ctx context.Context, workerHeartbeatKey string) (*WorkerHeartbeat, error) {
	workerHeartbeatMap, err := r.client.HGetAll(ctx, workerHeartbeatKey).Result()
	if len(workerHeartbeatMap) == 0 {
		return nil, err
	}
	pid, err := strconv.Atoi(workerHeartbeatMap["pid"])
	if err != nil {
		return nil, err
	}
	workerHeartbeat := &WorkerHeartbeat{
		Tid:             workerHeartbeatMap["tid"],
		Pid:             pid,
		Queue:           workerHeartbeatMap["queue"],
		InProgressQueue: workerHeartbeatMap["inprogress_queue"],
	}
	if err != nil && err != redis.Nil {
		return &WorkerHeartbeat{}, err
	}
	return workerHeartbeat, err
}

func (r *redisStore) evictWorkerHeartbeat(ctx context.Context, pid int, tid string) error {
	workerID := GetWorkerID(pid, tid)
	workerKey := GetWorkerKey(r.namespace, workerID)
	pipe := r.client.Pipeline()
	pipe.ZRem(ctx, GetActiveWorkersKey(r.namespace), workerKey)
	pipe.Del(ctx, workerKey)

	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return err
	}
	return nil
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

func (r *redisStore) HandleExpiredWorkerHeartbeats(ctx context.Context, expireTS int64) ([]*StaleMessageUpdate, error) {
	// now
	expiredWorkerHeartbeatKeys, err := r.getExpiredWorkerHeartbeatKeys(ctx, expireTS)
	if err != nil {
		return nil, err
	}

	var staleMessageUpdates []*StaleMessageUpdate
	requeuedInProgressQueues := make(map[string]bool)
	for _, expiredWorkerHeartbeatKey := range expiredWorkerHeartbeatKeys {
		expiredWorkerHeartbeat, err := r.getWorkerHeartbeat(ctx, expiredWorkerHeartbeatKey)
		if err != nil {
			return nil, err
		}
		if expiredWorkerHeartbeat == nil {
			continue
		}
		var requeuedMsgs []string
		if _, exists := requeuedInProgressQueues[expiredWorkerHeartbeat.InProgressQueue]; exists {
			continue
		}
		requeuedMsgs, err = r.RequeueMessagesFromInProgressQueue(ctx, expiredWorkerHeartbeat.InProgressQueue, expiredWorkerHeartbeat.Queue)
		if err != nil {
			return nil, err
		}
		staleMessageUpdate := &StaleMessageUpdate{
			Queue:           expiredWorkerHeartbeat.Queue,
			InprogressQueue: expiredWorkerHeartbeat.InProgressQueue,
			RequeuedMsgs:    requeuedMsgs,
		}
		err = r.evictWorkerHeartbeat(ctx, expiredWorkerHeartbeat.Pid, expiredWorkerHeartbeat.Tid)
		if err != nil {
			return staleMessageUpdates, err
		}
		staleMessageUpdates = append(staleMessageUpdates, staleMessageUpdate)
		requeuedInProgressQueues[expiredWorkerHeartbeat.InProgressQueue] = true
	}
	return staleMessageUpdates, nil
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
