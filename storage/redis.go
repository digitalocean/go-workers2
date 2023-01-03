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

var sidekiqHeartbeatJobsKey = ":work"

func (r *redisStore) SendHeartbeat(ctx context.Context, heartbeat *Heartbeat, heartbeatManagerTTL time.Duration) error {
	now, err := r.GetTime(ctx)
	if err != nil {
		return err
	}

	pipe := r.client.Pipeline()
	rtt := r.CheckRtt(ctx)

	managerKey := r.getManagerKey(heartbeat.Identity)
	sidekiqProcessesKey := r.namespace + "processes"
	pipe.SAdd(ctx, sidekiqProcessesKey, heartbeat.Identity) // add to the sidekiq processes set without the namespace

	pipe.HMSet(ctx, managerKey, "beat", heartbeat.Beat.UTC().Unix())
	pipe.HSet(ctx, managerKey, "quiet", heartbeat.Quiet)
	pipe.HSet(ctx, managerKey, "busy", heartbeat.Busy)
	pipe.HSet(ctx, managerKey, "rtt_us", rtt)
	pipe.HSet(ctx, managerKey, "rss", heartbeat.RSS)
	pipe.HSet(ctx, managerKey, "info", heartbeat.Info)
	pipe.Expire(ctx, managerKey, heartbeatManagerTTL)

	workKey := r.getWorkKey(managerKey)
	pipe.Del(ctx, workKey)

	for _, taskRunnerInfo := range heartbeat.TaskRunnersInfo {
		taskRunnerID := r.getTaskRunnerID(heartbeat.Pid, taskRunnerInfo.Tid)
		if taskRunnerInfo.WorkerMsg != "" {
			// workKey contains in-progress work messages for a given task runner as of the heartbeat
			pipe.HSet(ctx, workKey, taskRunnerID, taskRunnerInfo.WorkerMsg)
		}
		// taskRunnerKey contains taskrunner info used for recovering un-handled in-progress work
		taskRunnerKey := r.getTaskRunnerKey(taskRunnerID)
		pipe.HMSet(ctx, taskRunnerKey,
			"pid", taskRunnerInfo.Pid,
			"tid", taskRunnerInfo.Tid,
			"queue", taskRunnerInfo.Queue,
			"inprogress_queue", taskRunnerInfo.InProgressQueue)
		pipe.ZAdd(ctx, r.namespace+"task-runners-active-ts", &redis.Z{Member: taskRunnerKey, Score: float64(now.Unix())})
	}
	_, err = pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return err
	}

	return nil
}

func (r *redisStore) GetExpiredTaskRunnerKeys(ctx context.Context, expireTS int64) ([]string, error) {
	expiredTaskRunnerKeys, err := r.client.ZRangeByScore(ctx, r.namespace+"task-runners-active-ts", &redis.ZRangeBy{Min: "-inf", Max: fmt.Sprintf("(%d", expireTS)}).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	return expiredTaskRunnerKeys, nil
}

func (r *redisStore) getManagerKey(heartbeatID string) string {
	return r.namespace + heartbeatID
}

func (r *redisStore) getWorkKey(managerID string) string {
	return managerID + sidekiqHeartbeatJobsKey
}

func (r *redisStore) getTaskRunnerID(pid int, tid string) string {
	return fmt.Sprintf("%d-%s", pid, tid)
}

func (r *redisStore) getTaskRunnerKey(taskRunnerID string) string {
	return r.namespace + "taskrunner-" + taskRunnerID
}

func (r *redisStore) GetTaskRunnerInfo(ctx context.Context, taskRunnerKey string) (TaskRunnerInfo, error) {
	taskRunnerInfoMap, err := r.client.HGetAll(ctx, taskRunnerKey).Result()
	pid, err := strconv.Atoi(taskRunnerInfoMap["pid"])
	if err != nil {
		return TaskRunnerInfo{}, err
	}
	taskRunnerInfo := TaskRunnerInfo{
		Tid:             taskRunnerInfoMap["tid"],
		Pid:             pid,
		Queue:           taskRunnerInfoMap["queue"],
		InProgressQueue: taskRunnerInfoMap["inprogress_queue"],
	}
	if err != nil && err != redis.Nil {
		return TaskRunnerInfo{}, err
	}
	return taskRunnerInfo, err
}

func (r *redisStore) EvictTaskRunnerInfo(ctx context.Context, pid int, tid string) error {
	taskRunnerID := r.getTaskRunnerID(pid, tid)
	taskRunnerKey := r.getTaskRunnerKey(taskRunnerID)
	pipe := r.client.Pipeline()
	pipe.ZRem(ctx, r.namespace+"task-runners-active-ts", taskRunnerKey)
	pipe.Del(ctx, taskRunnerKey)

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
	managerKey := r.getManagerKey(heartbeatID)

	pipe := r.client.Pipeline()
	pipe.Del(ctx, managerKey)

	workerKey := r.getWorkKey(managerKey)
	pipe.Del(ctx, workerKey)

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

func (r *redisStore) AddActiveCluster(ctx context.Context, clusterID string, clusterPriority float64) error {
	now, err := r.GetTime(ctx)
	if err != nil {
		return err
	}
	_, err = r.client.ZAdd(ctx, r.namespace+"clusters-active-ts", &redis.Z{Member: clusterID, Score: float64(now.Unix())}).Result()
	if err != nil && err != redis.Nil {
		return err
	}
	_, err = r.client.ZAdd(ctx, r.namespace+"clusters-active-priority", &redis.Z{Member: clusterID, Score: clusterPriority}).Result()
	if err != nil && err != redis.Nil {
		return err
	}
	return nil
}

func (r *redisStore) EvictExpiredClusters(ctx context.Context, expireTS int64) error {
	evictClusterIDs, err := r.client.ZRangeByScore(ctx, r.namespace+"clusters-active-ts", &redis.ZRangeBy{Min: "-inf", Max: fmt.Sprintf("(%d", expireTS)}).Result()
	if err != nil {
		return err
	}
	if len(evictClusterIDs) == 0 {
		return nil
	}
	pipe := r.client.Pipeline()
	pipe.ZRem(ctx, r.namespace+"clusters-active-priority", evictClusterIDs)
	pipe.ZRemRangeByScore(ctx, r.namespace+"clusters-active-ts", "-inf", fmt.Sprintf("(%d", expireTS))

	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return err
	}
	return nil
}

func (r *redisStore) GetActiveClusterIDs(ctx context.Context) ([]string, error) {
	activeClusterIDs, err := r.client.ZRangeByScore(ctx, r.namespace+"clusters-active-ts", &redis.ZRangeBy{Min: "-inf", Max: "+inf", Offset: 0, Count: 1}).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	return activeClusterIDs, nil
}

func (r *redisStore) GetHighestPriorityActiveClusterID(ctx context.Context) (string, error) {
	activeClusterIDs, err := r.client.ZRangeByScore(ctx, r.namespace+"clusters-active-priority", &redis.ZRangeBy{Min: "-inf", Max: "+inf", Offset: 0, Count: 1}).Result()
	if err != nil && err != redis.Nil {
		return "", err
	}
	if err == redis.Nil || len(activeClusterIDs) == 0 {
		return "", nil
	}
	return activeClusterIDs[0], nil
}
