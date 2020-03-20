package storage

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/bitly/go-simplejson"
	"github.com/go-redis/redis"
)

type redisStore struct {
	namespace string

	client *redis.Client
	logger *log.Logger
}

// Compile-time check to ensure that Redis store does in fact implement the Store interface
var _ Store = &redisStore{}

func NewRedisStore(namespace string, client *redis.Client) Store {
	return &redisStore{
		namespace: namespace,
		client:    client,
	}
}

func (r *redisStore) DequeueMessage(queue string, inprogressQueue string, timeout time.Duration) (string, error) {
	message, err := r.client.BRPopLPush(r.getQueueName(queue), r.getQueueName(inprogressQueue), timeout).Result()

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
	} else {
		return message, nil
	}
}

func (r *redisStore) EnqueueMessage(queue string, priority float64, message string) error {
	_, err := r.client.ZAdd(r.getQueueName(queue), redis.Z{
		Score:  priority,
		Member: message,
	}).Result()

	return err
}

func (r *redisStore) EnqueueScheduledMessage(priority float64, message string) error {
	_, err := r.client.ZAdd(r.namespace+ScheduledJobsKey, redis.Z{
		Score:  priority,
		Member: message,
	}).Result()

	return err
}

func (r *redisStore) DequeueScheduledMessage(priority float64) (string, error) {
	key := r.namespace + ScheduledJobsKey

	messages, err := r.client.ZRangeByScore(key, redis.ZRangeBy{
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

	removed, err := r.client.ZRem(key, messages[0]).Result()
	if err != nil {
		return "", err
	}

	if removed == 0 {
		return "", NoMessage
	}

	return messages[0], nil
}

func (r *redisStore) EnqueueRetriedMessage(priority float64, message string) error {
	_, err := r.client.ZAdd(r.namespace+RetryKey, redis.Z{
		Score:  priority,
		Member: message,
	}).Result()

	return err
}

func (r *redisStore) DequeueRetriedMessage(priority float64) (string, error) {
	key := r.namespace + RetryKey

	messages, err := r.client.ZRangeByScore(key, redis.ZRangeBy{
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

	removed, err := r.client.ZRem(key, messages[0]).Result()
	if err != nil {
		return "", err
	}

	if removed == 0 {
		return "", NoMessage
	}

	return messages[0], nil
}

func (r *redisStore) EnqueueMessageNow(queue string, message string) error {
	queue = r.namespace + "queue:" + queue
	_, err := r.client.LPush(queue, message).Result()
	return err
}

func (r *redisStore) GetAllRetries() (*Retries, error) {
	pipe := r.client.Pipeline()
	retries := &Retries{}
	retryCountGet := pipe.ZCard(r.namespace + RetryKey)
	retryJobsGet, err := r.client.ZRange(r.namespace+RetryKey, 0, 1).Result()
	if err != nil {
		return nil, err
	}

	retryJobStats, err := r.getRetryJson(retryJobsGet)
	if err != nil {
		return nil, err
	}

	_, err = pipe.Exec()

	if err != nil && err != redis.Nil {
		return nil, err
	}

	retries.RetryJobs = retryJobStats
	retries.TotalRetryCount = retryCountGet.Val()

	return retries, nil
}

func (r *redisStore) GetAllStats(queues []string) (*Stats, error) {
	pipe := r.client.Pipeline()

	pGet := pipe.Get(r.namespace + "stat:processed")
	fGet := pipe.Get(r.namespace + "stat:failed")
	rGet := pipe.ZCard(r.namespace + RetryKey)
	qLen := map[string]*redis.IntCmd{}

	for _, queue := range queues {
		qLen[r.namespace+queue] = pipe.LLen(fmt.Sprintf("%squeue:%s", r.namespace, queue))
	}

	_, err := pipe.Exec()

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

func (r *redisStore) AcknowledgeMessage(queue string, message string) error {
	_, err := r.client.LRem(r.getQueueName(queue), -1, message).Result()

	return err
}

func (r *redisStore) CreateQueue(queue string) error {
	_, err := r.client.SAdd(r.namespace+"queues", queue).Result()
	return err
}

func (r *redisStore) ListMessages(queue string) ([]string, error) {
	messages, err := r.client.LRange(r.getQueueName(queue), 0, -1).Result()
	if err != nil {
		return nil, err
	}

	return messages, nil
}

func (r *redisStore) IncrementStats(metric string) error {
	rc := r.client

	today := time.Now().UTC().Format("2006-01-02")

	pipe := rc.Pipeline()
	pipe.Incr(r.namespace + "stat:" + metric)
	pipe.Incr(r.namespace + "stat:" + metric + ":" + today)

	if _, err := pipe.Exec(); err != nil {
		return err
	}

	return nil
}

func (r *redisStore) getQueueName(queue string) string {
	return r.namespace + "queue:" + queue
}

func (r *redisStore) getRetryJson(retryStats []string) ([]RetryJobStats, error) {
	// parse json from string of retry data
	allRetryStats, err := newMsg(retryStats[0])
	if err != nil {
		return nil, err
	}

	class, err := allRetryStats.Get("class").String()
	if err != nil {
		return nil, err
	}

	error_msg, err := allRetryStats.Get("error_message").String()
	if err != nil {
		return nil, err
	}

	failed_at, err := allRetryStats.Get("failed_at").String()
	if err != nil {
		return nil, err
	}

	job_id, err := allRetryStats.Get("jid").String()
	if err != nil {
		return nil, err
	}

	queue, err := allRetryStats.Get("queue").String()
	if err != nil {
		return nil, err
	}

	retry_count, err := allRetryStats.Get("retry_count").Int64()
	if err != nil {
		return nil, err
	}

	var retryJobStats []RetryJobStats
	for i := 0; i < len(retryStats[0]); i++ {
		retryJobStats = append(retryJobStats, RetryJobStats{
			Class:        class,
			ErrorMessage: error_msg,
			FailedAt:     failed_at,
			JobID:        job_id,
			Queue:        queue,
			RetryCount:   retry_count,
		})
	}

	return retryJobStats, nil
}

type data struct {
	*simplejson.Json
}

type Msg struct {
	*data
	original  string
	ack       bool
	startedAt int64
}

func newMsg(content string) (*Msg, error) {
	d, err := newData(content)
	if err != nil {
		return nil, err
	}
	return &Msg{
		data:      d,
		original:  content,
		ack:       true,
		startedAt: 0,
	}, nil
}

func newData(content string) (*data, error) {
	json, err := simplejson.NewJson([]byte(content))
	if err != nil {
		return nil, err
	}
	return &data{json}, nil
}
