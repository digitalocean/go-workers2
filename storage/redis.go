package storage

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"
	"reflect"

	"github.com/go-redis/redis/v8"

	"math/rand"
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

func (r* redisStore) CheckRtt(ctx context.Context) int64 {
	start := time.Now()
	r.client.Ping(ctx)
	ellapsed := time.Since(start)

	return ellapsed.Microseconds()
}

func (r *redisStore) SendHeartbeat(ctx context.Context, identity string, beat time.Time, quiet bool, busy int, rttUs int, rss int64, info string, pid int, workers map[string][]string) error {

	pipe := r.client.Pipeline()
	rtt := r.CheckRtt(ctx)

	maangerIdentity := r.namespace + identity
	sidekiqProcessesKey := r.namespace + "processes"

	pipe.SAdd(ctx, sidekiqProcessesKey, identity) // add to the sidekiq processes set without the namespace

	pipe.HSet(ctx, maangerIdentity, "beat", beat.UTC().Unix())
	pipe.HSet(ctx, maangerIdentity, "quiet", quiet)
	pipe.HSet(ctx, maangerIdentity, "busy", busy)
	pipe.HSet(ctx, maangerIdentity, "rtt_us", rtt)
	pipe.HSet(ctx, maangerIdentity, "rss", rss)
	pipe.HSet(ctx, maangerIdentity, "info", info)
	pipe.Expire(ctx, maangerIdentity, 60 * time.Second) // set the TTL of the heartbeat to 60

	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return err
	}


	// workers
	// found msg &{0xc00033a058 {"queue":"sleepgo","class":"Add","args":[10],"jid":"f4914398ea383d1a0611e884","enqueued_at":1631906124.4731379,"at":1631906124.473137} true 1631906139}

	//  => {"retry"=>9, "queue"=>"sleeprb", "backtrace"=>true, "class"=>"SleepWorker", "args"=>[60], "jid"=>"348adede638ab7d4c2e547e7", "created_at"=>1631905645.1018732, "Trace-Context"=>{"uber-trace-id"=>"8e55bdaf3409cbbb:8e55bdaf3409cbbb:0:1"}, "enqueued_at"=>1631905645.1061718}

	workersKey := maangerIdentity + ":workers"

	pipe = r.client.Pipeline()

	// 2) "{\"retry\":1,\"queue\":\"sleepgo\",\"backtrace\":false,\"class\":\"Add\",\"args\":[],\"jid\":\"0bedcd4e6788342e9a2e26ef\",\"created_at\":1631910386,\"enqueued_at\":1631910391}"


	// 2) "{\"queue\":\"sleeprb\",\"payload\":\"{\\\"retry\\\":9,\\\"queue\\\":\\\"sleeprb\\\",\\\"backtrace\\\":true,\\\"class\\\":\\\"SleepWorker\\\",\\\"args\\\":[60],\\\"jid\\\":\\\"d722863bc0092f44d23f655e\\\",\\\"created_at\\\":1631910445.881293,\\\"Trace-Context\\\":{\\\"uber-trace-id\\\":\\\"8aa4890c1585e9f3:8aa4890c1585e9f3:0:1\\\"},\\\"enqueued_at\\\":1631910445.8897479}\",\"run_at\":1631910445}"

	pipe.Del(ctx, workersKey)

	for queue, msgs := range workers {
		fmt.Println("found msgs in queue:",queue, "msgs:", msgs)
		fmt.Println(reflect.TypeOf(msgs), msgs)

		for _, msg := range msgs {
			fmt.Println("found msg", reflect.TypeOf(msg), msg)

			num := rand.Intn(1000)
			fmt.Println(num)
			fakeThreadId := fmt.Sprintf("go-%d-%d", pid, num)

			pipe.HSet(ctx, workersKey, fakeThreadId, msg)
		}

		pipe.Expire(ctx, workersKey, 60 * time.Second)
	}

	// for _, worker := range workers {
	// 	pipe.SAdd(ctx, workersKey, "go-no-thread-id", worker)
	// }


	_, err = pipe.Exec(ctx)
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
