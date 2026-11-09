package workers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/friesencr/go-workers2/storage"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

const errorText = "AHHHH"

var panickingFunc = func(message *Msg) error {
	panic(errors.New(errorText))
}

var wares = NewMiddlewares(RetryMiddleware)

func TestRetryQueue(t *testing.T) {
	ctx := context.Background()

	//puts messages in retry queue when they fail
	message, _ := NewMsg("{\"jid\":\"2\",\"retry\":true}")

	tests := []struct {
		name string
		f    JobFunc
	}{
		{
			name: "retry on panic",
			f:    panickingFunc,
		},
		{
			name: "retry on error",
			f: func(m *Msg) error {
				return errors.New("ERROR")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := SetupDefaultTestOptionsWithNamespace("prod")
			assert.NoError(t, err)

			mgr := &Manager{opts: opts}

			// Test panic
			wares.build("myqueue", mgr, tt.f)(message)

			retries, _ := opts.client.ZRange(ctx, retryQueue(opts.Namespace), 0, 1).Result()
			assert.Len(t, retries, 1)
			assert.Equal(t, message.ToJson(), retries[0])
		})
	}
}

func TestDisableRetries(t *testing.T) {
	ctx := context.Background()

	opts, err := SetupDefaultTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	mgr := &Manager{opts: opts}

	message, _ := NewMsg("{\"jid\":\"2\",\"retry\":false}")

	wares.build("myqueue", mgr, panickingFunc)(message)

	count, _ := opts.client.ZCard(ctx, retryQueue(opts.Namespace)).Result()
	assert.Equal(t, int64(0), count)
}

func TestNoDefaultRetry(t *testing.T) {
	ctx := context.Background()

	opts, err := SetupDefaultTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	mgr := &Manager{opts: opts}

	//puts messages in retry queue when they fail
	message, _ := NewMsg("{\"jid\":\"2\"}")

	wares.build("myqueue", mgr, panickingFunc)(message)

	count, _ := opts.client.ZCard(ctx, retryQueue(opts.Namespace)).Result()
	assert.Equal(t, int64(0), count)
}

func TestNumericRetries(t *testing.T) {
	ctx := context.Background()

	opts, err := SetupDefaultTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	mgr := &Manager{opts: opts}

	message, _ := NewMsg("{\"jid\":\"2\",\"retry\":true}")

	wares.build("myqueue", mgr, panickingFunc)(message)

	retries, _ := opts.client.ZRange(ctx, retryQueue(opts.Namespace), 0, 1).Result()
	assert.Equal(t, message.ToJson(), retries[0])
}

func TestHandleNewFailedMessages(t *testing.T) {
	ctx := context.Background()

	opts, err := SetupDefaultTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	mgr := &Manager{opts: opts}

	message, _ := NewMsg("{\"jid\":\"2\",\"retry\":true}")

	wares.build("prod:myqueue", mgr, panickingFunc)(message)

	retries, _ := opts.client.ZRange(ctx, retryQueue(opts.Namespace), 0, 1).Result()
	message, _ = NewMsg(retries[0])

	queue, _ := message.Get("queue").String()
	errorMessage, _ := message.Get("error_message").String()
	errorClass, _ := message.Get("error_class").String()
	retryCount, _ := message.Get("retry_count").Int()
	errorBacktrace, _ := message.Get("error_backtrace").String()
	failedAt, _ := message.Get("failed_at").String()

	assert.Equal(t, "prod:myqueue", queue)
	assert.Equal(t, errorText, errorMessage)
	assert.Equal(t, "", errorClass)
	assert.Equal(t, 0, retryCount)
	assert.Equal(t, "", errorBacktrace)

	layout := "2006-01-02 15:04:05 MST"
	assert.Equal(t, time.Now().UTC().Format(layout), failedAt)
}

func TestRecurringFailedMessages(t *testing.T) {
	ctx := context.Background()

	opts, err := SetupDefaultTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	mgr := &Manager{opts: opts}

	layout := "2006-01-02 15:04:05 MST"

	message, _ := NewMsg("{\"jid\":\"2\",\"retry\":true,\"queue\":\"default\",\"error_message\":\"bam\",\"failed_at\":\"2013-07-20 14:03:42 UTC\",\"retry_count\":10}")

	wares.build("prod:myqueue", mgr, panickingFunc)(message)

	retries, _ := opts.client.ZRange(ctx, retryQueue(opts.Namespace), 0, 1).Result()
	message, _ = NewMsg(retries[0])

	queue, _ := message.Get("queue").String()
	errorMessage, _ := message.Get("error_message").String()
	retryCount, _ := message.Get("retry_count").Int()
	failedAt, _ := message.Get("failed_at").String()
	retriedAt, _ := message.Get("retried_at").String()

	assert.Equal(t, "prod:myqueue", queue)
	assert.Equal(t, errorText, errorMessage)
	assert.Equal(t, 11, retryCount)
	assert.Equal(t, "2013-07-20 14:03:42 UTC", failedAt)
	assert.Equal(t, time.Now().UTC().Format(layout), retriedAt)
}

func TestRecurringFailedMessagesWithMax(t *testing.T) {
	ctx := context.Background()

	opts, err := SetupDefaultTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	mgr := &Manager{opts: opts}

	layout := "2006-01-02 15:04:05 MST"

	message, _ := NewMsg("{\"jid\":\"2\",\"retry\":true,\"queue\":\"default\",\"error_message\":\"bam\",\"failed_at\":\"2013-07-20 14:03:42 UTC\",\"retry_count\":8}")

	wares.build("prod:myqueue", mgr, panickingFunc)(message)

	retries, _ := opts.client.ZRange(ctx, retryQueue(opts.Namespace), 0, 1).Result()
	message, _ = NewMsg(retries[0])

	queue, _ := message.Get("queue").String()
	errorMessage, _ := message.Get("error_message").String()
	retryCount, _ := message.Get("retry_count").Int()
	failedAt, _ := message.Get("failed_at").String()
	retriedAt, _ := message.Get("retried_at").String()

	assert.Equal(t, "prod:myqueue", queue)
	assert.Equal(t, errorText, errorMessage)
	assert.Equal(t, 9, retryCount)
	assert.Equal(t, "2013-07-20 14:03:42 UTC", failedAt)
	assert.Equal(t, time.Now().UTC().Format(layout), retriedAt)
}

func TestRetryOnlyToMax(t *testing.T) {
	ctx := context.Background()

	opts, err := SetupDefaultTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	mgr := &Manager{opts: opts}

	message, _ := NewMsg("{\"jid\":\"2\",\"retry\":true,\"retry_count\":25}")

	wares.build("prod:myqueue", mgr, panickingFunc)(message)

	count, _ := opts.client.ZCard(ctx, retryQueue(opts.Namespace)).Result()
	assert.Equal(t, int64(0), count)
}

func TestRetryMaxCallsRetryExhaustionHandler(t *testing.T) {
	ctx := context.Background()

	opts, err := SetupDefaultTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	mgr := &Manager{opts: opts}
	var resultQueue string
	var resultError error
	var resultMessage *Msg
	mgr.SetRetriesExhaustedHandlers(func(queue string, message *Msg, err error) {
		resultQueue = queue
		resultError = err
		resultMessage = message
	})

	message, _ := NewMsg("{\"class\":\"clazz\",\"jid\":\"2\",\"retry\":true,\"retry_count\":25}")

	wares.build("prod:myqueue", mgr, panickingFunc)(message)

	count, _ := opts.client.ZCard(ctx, retryQueue(opts.Namespace)).Result()
	assert.Equal(t, int64(0), count)
	assert.Equal(t, "prod:myqueue", resultQueue)
	assert.Equal(t, errorText, resultError.Error())
	assert.Equal(t, "clazz", resultMessage.Class())
	assert.Equal(t, "2", resultMessage.Jid())
	assert.NotNil(t, resultMessage.Args())
}

func TestDisableRetriesCallsRetryExhaustionHandler(t *testing.T) {
	ctx := context.Background()

	opts, err := SetupDefaultTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	mgr := &Manager{opts: opts}
	var resultQueue string
	var resultError error
	var resultMessage *Msg
	var exhaustedCount int
	mgr.SetRetriesExhaustedHandlers(func(queue string, message *Msg, err error) {
		resultQueue = queue
		resultError = err
		resultMessage = message
		exhaustedCount++
	})

	message, _ := NewMsg("{\"class\":\"clazz\",\"jid\":\"2\",\"retry\":false}")

	err = wares.build("prod:myqueue", mgr, panickingFunc)(message)

	count, _ := opts.client.ZCard(ctx, retryQueue(opts.Namespace)).Result()
	assert.Equal(t, int64(0), count)
	assert.Equal(t, 1, exhaustedCount)
	assert.Equal(t, "prod:myqueue", resultQueue)
	if assert.Error(t, resultError) {
		assert.Equal(t, errorText, resultError.Error())
	}
	if assert.NotNil(t, resultMessage) {
		assert.Equal(t, "clazz", resultMessage.Class())
		assert.Equal(t, "2", resultMessage.Jid())
	}
	assert.EqualError(t, err, errorText)
	assert.True(t, message.ack)
}

func TestDisableRetriesSendsToDeadSet(t *testing.T) {
	ctx := context.Background()

	opts, err := SetupDefaultTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	mgr := &Manager{opts: opts}

	message, _ := NewMsg("{\"class\":\"clazz\",\"jid\":\"2\",\"retry\":false}")

	wares.build("prod:myqueue", mgr, panickingFunc)(message)

	retries, _ := opts.client.ZCard(ctx, retryQueue(opts.Namespace)).Result()
	assert.Equal(t, int64(0), retries)

	dead, _ := opts.client.ZRange(ctx, deadSet(opts.Namespace), 0, -1).Result()
	if assert.Len(t, dead, 1) {
		assert.Equal(t, message.ToJson(), dead[0])
	}
	assert.True(t, message.ack)
}

func TestDeadEnqueueFailureLeavesMessageUnacknowledged(t *testing.T) {
	opts, err := SetupDefaultTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	closedClient := redis.NewClient(&redis.Options{Addr: testServerAddr, DB: testDatabase})
	closedClient.Close()
	opts.store = storage.NewRedisStore(opts.Namespace, closedClient, opts.Logger, 0, 0)

	mgr := &Manager{opts: opts, logger: opts.Logger}
	var exhaustedCount int
	mgr.SetRetriesExhaustedHandlers(func(queue string, message *Msg, err error) {
		exhaustedCount++
	})

	message, _ := NewMsg("{\"class\":\"clazz\",\"jid\":\"2\",\"retry\":false}")

	err = wares.build("prod:myqueue", mgr, panickingFunc)(message)

	assert.EqualError(t, err, errorText)
	assert.False(t, message.ack)
	assert.Equal(t, 1, exhaustedCount)
}

func TestPanickingRetriesExhaustedHandlerRecordsDeadJobOnce(t *testing.T) {
	ctx := context.Background()

	opts, err := SetupDefaultTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	mgr := &Manager{opts: opts, logger: opts.Logger}
	var exhaustedCount int
	mgr.SetRetriesExhaustedHandlers(func(queue string, message *Msg, err error) {
		exhaustedCount++
		panic("handler failed")
	})

	message, _ := NewMsg("{\"class\":\"clazz\",\"jid\":\"2\",\"retry\":false}")

	err = DefaultMiddlewares().build("prod:myqueue", mgr, panickingFunc)(message)

	assert.EqualError(t, err, "handler failed")
	assert.Equal(t, 1, exhaustedCount)
	dead, _ := opts.client.ZRange(ctx, deadSet(opts.Namespace), 0, -1).Result()
	if assert.Len(t, dead, 1) {
		deadMessage, _ := NewMsg(dead[0])
		assert.Equal(t, errorText, deadMessage.Get("error_message").MustString())
	}
	assert.True(t, message.ack)
}

func TestRetriedJobIsReportedAsFailed(t *testing.T) {
	ctx := context.Background()

	opts, err := SetupDefaultTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	var logs bytes.Buffer
	mgr := &Manager{opts: opts, logger: slog.New(slog.NewTextHandler(&logs, nil))}

	message, _ := NewMsg("{\"class\":\"clazz\",\"jid\":\"2\",\"retry\":true}")

	err = DefaultMiddlewares().build("prod:myqueue", mgr, panickingFunc)(message)

	assert.EqualError(t, err, errorText)
	count, _ := opts.client.ZCard(ctx, retryQueue(opts.Namespace)).Result()
	assert.Equal(t, int64(1), count)
	assert.Contains(t, logs.String(), `msg="job fail"`)
	assert.NotContains(t, logs.String(), `msg="job done"`)
	assert.True(t, message.ack)
}

func TestRetryEnqueueFailureReturnsJobErrorAndLeavesMessageUnacknowledged(t *testing.T) {
	opts, err := SetupDefaultTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	closedClient := redis.NewClient(&redis.Options{Addr: testServerAddr, DB: testDatabase})
	closedClient.Close()
	opts.store = storage.NewRedisStore(opts.Namespace, closedClient, opts.Logger, 0, 0)

	mgr := &Manager{opts: opts, logger: opts.Logger}

	message, _ := NewMsg("{\"class\":\"clazz\",\"jid\":\"2\",\"retry\":true}")

	err = wares.build("prod:myqueue", mgr, panickingFunc)(message)

	assert.EqualError(t, err, errorText)
	assert.False(t, message.ack)
}

// Run with -race: the setter and the failing jobs race on the handlers slice unless the read is locked.
func TestRetriesExhaustedHandlersCanBeSetWhileJobsFail(t *testing.T) {
	opts, err := SetupDefaultTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	mgr := &Manager{opts: opts, logger: opts.Logger}
	var exhaustedCount atomic.Int32
	handler := func(queue string, message *Msg, err error) {
		exhaustedCount.Add(1)
	}
	mgr.SetRetriesExhaustedHandlers(handler)

	done := make(chan struct{})
	var setter sync.WaitGroup
	setter.Add(1)
	go func() {
		defer setter.Done()
		for {
			select {
			case <-done:
				return
			default:
				mgr.SetRetriesExhaustedHandlers(handler)
			}
		}
	}()

	const jobs = 50
	job := wares.build("prod:myqueue", mgr, panickingFunc)
	for i := 0; i < jobs; i++ {
		message, _ := NewMsg(fmt.Sprintf("{\"class\":\"clazz\",\"jid\":\"%d\",\"retry\":false}", i))
		job(message)
	}
	close(done)
	setter.Wait()

	assert.Equal(t, int32(jobs), exhaustedCount.Load())
}

func TestRetryOnlyToCustomMax(t *testing.T) {
	ctx := context.Background()

	opts, err := SetupDefaultTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	mgr := &Manager{opts: opts}

	message, _ := NewMsg("{\"jid\":\"2\",\"max_retry\":3,\"retry_count\":3}")

	wares.build("prod:myqueue", mgr, panickingFunc)(message)

	count, _ := opts.client.ZCard(ctx, retryQueue(opts.Namespace)).Result()
	assert.Equal(t, int64(0), count)
}

func TestRetryMaxZeroNeverRetries(t *testing.T) {
	ctx := context.Background()

	opts, err := SetupDefaultTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	mgr := &Manager{opts: opts}
	var exhaustedCount int
	mgr.SetRetriesExhaustedHandlers(func(queue string, message *Msg, err error) {
		exhaustedCount++
	})

	p := &Producer{opts: opts}
	retryMax := 0
	_, err = p.EnqueueWithOptions("myqueue", "clazz", []int{1}, EnqueueOptions{Retry: true, RetryMax: &retryMax})
	assert.NoError(t, err)

	raw, err := opts.client.LPop(ctx, "prod:queue:myqueue").Result()
	assert.NoError(t, err)

	var wire map[string]interface{}
	err = json.Unmarshal([]byte(raw), &wire)
	assert.NoError(t, err)
	assert.Contains(t, wire, "retry_max")
	assert.Equal(t, float64(0), wire["retry_max"])

	message, err := NewMsg(raw)
	assert.NoError(t, err)

	wares.build("prod:myqueue", mgr, panickingFunc)(message)

	count, _ := opts.client.ZCard(ctx, retryQueue(opts.Namespace)).Result()
	assert.Equal(t, int64(0), count)
	assert.Equal(t, 1, exhaustedCount)
}

func TestRetryMaxFromExistingMessageIsHonored(t *testing.T) {
	ctx := context.Background()

	opts, err := SetupDefaultTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	mgr := &Manager{opts: opts}

	raw := "{\"jid\":\"2\",\"retry\":true,\"retry_max\":2,\"retry_count\":2}"

	var data EnqueueData
	err = json.Unmarshal([]byte(raw), &data)
	assert.NoError(t, err)
	if assert.NotNil(t, data.RetryMax) {
		assert.Equal(t, 2, *data.RetryMax)
	}

	message, _ := NewMsg(raw)

	wares.build("prod:myqueue", mgr, panickingFunc)(message)

	count, _ := opts.client.ZCard(ctx, retryQueue(opts.Namespace)).Result()
	assert.Equal(t, int64(0), count)
}

func boolPtr(b bool) *bool { return &b }

func TestMessageRetainExhaustedFalseSkipsDeadSet(t *testing.T) {
	ctx := context.Background()

	opts, err := SetupDefaultTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	mgr := &Manager{opts: opts, logger: opts.Logger}
	var exhaustedCount int
	mgr.SetRetriesExhaustedHandlers(func(queue string, message *Msg, err error) {
		exhaustedCount++
	})

	message, _ := NewMsg("{\"class\":\"clazz\",\"jid\":\"2\",\"retry\":false,\"retain_exhausted\":false}")

	err = wares.build("prod:myqueue", mgr, panickingFunc)(message)

	assert.EqualError(t, err, errorText)
	dead, _ := opts.client.ZCard(ctx, deadSet(opts.Namespace)).Result()
	assert.Equal(t, int64(0), dead)
	assert.True(t, message.ack)
	assert.Equal(t, 1, exhaustedCount)
}

func TestMessageRetainExhaustedTrueOverridesClassAndQueue(t *testing.T) {
	ctx := context.Background()

	opts, err := SetupDefaultTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	mgr := &Manager{opts: opts, logger: opts.Logger}
	mgr.SetClassOptions("clazz", ClassOptions{RetainExhausted: boolPtr(false)})
	mgr.queueOptions = map[string]WorkerOptions{
		"myqueue": {RetainExhausted: boolPtr(false)},
	}

	message, _ := NewMsg("{\"class\":\"clazz\",\"jid\":\"2\",\"retry\":false,\"retain_exhausted\":true}")

	err = wares.build("prod:myqueue", mgr, panickingFunc)(message)

	assert.EqualError(t, err, errorText)
	dead, _ := opts.client.ZRange(ctx, deadSet(opts.Namespace), 0, -1).Result()
	assert.Len(t, dead, 1)
	assert.True(t, message.ack)
}

func TestClassRetainExhaustedFalseSkipsDeadSet(t *testing.T) {
	ctx := context.Background()

	opts, err := SetupDefaultTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	mgr := &Manager{opts: opts, logger: opts.Logger}
	mgr.SetClassOptions("clazz", ClassOptions{RetainExhausted: boolPtr(false)})

	message, _ := NewMsg("{\"class\":\"clazz\",\"jid\":\"2\",\"retry\":false}")

	err = wares.build("prod:myqueue", mgr, panickingFunc)(message)

	assert.EqualError(t, err, errorText)
	dead, _ := opts.client.ZCard(ctx, deadSet(opts.Namespace)).Result()
	assert.Equal(t, int64(0), dead)
	assert.True(t, message.ack)
}

func TestQueueRetainExhaustedFalseSkipsDeadSet(t *testing.T) {
	ctx := context.Background()

	opts, err := SetupDefaultTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	mgr := &Manager{opts: opts, logger: opts.Logger}
	mgr.AddWorkerWithOptions("myqueue", 1, func(m *Msg) error { return nil }, WorkerOptions{
		RetainExhausted: boolPtr(false),
	}, NopMiddleware)

	message, _ := NewMsg("{\"class\":\"clazz\",\"jid\":\"2\",\"retry\":false}")

	err = wares.build("prod:myqueue", mgr, panickingFunc)(message)

	assert.EqualError(t, err, errorText)
	dead, _ := opts.client.ZCard(ctx, deadSet(opts.Namespace)).Result()
	assert.Equal(t, int64(0), dead)
	assert.True(t, message.ack)
}

func TestClassRetainExhaustedWinsOverQueue(t *testing.T) {
	ctx := context.Background()

	opts, err := SetupDefaultTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	mgr := &Manager{opts: opts, logger: opts.Logger}
	mgr.SetClassOptions("clazz", ClassOptions{RetainExhausted: boolPtr(false)})
	mgr.queueOptions = map[string]WorkerOptions{
		"myqueue": {RetainExhausted: boolPtr(true)},
	}

	message, _ := NewMsg("{\"class\":\"clazz\",\"jid\":\"2\",\"retry\":false}")

	err = wares.build("prod:myqueue", mgr, panickingFunc)(message)

	assert.EqualError(t, err, errorText)
	dead, _ := opts.client.ZCard(ctx, deadSet(opts.Namespace)).Result()
	assert.Equal(t, int64(0), dead)
	assert.True(t, message.ack)
}

func TestExhaustedRetriesWithRetainExhaustedFalseSkipsDeadSet(t *testing.T) {
	ctx := context.Background()

	opts, err := SetupDefaultTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	mgr := &Manager{opts: opts, logger: opts.Logger}

	message, _ := NewMsg("{\"class\":\"clazz\",\"jid\":\"2\",\"retry\":true,\"retry_count\":25,\"retain_exhausted\":false}")

	err = wares.build("prod:myqueue", mgr, panickingFunc)(message)

	assert.EqualError(t, err, errorText)
	retries, _ := opts.client.ZCard(ctx, retryQueue(opts.Namespace)).Result()
	assert.Equal(t, int64(0), retries)
	dead, _ := opts.client.ZCard(ctx, deadSet(opts.Namespace)).Result()
	assert.Equal(t, int64(0), dead)
	assert.True(t, message.ack)
}
