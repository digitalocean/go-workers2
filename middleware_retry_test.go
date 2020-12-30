package workers

import (
	"context"
	"errors"
	"testing"
	"time"

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
			opts, err := setupTestOptionsWithNamespace("prod")
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

	opts, err := setupTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	mgr := &Manager{opts: opts}

	message, _ := NewMsg("{\"jid\":\"2\",\"retry\":false}")

	wares.build("myqueue", mgr, panickingFunc)(message)

	count, _ := opts.client.ZCard(ctx, retryQueue(opts.Namespace)).Result()
	assert.Equal(t, int64(0), count)
}

func TestNoDefaultRetry(t *testing.T) {
	ctx := context.Background()

	opts, err := setupTestOptionsWithNamespace("prod")
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

	opts, err := setupTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	mgr := &Manager{opts: opts}

	message, _ := NewMsg("{\"jid\":\"2\",\"retry\":5}")

	wares.build("myqueue", mgr, panickingFunc)(message)

	retries, _ := opts.client.ZRange(ctx, retryQueue(opts.Namespace), 0, 1).Result()
	assert.Equal(t, message.ToJson(), retries[0])
}

func TestHandleNewFailedMessages(t *testing.T) {
	ctx := context.Background()

	opts, err := setupTestOptionsWithNamespace("prod")
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

	opts, err := setupTestOptionsWithNamespace("prod")
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

	opts, err := setupTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	mgr := &Manager{opts: opts}

	layout := "2006-01-02 15:04:05 MST"

	message, _ := NewMsg("{\"jid\":\"2\",\"retry\":10,\"queue\":\"default\",\"error_message\":\"bam\",\"failed_at\":\"2013-07-20 14:03:42 UTC\",\"retry_count\":8}")

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

	opts, err := setupTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	mgr := &Manager{opts: opts}

	message, _ := NewMsg("{\"jid\":\"2\",\"retry\":true,\"retry_count\":25}")

	wares.build("prod:myqueue", mgr, panickingFunc)(message)

	count, _ := opts.client.ZCard(ctx, retryQueue(opts.Namespace)).Result()
	assert.Equal(t, int64(0), count)
}

func TestRetryMaxCallsRetryExhaustionHandler(t *testing.T) {
	ctx := context.Background()

	opts, err := setupTestOptionsWithNamespace("prod")
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

func TestRetryOnlyToCustomMax(t *testing.T) {
	ctx := context.Background()

	opts, err := setupTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	mgr := &Manager{opts: opts}

	message, _ := NewMsg("{\"jid\":\"2\",\"retry\":3,\"retry_count\":3}")

	wares.build("prod:myqueue", mgr, panickingFunc)(message)

	count, _ := opts.client.ZCard(ctx, retryQueue(opts.Namespace)).Result()
	assert.Equal(t, int64(0), count)
}
