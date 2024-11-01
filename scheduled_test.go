package workers

import (
	"context"
	"testing"

	"github.com/digitalocean/go-workers2/storage"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestScheduled(t *testing.T) {
	ctx := context.Background()

	opts, err := SetupDefaultTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	scheduled := newScheduledWorker(opts)

	rc := opts.client

	now := nowToSecondsWithNanoPrecision()

	message1, _ := NewMsg("{\"queue\":\"default\",\"foo\":\"bar1\"}")
	message2, _ := NewMsg("{\"queue\":\"myqueue\",\"foo\":\"bar2\"}")
	message3, _ := NewMsg("{\"queue\":\"default\",\"foo\":\"bar3\"}")

	rc.ZAdd(ctx, retryQueue(opts.Namespace), redis.Z{Score: now - 60.0, Member: message1.ToJson()}).Result()
	rc.ZAdd(ctx, retryQueue(opts.Namespace), redis.Z{Score: now - 10.0, Member: message2.ToJson()}).Result()
	rc.ZAdd(ctx, retryQueue(opts.Namespace), redis.Z{Score: now + 60.0, Member: message3.ToJson()}).Result()

	scheduled.poll()

	defaultCount, _ := rc.LLen(ctx, "prod:queue:default").Result()
	myqueueCount, _ := rc.LLen(ctx, "prod:queue:myqueue").Result()
	pending, _ := rc.ZCard(ctx, retryQueue(opts.Namespace)).Result()

	assert.Equal(t, int64(1), defaultCount)
	assert.Equal(t, int64(1), myqueueCount)
	assert.Equal(t, int64(1), pending)
}

func retryQueue(namespace string) string {
	return namespace + storage.RetryKey
}
