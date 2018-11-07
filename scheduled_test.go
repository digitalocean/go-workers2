package workers

import (
	"testing"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
)

func TestScheduled(t *testing.T) {
	opts, err := setupTestOptionsWithNamespace("prod")
	assert.NoError(t, err)
	mgr := &Manager{opts: opts}
	scheduled := newScheduledWorker(opts)

	rc := opts.client

	now := nowToSecondsWithNanoPrecision()

	message1, _ := NewMsg("{\"queue\":\"default\",\"foo\":\"bar1\"}")
	message2, _ := NewMsg("{\"queue\":\"myqueue\",\"foo\":\"bar2\"}")
	message3, _ := NewMsg("{\"queue\":\"default\",\"foo\":\"bar3\"}")

	rc.ZAdd(mgr.RetryQueue(), redis.Z{Score: now - 60.0, Member: message1.ToJson()}).Result()
	rc.ZAdd(mgr.RetryQueue(), redis.Z{Score: now - 10.0, Member: message2.ToJson()}).Result()
	rc.ZAdd(mgr.RetryQueue(), redis.Z{Score: now + 60.0, Member: message3.ToJson()}).Result()

	scheduled.poll()

	defaultCount, _ := rc.LLen("prod:queue:default").Result()
	myqueueCount, _ := rc.LLen("prod:queue:myqueue").Result()
	pending, _ := rc.ZCard(mgr.RetryQueue()).Result()

	assert.Equal(t, int64(1), defaultCount)
	assert.Equal(t, int64(1), myqueueCount)
	assert.Equal(t, int64(1), pending)
}
