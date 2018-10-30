package workers

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestProcessedStats(t *testing.T) {
	namespace := "prod"
	opts, err := setupTestOptionsWithNamespace(namespace)
	assert.NoError(t, err)
	mgr := &Manager{opts: opts}

	rc := opts.client

	count, _ := rc.Get("prod:stat:processed").Result()
	countInt, _ := strconv.ParseInt(count, 10, 64)
	assert.Equal(t, int64(0), countInt)

	layout := "2006-01-02"
	dayCount, _ := rc.Get("prod:stat:processed:" + time.Now().UTC().Format(layout)).Result()
	dayCountInt, _ := strconv.ParseInt(dayCount, 10, 64)
	assert.Equal(t, int64(0), dayCountInt)

	message, _ := NewMsg("{\"jid\":\"2\",\"retry\":true}")
	NewMiddlewares(StatsMiddleware).build("myqueue", mgr, func(m *Msg) error {
		// noop
		return nil
	})(message)

	count, _ = rc.Get("prod:stat:processed").Result()
	countInt, _ = strconv.ParseInt(count, 10, 64)
	assert.Equal(t, int64(1), countInt)

	dayCount, _ = rc.Get("prod:stat:processed:" + time.Now().UTC().Format(layout)).Result()
	dayCountInt, _ = strconv.ParseInt(dayCount, 10, 64)
	assert.Equal(t, int64(1), dayCountInt)
}

func TestFailedStats(t *testing.T) {
	namespace := "prod"
	opts, err := setupTestOptionsWithNamespace(namespace)
	assert.NoError(t, err)
	mgr := &Manager{opts: opts}

	rc := opts.client

	layout := "2006-01-02"

	count, _ := rc.Get("prod:stat:failed").Result()
	countInt, _ := strconv.ParseInt(count, 10, 64)
	assert.Equal(t, int64(0), countInt)

	dayCount, _ := rc.Get("prod:stat:failed:" + time.Now().UTC().Format(layout)).Result()
	dayCountInt, _ := strconv.ParseInt(dayCount, 10, 64)
	assert.Equal(t, int64(0), dayCountInt)

	message, _ := NewMsg("{\"jid\":\"2\",\"retry\":true}")

	var job = func(message *Msg) error {
		panic(errors.New("AHHHH"))
	}

	NewMiddlewares(StatsMiddleware).build("myqueue", mgr, job)(message)

	count, _ = rc.Get("prod:stat:failed").Result()
	countInt, _ = strconv.ParseInt(count, 10, 64)
	assert.Equal(t, int64(1), countInt)

	dayCount, _ = rc.Get("prod:stat:failed:" + time.Now().UTC().Format(layout)).Result()
	dayCountInt, _ = strconv.ParseInt(dayCount, 10, 64)
	assert.Equal(t, int64(1), dayCountInt)
}
