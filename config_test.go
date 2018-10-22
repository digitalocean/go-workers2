package workers

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

var recoverOnPanic = func(f func()) (err error) {
	defer func() {
		if cause := recover(); cause != nil {
			var ok bool
			err, ok = cause.(error)
			if !ok {
				err = fmt.Errorf("not error; %v", cause)
			}
		}
	}()

	f()

	return
}

func TestRedisPoolConfig(t *testing.T) {
	// Tests redis pool size which defaults to 1
	Configure(Options{
		ServerAddr: "localhost:6379",
		ProcessID:  "2",
	})
	assert.Equal(t, 1, Config.Client.Options().PoolSize)

	Configure(Options{
		ServerAddr: "localhost:6379",
		ProcessID:  "1",
		PoolSize:   20,
	})

	assert.Equal(t, 20, Config.Client.Options().PoolSize)
}

func TestCustomProcessConfig(t *testing.T) {
	Configure(Options{
		ServerAddr: "localhost:6379",
		ProcessID:  "1",
	})
	assert.Equal(t, "1", Config.processId)

	Configure(Options{
		ServerAddr: "localhost:6379",
		ProcessID:  "2",
	})
	assert.Equal(t, "2", Config.processId)
}

func TestRequiresRedisConfig(t *testing.T) {
	err := Configure(Options{ProcessID: "2"})

	assert.Error(t, err, "Configure requires either the Server or Sentinels option")
}

func TestRequiresProcessConfig(t *testing.T) {
	err := Configure(Options{ServerAddr: "localhost:6379"})

	assert.Error(t, err, "Configure requires a ProcessID, which uniquely identifies this instance")
}

func TestAddsColonToNamespace(t *testing.T) {
	Configure(Options{
		ServerAddr: "localhost:6379",
		ProcessID:  "1",
	})
	assert.Equal(t, "", Config.Namespace)

	Configure(Options{
		ServerAddr: "localhost:6379",
		ProcessID:  "1",
		Namespace:  "prod",
	})
	assert.Equal(t, "prod:", Config.Namespace)
}

func TestDefaultPollIntervalConfig(t *testing.T) {
	Configure(Options{
		ServerAddr: "localhost:6379",
		ProcessID:  "1",
	})

	assert.Equal(t, 15, Config.PollInterval)

	Configure(Options{
		ServerAddr:   "localhost:6379",
		ProcessID:    "1",
		PollInterval: 1,
	})

	assert.Equal(t, 1, Config.PollInterval)
}

func TestSentinelConfig(t *testing.T) {
	Configure(Options{
		SentinelAddrs: "localhost:26379,localhost:46379",
		ProcessID:     "1",
		PollInterval:  1,
	})

	assert.Equal(t, "FailoverClient", Config.Client.Options().Addr)
}
