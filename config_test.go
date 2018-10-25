package workers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRedisPoolConfig(t *testing.T) {
	// Tests redis pool size which defaults to 1
	err := Configure(Options{
		ServerAddr: "localhost:6379",
		ProcessID:  "2",
	})

	assert.NoError(t, err)
	assert.Equal(t, 1, Config.Client.Options().PoolSize)

	err = Configure(Options{
		ServerAddr: "localhost:6379",
		ProcessID:  "1",
		PoolSize:   20,
	})

	assert.NoError(t, err)
	assert.Equal(t, 20, Config.Client.Options().PoolSize)
}

func TestCustomProcessConfig(t *testing.T) {
	err := Configure(Options{
		ServerAddr: "localhost:6379",
		ProcessID:  "1",
	})

	assert.NoError(t, err)
	assert.Equal(t, "1", Config.processId)

	err = Configure(Options{
		ServerAddr: "localhost:6379",
		ProcessID:  "2",
	})

	assert.NoError(t, err)
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
	err := Configure(Options{
		ServerAddr: "localhost:6379",
		ProcessID:  "1",
	})

	assert.NoError(t, err)
	assert.Equal(t, "", Config.Namespace)

	err = Configure(Options{
		ServerAddr: "localhost:6379",
		ProcessID:  "1",
		Namespace:  "prod",
	})

	assert.NoError(t, err)
	assert.Equal(t, "prod:", Config.Namespace)
}

func TestDefaultPollIntervalConfig(t *testing.T) {
	err := Configure(Options{
		ServerAddr: "localhost:6379",
		ProcessID:  "1",
	})

	assert.NoError(t, err)
	assert.Equal(t, 15, Config.PollInterval)

	err = Configure(Options{
		ServerAddr:   "localhost:6379",
		ProcessID:    "1",
		PollInterval: 1,
	})

	assert.NoError(t, err)
	assert.Equal(t, 1, Config.PollInterval)
}

func TestSentinelConfigGood(t *testing.T) {
	err := Configure(Options{
		SentinelAddrs:   "localhost:26379,localhost:46379",
		RedisMasterName: "123",
		ProcessID:       "1",
		PollInterval:    1,
	})

	assert.NoError(t, err)
	assert.Equal(t, "FailoverClient", Config.Client.Options().Addr)
}

func TestSentinelConfigNoMaster(t *testing.T) {
	err := Configure(Options{
		SentinelAddrs: "localhost:26379,localhost:46379",
		ProcessID:     "1",
		PollInterval:  1,
	})

	assert.Error(t, err)
}
