package workers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRedisPoolConfig(t *testing.T) {
	// Tests redis pool size which defaults to 1
	opts, err := processOptions(Options{
		ServerAddr: "localhost:6379",
		ProcessID:  "2",
	})

	assert.NoError(t, err)
	assert.Equal(t, 1, opts.client.Options().PoolSize)

	opts, err = processOptions(Options{
		ServerAddr: "localhost:6379",
		ProcessID:  "1",
		PoolSize:   20,
	})

	assert.NoError(t, err)
	assert.Equal(t, 20, opts.client.Options().PoolSize)
}

func TestCustomProcessConfig(t *testing.T) {
	opts, err := processOptions(Options{
		ServerAddr: "localhost:6379",
		ProcessID:  "1",
	})

	assert.NoError(t, err)
	assert.Equal(t, "1", opts.ProcessID)

	opts, err = processOptions(Options{
		ServerAddr: "localhost:6379",
		ProcessID:  "2",
	})

	assert.NoError(t, err)
	assert.Equal(t, "2", opts.ProcessID)
}

func TestRequiresRedisConfig(t *testing.T) {
	_, err := processOptions(Options{ProcessID: "2"})

	assert.Error(t, err, "Configure requires either the Server or Sentinels option")
}

func TestRequiresProcessConfig(t *testing.T) {
	_, err := processOptions(Options{ServerAddr: "localhost:6379"})

	assert.Error(t, err, "Configure requires a ProcessID, which uniquely identifies this instance")
}

func TestAddsColonToNamespace(t *testing.T) {
	opts, err := processOptions(Options{
		ServerAddr: "localhost:6379",
		ProcessID:  "1",
	})

	assert.NoError(t, err)
	assert.Equal(t, "", opts.Namespace)

	opts, err = processOptions(Options{
		ServerAddr: "localhost:6379",
		ProcessID:  "1",
		Namespace:  "prod",
	})

	assert.NoError(t, err)
	assert.Equal(t, "prod:", opts.Namespace)
}

func TestDefaultPollIntervalConfig(t *testing.T) {
	opts, err := processOptions(Options{
		ServerAddr: "localhost:6379",
		ProcessID:  "1",
	})

	assert.NoError(t, err)
	assert.Equal(t, 15, opts.PollInterval)

	opts, err = processOptions(Options{
		ServerAddr:   "localhost:6379",
		ProcessID:    "1",
		PollInterval: 1,
	})

	assert.NoError(t, err)
	assert.Equal(t, 1, opts.PollInterval)
}

func TestSentinelConfigGood(t *testing.T) {
	opts, err := processOptions(Options{
		SentinelAddrs:   "localhost:26379,localhost:46379",
		RedisMasterName: "123",
		ProcessID:       "1",
		PollInterval:    1,
	})

	assert.NoError(t, err)
	assert.Equal(t, "FailoverClient", opts.client.Options().Addr)
}

func TestSentinelConfigNoMaster(t *testing.T) {
	_, err := processOptions(Options{
		SentinelAddrs: "localhost:26379,localhost:46379",
		ProcessID:     "1",
		PollInterval:  1,
	})

	assert.Error(t, err)
}
