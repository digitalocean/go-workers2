package workers

import (
	"errors"
	"strings"
	"time"

	"github.com/go-redis/redis"
)

type Options struct {
	ProcessID    string
	Namespace    string
	PollInterval int
	Database     int
	Password     string
	PoolSize     int

	// Provide one of ServerAddr or (SentinelAddrs + RedisMasterName)
	ServerAddr      string
	SentinelAddrs   string
	RedisMasterName string

	// Optional display name used when displaying manager stats
	ManagerDisplayName string

	client *redis.Client
}

func processOptions(options Options) (Options, error) {
	if options.ProcessID == "" {
		return Options{}, errors.New("Options requires a ProcessID, which uniquely identifies this instance")
	}

	if options.Namespace != "" {
		options.Namespace += ":"
	}
	if options.PoolSize == 0 {
		options.PoolSize = 1
	}
	if options.PollInterval <= 0 {
		options.PollInterval = 15
	}

	redisIdleTimeout := 240 * time.Second

	if options.ServerAddr != "" {
		options.client = redis.NewClient(&redis.Options{
			IdleTimeout: redisIdleTimeout,
			Password:    options.Password,
			DB:          options.Database,
			PoolSize:    options.PoolSize,
			Addr:        options.ServerAddr,
		})
	} else if options.SentinelAddrs != "" {
		if options.RedisMasterName == "" {
			return Options{}, errors.New("Sentinel configuration requires a master name")
		}

		options.client = redis.NewFailoverClient(&redis.FailoverOptions{
			IdleTimeout:   redisIdleTimeout,
			Password:      options.Password,
			DB:            options.Database,
			PoolSize:      options.PoolSize,
			SentinelAddrs: strings.Split(options.SentinelAddrs, ","),
			MasterName:    options.RedisMasterName,
		})
	} else {
		return Options{}, errors.New("Options requires either the Server or Sentinels option")
	}
	return options, nil
}
