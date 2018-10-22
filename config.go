package workers

import (
	"errors"
	"strings"
	"time"

	"github.com/go-redis/redis"
)

type config struct {
	processId    string
	Namespace    string
	PollInterval int
	Client       *redis.Client
	Fetch        func(queue string) Fetcher
}

type Options struct {
	ProcessID    string
	Namespace    string
	PollInterval int
	Database     int
	Password     string
	PoolSize     int

	// Provide one of ServerAddr or SentinelAddrs
	ServerAddr    string
	SentinelAddrs string
}

var Config *config

func Configure(options Options) error {
	if options.ProcessID == "" {
		return errors.New("Configure requires a ProcessID, which uniquely identifies this instance")
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

	var rc *redis.Client
	if options.ServerAddr != "" {
		rc = redis.NewClient(&redis.Options{
			IdleTimeout: redisIdleTimeout,
			Password:    options.Password,
			DB:          options.Database,
			PoolSize:    options.PoolSize,
			Addr:        options.ServerAddr,
		})
	} else if options.SentinelAddrs != "" {
		rc = redis.NewFailoverClient(&redis.FailoverOptions{
			IdleTimeout:   redisIdleTimeout,
			Password:      options.Password,
			DB:            options.Database,
			PoolSize:      options.PoolSize,
			SentinelAddrs: strings.Split(options.SentinelAddrs, ","),
		})
	} else {
		return errors.New("Configure requires either the Server or Sentinels option")
	}

	Config = &config{
		processId:    options.ProcessID,
		Namespace:    options.Namespace,
		PollInterval: options.PollInterval,
		Client:       rc,
		Fetch: func(queue string) Fetcher {
			return NewFetch(queue, make(chan *Msg), make(chan bool))
		},
	}
	return nil
}
