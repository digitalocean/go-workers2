package workers

import (
	"crypto/tls"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/friesencr/go-workers2/storage"
	"github.com/redis/go-redis/v9"
)

const (
	defaultHeartbeatInterval = 5 * time.Second

	defaultHeartbeatTTL = 60 * time.Second
)

// Options contains the set of configuration options for a manager and/or producer
type Options struct {
	ProcessID    string
	Namespace    string
	PollInterval time.Duration
	Database     int
	Password     string
	Username     string
	PoolSize     int

	// Provide one of ServerAddr or (SentinelAddrs + RedisMasterName)
	ServerAddr      string
	SentinelAddrs   string
	RedisMasterName string
	RedisTLSConfig  *tls.Config

	// Optional display name used when displaying manager stats
	ManagerDisplayName   string
	ManagerStartInactive bool

	// Define Heartbeat to enable heartbeat
	Heartbeat *HeartbeatOptions

	// DeadMaxJobs is the maximum number of jobs kept in the dead set.
	// Zero uses storage.DeadMaxJobs (10000).
	DeadMaxJobs int64
	// DeadMaxAge is how long a dead job is retained.
	// Zero uses storage.DeadMaxAge (180 days).
	DeadMaxAge time.Duration

	// Log
	Logger *slog.Logger

	client *redis.Client
	store  storage.Store
}

func (o *Options) Client() *redis.Client {
	return o.client
}

type HeartbeatOptions struct {
	// Optional heartbeat interval config
	Interval time.Duration

	// redis eviction ttl config
	HeartbeatTTL time.Duration

	PrioritizedManager *PrioritizedManagerOptions
}

type PrioritizedManagerOptions struct {
	ManagerPriority     int
	TotalActiveManagers int
}

func processOptions(options Options) (Options, error) {
	options, err := validateGeneralOptions(options)
	if err != nil {
		return Options{}, err
	}

	//redis options
	if options.PoolSize == 0 {
		options.PoolSize = 1
	}
	redisIdleTimeout := 240 * time.Second

	if options.ServerAddr != "" {
		options.client = redis.NewClient(&redis.Options{
			ConnMaxIdleTime: redisIdleTimeout,
			Password:        options.Password,
			Username:        options.Username,
			DB:              options.Database,
			PoolSize:        options.PoolSize,
			Addr:            options.ServerAddr,
			TLSConfig:       options.RedisTLSConfig,
		})
	} else if options.SentinelAddrs != "" {
		if options.RedisMasterName == "" {
			return Options{}, errors.New("Sentinel configuration requires a master name")
		}

		options.client = redis.NewFailoverClient(&redis.FailoverOptions{
			ConnMaxIdleTime: redisIdleTimeout,
			Password:        options.Password,
			Username:        options.Username,
			DB:              options.Database,
			PoolSize:        options.PoolSize,
			SentinelAddrs:   strings.Split(options.SentinelAddrs, ","),
			MasterName:      options.RedisMasterName,
			TLSConfig:       options.RedisTLSConfig,
		})
	} else {
		return Options{}, errors.New("Options requires either the Server or Sentinels option")
	}

	if options.Logger == nil {
		options.Logger = slog.Default()
	}

	options.store = storage.NewRedisStore(options.Namespace, options.client, options.Logger, options.DeadMaxJobs, options.DeadMaxAge)

	if options.Heartbeat != nil {
		if options.Heartbeat.Interval <= 0 {
			options.Heartbeat.Interval = defaultHeartbeatInterval
		}
		if options.Heartbeat.HeartbeatTTL <= 0 {
			options.Heartbeat.HeartbeatTTL = defaultHeartbeatTTL
		}
	}

	return options, nil
}

func processOptionsWithRedisClient(options Options, client *redis.Client) (Options, error) {
	options, err := validateGeneralOptions(options)
	if err != nil {
		return Options{}, err
	}

	if client == nil {
		return Options{}, errors.New("redis client is nil; Redis client is not configured")
	}

	options.client = client

	if options.Logger == nil {
		options.Logger = slog.Default()
	}

	options.store = storage.NewRedisStore(options.Namespace, options.client, options.Logger, options.DeadMaxJobs, options.DeadMaxAge)

	return options, nil
}

func validateGeneralOptions(options Options) (Options, error) {
	if options.ProcessID == "" {
		return Options{}, errors.New("options requires a ProcessID, which uniquely identifies this instance")
	}

	if options.Namespace != "" {
		options.Namespace += ":"
	}

	if options.PollInterval <= 0 {
		options.PollInterval = 15 * time.Second
	}

	if options.DeadMaxJobs <= 0 {
		options.DeadMaxJobs = storage.DeadMaxJobs
	}
	if options.DeadMaxAge <= 0 {
		options.DeadMaxAge = storage.DeadMaxAge
	}

	if options.Heartbeat != nil &&
		options.Heartbeat.Interval >= options.Heartbeat.HeartbeatTTL {
		return Options{}, errors.New("invalid heartbeat configuration, heartbeat interval longer than or equal to heartbeat tll")
	}

	return options, nil
}
