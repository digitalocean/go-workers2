package workers

import (
	"crypto/tls"
	"errors"
	"log"
	"os"
	"strings"
	"time"

	"github.com/digitalocean/go-workers2/storage"
	"github.com/go-redis/redis/v8"
)

const (
	defaultHeartbeatInterval                = 5 * time.Second
	defaultHeartbeatTaskRunnerEvictInterval = 60 * time.Second
	defaultHeartbeatClusterEvictInterval    = 60 * time.Second

	defaultHeartbeatManagerTTL = 60 * time.Second
)

// Options contains the set of configuration options for a manager and/or producer
type Options struct {
	ProcessID    string
	Namespace    string
	PollInterval time.Duration
	Database     int
	Password     string
	PoolSize     int

	// Provide one of ServerAddr or (SentinelAddrs + RedisMasterName)
	ServerAddr      string
	SentinelAddrs   string
	RedisMasterName string
	RedisTLSConfig  *tls.Config

	// Optional display name used when displaying manager stats
	ManagerDisplayName string

	// Define Heartbeat to enable heartbeat
	Heartbeat *HeartbeatOptions
	// Define ActivePassiveFailover to enable active passive failover
	ActivePassiveFailover *ActivePassFailoverOptions

	// Log
	Logger *log.Logger

	client *redis.Client
	store  storage.Store
}

type HeartbeatOptions struct {
	// Optional heartbeat interval config
	Interval time.Duration

	// Optional redis eviction intervals and ttl config
	TaskRunnerEvictInterval time.Duration
	ClusterEvictInterval    time.Duration
	ManagerTTL              time.Duration
}

// ActivePassFailoverOptions are config options if active/passive failover of clusters of managers
type ActivePassFailoverOptions struct {
	ClusterID       string
	ClusterPriority float64
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
			IdleTimeout: redisIdleTimeout,
			Password:    options.Password,
			DB:          options.Database,
			PoolSize:    options.PoolSize,
			Addr:        options.ServerAddr,
			TLSConfig:   options.RedisTLSConfig,
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
			TLSConfig:     options.RedisTLSConfig,
		})
	} else {
		return Options{}, errors.New("Options requires either the Server or Sentinels option")
	}

	if options.Logger == nil {
		options.Logger = log.New(os.Stdout, "go-workers2: ", log.Ldate|log.Lmicroseconds)
	}

	redisStore := storage.NewRedisStore(options.Namespace, options.client, options.Logger)
	options.store = redisStore

	if options.Heartbeat != nil {
		if options.Heartbeat.Interval <= 0 {
			options.Heartbeat.Interval = defaultHeartbeatInterval
		}
		if options.Heartbeat.TaskRunnerEvictInterval <= 0 {
			options.Heartbeat.TaskRunnerEvictInterval = defaultHeartbeatTaskRunnerEvictInterval
		}
		if options.Heartbeat.ClusterEvictInterval <= 0 {
			options.Heartbeat.ClusterEvictInterval = defaultHeartbeatClusterEvictInterval
		}
		if options.Heartbeat.ManagerTTL <= 0 {
			options.Heartbeat.ManagerTTL = defaultHeartbeatManagerTTL
		}
		if options.ActivePassiveFailover != nil && options.Heartbeat == nil {
			return Options{}, errors.New("Active/passive failover requires heartbeat to be enabled")
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
		return Options{}, errors.New("Redis client is nil; Redis client is not configured")
	}

	options.client = client

	if options.Logger == nil {
		options.Logger = log.New(os.Stdout, "go-workers2: ", log.Ldate|log.Lmicroseconds)
	}

	redisStore := storage.NewRedisStore(options.Namespace, options.client, options.Logger)
	options.store = redisStore

	return options, nil
}

func validateGeneralOptions(options Options) (Options, error) {
	if options.ProcessID == "" {
		return Options{}, errors.New("Options requires a ProcessID, which uniquely identifies this instance")
	}

	if options.Namespace != "" {
		options.Namespace += ":"
	}

	if options.PollInterval <= 0 {
		options.PollInterval = 15 * time.Second
	}

	return options, nil
}
