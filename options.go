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

type FailoverStrategy int

const (
	NoFailover FailoverStrategy = iota
	ActivePassiveFailover
	ActiveActiveFailover
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

	FailoverStrategy FailoverStrategy

	// One or more managers can belong to a cluster
	// Active/passive failover will failover by cluster
	ClusterName     string
	ClusterPriority float64

	// Log
	Logger *log.Logger

	client *redis.Client
	store  storage.Store
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
