package storage

import (
	"context"
	"time"
)

// TODO(wtlangford): Check if the value of these keys are Sidekiq-compatible
const (
	RetryKey         = "goretry"
	ScheduledJobsKey = "schedule"
)

// StorageError is used to return errors from the storage layer
type StorageError string

func (e StorageError) Error() string { return string(e) }

// list of known errors
const (
	NoMessage = StorageError("no message")
)

// Stats has all the stats related to a manager
type Stats struct {
	Processed  int64
	Failed     int64
	RetryCount int64
	Enqueued   map[string]int64
}

// Retries has the list of messages in the retry queue
type Retries struct {
	TotalRetryCount int64
	RetryJobs       []string
}

// Heartbeat is used for the ruby sidekiq web ui
type Heartbeat struct {
	Identity string

	Beat  time.Time
	Quiet bool
	Busy  int
	RttUS int
	RSS   int64
	Info  string
	Pid   int

	WorkerMessages map[string]string
}

// Store is the interface for storing and retrieving data
type Store interface {

	// General queue operations
	CreateQueue(ctx context.Context, queue string) error
	ListMessages(ctx context.Context, queue string) ([]string, error)
	AcknowledgeMessage(ctx context.Context, queue string, message string) error
	EnqueueMessage(ctx context.Context, queue string, priority float64, message string) error
	EnqueueMessageNow(ctx context.Context, queue string, message string) error
	DequeueMessage(ctx context.Context, queue string, inprogressQueue string, timeout time.Duration) (string, error)

	// Special purpose queue operations
	EnqueueScheduledMessage(ctx context.Context, priority float64, message string) error
	DequeueScheduledMessage(ctx context.Context, priority float64) (string, error)

	EnqueueRetriedMessage(ctx context.Context, priority float64, message string) error
	DequeueRetriedMessage(ctx context.Context, priority float64) (string, error)

	// Stats
	IncrementStats(ctx context.Context, metric string) error
	GetAllStats(ctx context.Context, queues []string) (*Stats, error)

	// Heartbeat
	SendHeartbeat(ctx context.Context, heartbeat *Heartbeat) error
	RemoveHeartbeat(ctx context.Context, heartbeat *Heartbeat) error

	// Retries
	GetAllRetries(ctx context.Context) (*Retries, error)
}
