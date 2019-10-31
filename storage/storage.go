package storage

import (
	"time"
)

// TODO(wtlangford): Check if the value of these keys are Sidekiq-compatible
const (
	RetryKey         = "goretry"
	ScheduledJobsKey = "schedule"
)

type StorageError string

func (e StorageError) Error() string { return string(e) }

const NoMessage = StorageError("no message")

type Stats struct {
	Processed int64
	Failed    int64
	Retries   int64
	Enqueued  map[string]int64
}

type Store interface {

	// General queue operations
	CreateQueue(queue string) error
	ListMessages(queue string) ([]string, error)
	AcknowledgeMessage(queue string, message string) error
	EnqueueMessage(queue string, priority float64, message string) error
	EnqueueMessageNow(queue string, message string) error
	DequeueMessage(queue string, inprogressQueue string, timeout time.Duration) (string, error)

	// Special purpose queue operations
	EnqueueScheduledMessage(priority float64, message string) error
	DequeueScheduledMessage(priority float64) (string, error)

	EnqueueRetriedMessage(priority float64, message string) error
	DequeueRetriedMessage(priority float64) (string, error)

	// Stats
	IncrementStats(metric string) error
	GetAllStats(queues []string) (*Stats, error)
}
