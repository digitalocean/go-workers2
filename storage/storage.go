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
	AcknowledgeMessage(queue string, message string) error
	EnqueueMessage(queue string, priority float64, message string) error
	EnqueueMessageNow(queue string, message string) error
	FetchMessage(queue string, inprogressQueue string, timeout time.Duration) (string, error)

	ScheduleMessage(priority float64, message string) error
	FetchScheduledMessage(priority float64) (string, error)

	RetryMessage(priority float64, message string) error
	FetchRetriedMessage(priority float64) (string, error)

	CreateQueue(queue string) error
	GetMessages(queue string) ([]string, error)

	IncrementStats(metric string) error
	GetStats(queues []string) (*Stats, error)
}
