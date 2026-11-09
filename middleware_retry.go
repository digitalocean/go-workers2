package workers

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"
)

// RetriesExhaustedFunc gets executed when retry attempts have been exhausted, or when a job with retries disabled fails.
type RetriesExhaustedFunc func(queue string, message *Msg, err error)

const (
	// DefaultRetryMax is default for max number of retries for a job
	DefaultRetryMax = 25

	// RetryTimeFormat is default for retry time format
	RetryTimeFormat = "2006-01-02 15:04:05 MST"
)

func retryProcessError(queue string, mgr *Manager, message *Msg, err error) error {
	if retry(message) && retryCount(message) < retryMax(message) {
		message.Set("queue", queue)
		message.Set("error_message", fmt.Sprintf("%v", err))
		retryCount := incrementRetry(message)

		waitDuration := durationToSecondsWithNanoPrecision(
			time.Duration(
				secondsToDelay(retryCount),
			) * time.Second,
		)

		retryErr := mgr.opts.store.EnqueueRetriedMessage(context.Background(), nowToSecondsWithNanoPrecision()+waitDuration, message.ToJson())

		// If we can't add the job to the retry queue,
		// then we shouldn't acknowledge the job, otherwise
		// it'll disappear into the void.
		if retryErr != nil {
			mgr.logger.Error("retry enqueue failed", "queue", queue, "jid", message.Jid(), "error", retryErr)
			message.ack = false
		}
	} else {
		message.Set("queue", queue)
		message.Set("error_message", fmt.Sprintf("%v", err))

		if retainExhausted(mgr, queue, message) {
			deadErr := mgr.opts.store.EnqueueDeadMessage(context.Background(), nowToSecondsWithNanoPrecision(), message.ToJson())

			// If we can't add the job to the dead set,
			// then we shouldn't acknowledge the job, otherwise
			// it'll disappear into the void. The job stays in the
			// in-progress queue and is delivered again, so a job that
			// isn't idempotent can run again from here, and the
			// handlers below can see the same job twice.
			if deadErr != nil {
				mgr.logger.Error("dead enqueue failed", "queue", queue, "jid", message.Jid(), "error", deadErr)
				message.ack = false
			}
		}

		for _, retriesExhaustedHandler := range mgr.getRetriesExhaustedHandlers() {
			retriesExhaustedHandler(queue, message, err)
		}
	}
	return err
}

// retainExhausted reports whether an exhausted job should be kept in the dead set.
// Precedence: message retain_exhausted, then class options, then queue options, then true.
func retainExhausted(mgr *Manager, queue string, message *Msg) bool {
	if v, err := message.Get("retain_exhausted").Bool(); err == nil {
		return v
	}
	if mgr != nil {
		if v, ok := mgr.classRetainExhausted(message.Class()); ok {
			return v
		}
		bare := strings.TrimPrefix(queue, mgr.opts.Namespace)
		if v, ok := mgr.queueRetainExhausted(bare); ok {
			return v
		}
	}
	return true
}

// RetryMiddleware middleware that allows retries for jobs failures
func RetryMiddleware(queue string, mgr *Manager, next JobFunc) JobFunc {
	return func(message *Msg) (err error) {
		// Only the job's panic is recovered. Recovering one from a retries exhausted handler
		// would process the failure again, writing a second dead entry and calling every handler twice.
		err = func() (err error) {
			defer func() {
				if e := recover(); e != nil {
					var ok bool
					if err, ok = e.(error); !ok {
						err = fmt.Errorf("%v", e)
					}
				}

			}()

			return next(message)
		}()

		if err != nil {
			err = retryProcessError(queue, mgr, message, err)
		}

		return
	}
}

func retry(message *Msg) bool {
	retry := false

	if param, err := message.Get("retry").Bool(); err == nil {
		retry = param
	}

	return retry
}

func retryCount(message *Msg) int {
	count, _ := message.Get("retry_count").Int()
	return count
}

func retryMax(message *Msg) int {
	max := DefaultRetryMax
	if messageRetryMax, err := message.Get("retry_max").Int(); err == nil && messageRetryMax >= 0 {
		max = messageRetryMax
	}
	return max
}

func incrementRetry(message *Msg) (retryCount int) {
	retryCount = 0

	if count, err := message.Get("retry_count").Int(); err != nil {
		message.Set("failed_at", time.Now().UTC().Format(RetryTimeFormat))
	} else {
		message.Set("retried_at", time.Now().UTC().Format(RetryTimeFormat))
		retryCount = count + 1
	}

	message.Set("retry_count", retryCount)

	return
}

func secondsToDelay(count int) int {
	power := math.Pow(float64(count), 4)
	return int(power) + 15 + (rand.Intn(30) * (count + 1))
}
