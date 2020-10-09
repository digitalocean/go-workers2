package workers

import (
	"fmt"
	"math"
	"math/rand"
	"time"
)

// A function that gets executed when retry attempts have been exhausted.
type RetriesExhaustedFunc func(queue string, message *Msg, err error)

const (
	// DefaultRetryMax is default for max number of retries for a job
	DefaultRetryMax = 25

	// RetryTimeFormat is default for retry time format
	RetryTimeFormat = "2006-01-02 15:04:05 MST"
)

func retryProcessError(queue string, mgr *Manager, message *Msg, err error) error {
	if !retry(message) {
		return err
	}
	if retryCount(message) < retryMax(message) {
		message.Set("queue", queue)
		message.Set("error_message", fmt.Sprintf("%v", err))
		retryCount := incrementRetry(message)

		waitDuration := durationToSecondsWithNanoPrecision(
			time.Duration(
				secondsToDelay(retryCount),
			) * time.Second,
		)

		err = mgr.opts.store.EnqueueRetriedMessage(nowToSecondsWithNanoPrecision()+waitDuration, message.ToJson())

		// If we can't add the job to the retry queue,
		// then we shouldn't acknowledge the job, otherwise
		// it'll disappear into the void.
		if err != nil {
			message.ack = false
		}
	} else {
		for _, retriesExhaustedHandler := range mgr.retriesExhaustedHandlers {
			retriesExhaustedHandler(queue, message, err)
		}
	}
	return err
}

// RetryMiddleware middleware that allows retries for jobs failures
func RetryMiddleware(queue string, mgr *Manager, next JobFunc) JobFunc {
	return func(message *Msg) (err error) {
		defer func() {
			if e := recover(); e != nil {
				var ok bool
				if err, ok = e.(error); !ok {
					err = fmt.Errorf("%v", e)
				}

				if err != nil {
					err = retryProcessError(queue, mgr, message, err)
				}
			}

		}()

		err = next(message)
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
	} else if _, err := message.Get("retry").Int(); err == nil {
		retry = true
	}
	return retry
}

func retryCount(message *Msg) int {
	count, _ := message.Get("retry_count").Int()
	return count
}

func retryMax(message *Msg) int {
	max := DefaultRetryMax
	if param, err := message.Get("retry").Int(); err == nil {
		max = param
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
