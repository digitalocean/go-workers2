package workers

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/go-redis/redis"
)

const (
	DefaultRetryMax = 25
	RetryTimeFormat = "2006-01-02 15:04:05 MST"
)

func retryProcessError(queue string, mgr *Manager, message *Msg, err error) error {
	if retry(message) {
		message.Set("queue", queue)
		message.Set("error_message", fmt.Sprintf("%v", err))
		retryCount := incrementRetry(message)

		waitDuration := durationToSecondsWithNanoPrecision(
			time.Duration(
				secondsToDelay(retryCount),
			) * time.Second,
		)

		rc := mgr.opts.client
		_, err = rc.ZAdd(mgr.RetryQueue(), redis.Z{
			Score:  nowToSecondsWithNanoPrecision() + waitDuration,
			Member: message.ToJson(),
		}).Result()

		// If we can't add the job to the retry queue,
		// then we shouldn't acknowledge the job, otherwise
		// it'll disappear into the void.
		if err != nil {
			message.ack = false
		}
	}
	return err
}

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
	max := DefaultRetryMax

	if param, err := message.Get("retry").Bool(); err == nil {
		retry = param
	} else if param, err := message.Get("retry").Int(); err == nil {
		max = param
		retry = true
	}

	count, _ := message.Get("retry_count").Int()

	return retry && count < max
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
