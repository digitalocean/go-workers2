package workers

import (
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
)

// TODO(wtlangford): Check if the value of these keys are Sidekiq-compatible
const (
	retryKey         = "goretry"
	scheduledJobsKey = "schedule"
)

type scheduledWorker struct {
	opts Options
	keys []string
	done chan bool
}

func (s *scheduledWorker) run() {
	for {
		select {
		case <-s.done:
			return
		default:
		}

		s.poll()

		time.Sleep(time.Duration(s.opts.PollInterval) * time.Second)
	}
}

func (s *scheduledWorker) quit() {
	close(s.done)
}

func (s *scheduledWorker) poll() {
	now := nowToSecondsWithNanoPrecision()

	for _, key := range s.keys {
		key = s.opts.Namespace + key
		for {
			messages, _ := s.opts.client.ZRangeByScore(key, redis.ZRangeBy{
				Min:    "-inf",
				Max:    strconv.FormatFloat(now, 'f', -1, 64),
				Offset: 0,
				Count:  1,
			}).Result()

			if len(messages) == 0 {
				break
			}

			message, _ := NewMsg(messages[0])

			if removed, _ := s.opts.client.ZRem(key, messages[0]).Result(); removed != 0 {
				queue, _ := message.Get("queue").String()
				queue = strings.TrimPrefix(queue, s.opts.Namespace)
				message.Set("enqueued_at", nowToSecondsWithNanoPrecision())
				s.opts.client.LPush(s.opts.Namespace+"queue:"+queue, message.ToJson()).Result()
			}
		}
	}
}

func newScheduledWorker(opts Options) *scheduledWorker {
	return &scheduledWorker{
		opts: opts,
		keys: []string{retryKey, scheduledJobsKey},
		done: make(chan bool),
	}
}
