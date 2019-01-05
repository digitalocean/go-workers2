package workers

import (
	"strings"
	"time"
)

type scheduledWorker struct {
	opts Options
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

	for {
		rawMessage, err := s.opts.store.FetchScheduledMessage(now)

		if err != nil {
			break
		}

		message, _ := NewMsg(rawMessage)
		queue, _ := message.Get("queue").String()
		queue = strings.TrimPrefix(queue, s.opts.Namespace)
		message.Set("enqueued_at", nowToSecondsWithNanoPrecision())

		s.opts.store.EnqueueMessageNow(queue, message.ToJson())
	}

	for {
		rawMessage, err := s.opts.store.FetchRetriedMessage(now)

		if err != nil {
			break
		}

		message, _ := NewMsg(rawMessage)
		queue, _ := message.Get("queue").String()
		queue = strings.TrimPrefix(queue, s.opts.Namespace)
		message.Set("enqueued_at", nowToSecondsWithNanoPrecision())

		s.opts.store.EnqueueMessageNow(queue, message.ToJson())
	}
}

func newScheduledWorker(opts Options) *scheduledWorker {
	return &scheduledWorker{
		opts: opts,
		done: make(chan bool),
	}
}
