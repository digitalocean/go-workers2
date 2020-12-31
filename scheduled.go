package workers

import (
	"context"
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

		time.Sleep(s.opts.PollInterval)
	}
}

func (s *scheduledWorker) quit() {
	close(s.done)
}

func (s *scheduledWorker) poll() {
	now := nowToSecondsWithNanoPrecision()

	for {
		rawMessage, err := s.opts.store.DequeueScheduledMessage(context.Background(), now)

		if err != nil {
			break
		}

		message, _ := NewMsg(rawMessage)
		queue, _ := message.Get("queue").String()
		queue = strings.TrimPrefix(queue, s.opts.Namespace)
		message.Set("enqueued_at", nowToSecondsWithNanoPrecision())

		s.opts.store.EnqueueMessageNow(context.Background(), queue, message.ToJson())
	}

	for {
		rawMessage, err := s.opts.store.DequeueRetriedMessage(context.Background(), now)

		if err != nil {
			break
		}

		message, _ := NewMsg(rawMessage)
		queue, _ := message.Get("queue").String()
		queue = strings.TrimPrefix(queue, s.opts.Namespace)
		message.Set("enqueued_at", nowToSecondsWithNanoPrecision())

		s.opts.store.EnqueueMessageNow(context.Background(), queue, message.ToJson())
	}
}

func newScheduledWorker(opts Options) *scheduledWorker {
	return &scheduledWorker{
		opts: opts,
		done: make(chan bool),
	}
}
