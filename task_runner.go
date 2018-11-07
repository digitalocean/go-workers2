package workers

import (
	"fmt"
	"sync"
	"time"
)

type taskRunner struct {
	stop       chan bool
	handler    JobFunc
	currentMsg *Msg
	lock       sync.RWMutex
}

func (w *taskRunner) quit() {
	close(w.stop)
}

func (w *taskRunner) work(messages <-chan *Msg, done chan<- *Msg, ready chan<- bool) {
	for {
		select {
		case msg := <-messages:
			msg.startedAt = time.Now().UTC().Unix()

			w.lock.Lock()
			w.currentMsg = msg
			w.lock.Unlock()

			w.process(msg)

			w.lock.Lock()
			w.currentMsg = nil
			w.lock.Unlock()

			done <- msg

		case ready <- true:
			// Signaled to fetcher that we're
			// ready to accept a message
		case <-w.stop:
			return
		}
	}
}

func (w *taskRunner) process(message *Msg) (err error) {
	defer func() {
		if e := recover(); e != nil {
			var ok bool
			if err, ok = e.(error); !ok {
				err = fmt.Errorf("%v", e)
			}
		}
	}()

	return w.handler(message)
}

func (w *taskRunner) inProgressMessage() *Msg {
	w.lock.RLock()
	defer w.lock.RUnlock()
	return w.currentMsg
}

func newTaskRunner(handler JobFunc) *taskRunner {
	return &taskRunner{
		handler: handler,
		stop:    make(chan bool),
	}
}
