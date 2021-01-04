package workers

import (
	"fmt"
	"log"
	"sync"
	"time"
)

type taskRunner struct {
	stop       chan bool
	handler    JobFunc
	currentMsg *Msg
	lock       sync.RWMutex
	logger     *log.Logger
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

			if err := w.process(msg); err != nil {
				w.logger.Println("ERR:", err)
			}

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

func newTaskRunner(logger *log.Logger, handler JobFunc) *taskRunner {
	return &taskRunner{
		handler: handler,
		stop:    make(chan bool),
		logger:  logger,
	}
}
