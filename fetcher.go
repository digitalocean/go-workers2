package workers

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/digitalocean/go-workers2/storage"
)

//Fetcher is an interface for managing work messages
type Fetcher interface {
	Queue() string
	InProgressQueue() string
	Fetch()
	Acknowledge(*Msg)
	Active(bool)
	IsActive() bool
	Ready() chan bool
	Messages() chan *Msg
	Close()
	Closed() bool
}

type simpleFetcher struct {
	store     storage.Store
	processID string
	queue     string
	lock      sync.Mutex
	isActive  bool

	ready    chan bool
	messages chan *Msg
	stop     chan bool
	exit     chan bool
	closed   chan bool
	logger   *log.Logger
}

func newSimpleFetcher(queue string, opts Options, isActive bool) *simpleFetcher {
	logger := opts.Logger
	if logger == nil {
		logger = log.New(os.Stdout, "go-workers2: ", log.Ldate|log.Lmicroseconds)
	}

	return &simpleFetcher{
		store:     opts.store,
		processID: opts.ProcessID,
		queue:     queue,
		isActive:  isActive,
		ready:     make(chan bool),
		messages:  make(chan *Msg),
		stop:      make(chan bool),
		exit:      make(chan bool),
		closed:    make(chan bool),
		logger:    logger,
	}
}

func (f *simpleFetcher) Queue() string {
	return f.queue
}

func (f *simpleFetcher) processOldMessages() {
	messages := f.inprogressMessages()

	for _, message := range messages {
		<-f.Ready()
		f.sendMessage(message)
	}
}

func (f *simpleFetcher) Fetch() {
	for !f.isActive {
		select {
		case <-f.stop:
			close(f.closed)
			close(f.exit)
			return
		}
	}
	f.processOldMessages()

	go func() {
		for {
			// f.Close() has been called
			if f.Closed() {
				break
			}
			<-f.Ready()
			if f.IsActive() {
				f.tryFetchMessage()
			}
		}
	}()

	for {
		select {
		case <-f.stop:
			// Stop the redis-polling goroutine
			close(f.closed)
			// Signal to Close() that the fetcher has stopped
			close(f.exit)
			break
		}
	}
}

func (f *simpleFetcher) tryFetchMessage() {
	message, err := f.store.DequeueMessage(context.Background(), f.queue, f.InProgressQueue(), 1*time.Second)
	if err != nil {
		// If redis returns null, the queue is empty.
		// Just ignore empty queue errors; print all other errors.
		if err != storage.NoMessage {
			f.logger.Println("ERR: ", f.queue, err)
		}
	} else {
		f.sendMessage(message)
	}
}

func (f *simpleFetcher) sendMessage(message string) {
	msg, err := NewMsg(message)

	if err != nil {
		f.logger.Println("ERR: Couldn't create message from", message, ":", err)
		return
	}

	f.Messages() <- msg
}

func (f *simpleFetcher) Acknowledge(message *Msg) {
	f.store.AcknowledgeMessage(context.Background(), f.InProgressQueue(), message.OriginalJson())
}

func (f *simpleFetcher) Messages() chan *Msg {
	return f.messages
}

func (f *simpleFetcher) Active(active bool) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.isActive = active
}

func (f *simpleFetcher) IsActive() bool {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.isActive
}

func (f *simpleFetcher) Ready() chan bool {
	return f.ready
}

func (f *simpleFetcher) Close() {
	f.stop <- true
	<-f.exit
}

func (f *simpleFetcher) Closed() bool {
	select {
	case <-f.closed:
		return true
	default:
		return false
	}
}

func (f *simpleFetcher) inprogressMessages() []string {
	messages, err := f.store.ListMessages(context.Background(), f.InProgressQueue())
	if err != nil {
		f.logger.Println("ERR: ", err)
	}

	return messages
}

func (f *simpleFetcher) InProgressQueue() string {
	return fmt.Sprint(f.queue, ":", f.processID, ":inprogress")
}
