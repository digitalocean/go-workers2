package workers

import (
	"fmt"
	"time"

	"github.com/go-redis/redis"
)

type Fetcher interface {
	Queue() string
	Fetch()
	Acknowledge(*Msg)
	Ready() chan bool
	Messages() chan *Msg
	Close()
	Closed() bool
}

type simpleFetcher struct {
	client    *redis.Client
	processID string
	queue     string
	ready     chan bool
	messages  chan *Msg
	stop      chan bool
	exit      chan bool
	closed    chan bool
}

func newSimpleFetcher(queue string, opts Options) *simpleFetcher {
	return &simpleFetcher{
		client:    opts.client,
		processID: opts.ProcessID,
		queue:     opts.Namespace + "queue:" + queue,
		ready:     make(chan bool),
		messages:  make(chan *Msg),
		stop:      make(chan bool),
		exit:      make(chan bool),
		closed:    make(chan bool),
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
	f.processOldMessages()

	go func() {
		for {
			// f.Close() has been called
			if f.Closed() {
				break
			}
			<-f.Ready()
			f.tryFetchMessage()
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
	message, err := f.client.BRPopLPush(f.queue, f.inprogressQueue(), 1*time.Second).Result()
	switch err {
	case nil:
		f.sendMessage(message)
	case redis.Nil:
		// If redis returns null, the queue is empty.
		// Just ignore empty queue errors.
		time.Sleep(1*time.Second)
	default:
		// Print all other errors.
		Logger.Println("ERR: ", f.queue, err)
	}
}

func (f *simpleFetcher) sendMessage(message string) {
	msg, err := NewMsg(message)

	if err != nil {
		Logger.Println("ERR: Couldn't create message from", message, ":", err)
		return
	}

	f.Messages() <- msg
}

func (f *simpleFetcher) Acknowledge(message *Msg) {
	count := int64(-1)
	val, err := message.Get("unique").Bool()
	if err == nil && val {
		count = 0
	}
	f.client.LRem(f.inprogressQueue(), count, message.OriginalJson()).Result()
}

func (f *simpleFetcher) Messages() chan *Msg {
	return f.messages
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
	messages, err := f.client.LRange(f.inprogressQueue(), 0, -1).Result()
	if err != nil {
		Logger.Println("ERR: ", err)
	}

	return messages
}

func (f *simpleFetcher) inprogressQueue() string {
	return fmt.Sprint(f.queue, ":", f.processID, ":inprogress")
}
