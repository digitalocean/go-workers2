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

	if err != nil {
		// If redis returns null, the queue is empty. Just ignore the error.
		if err == redis.Nil {
			Logger.Println("ERR: ", f.queue, err)
			time.Sleep(1 * time.Second)
		}
	} else {
		f.sendMessage(message)
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
	f.client.LRem(f.inprogressQueue(), -1, message.OriginalJson()).Result()
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
