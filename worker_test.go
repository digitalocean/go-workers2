package workers

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type dummyFetcher struct {
	lock     sync.Mutex
	isActive bool

	inProgressQueue func() string
	queue           func() string
	fetch           func()
	acknowledge     func(*Msg)
	ready           func() chan bool
	messages        func() chan *Msg
	close           func()
	closed          func() bool
}

func (d *dummyFetcher) Queue() string           { return d.queue() }
func (d *dummyFetcher) InProgressQueue() string { return d.inProgressQueue() }
func (d *dummyFetcher) Fetch()                  { d.fetch() }
func (d *dummyFetcher) Acknowledge(m *Msg)      { d.acknowledge(m) }
func (d *dummyFetcher) Ready() chan bool        { return d.ready() }
func (d *dummyFetcher) Messages() chan *Msg     { return d.messages() }
func (d *dummyFetcher) Close()                  { d.close() }
func (d *dummyFetcher) Closed() bool            { return d.closed() }

func (d *dummyFetcher) SetActive(active bool) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.isActive = active
}
func (d *dummyFetcher) IsActive() bool {
	d.lock.Lock()
	defer d.lock.Unlock()
	return d.isActive
}

func TestNewWorker(t *testing.T) {
	testLogger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	cc := NewCallCounter()
	w := newWorker(testLogger, "q", 0, cc.F)
	assert.Equal(t, "q", w.queue)
	assert.Equal(t, 1, w.concurrency)
	assert.NotNil(t, w.stop)

	assert.NotNil(t, w.handler)
	w.handler(nil)
	assert.Equal(t, 1, cc.count)

	w = newWorker(testLogger, "q", -5, cc.F)
	assert.Equal(t, 1, w.concurrency)

	w = newWorker(testLogger, "q", 10, cc.F)
	assert.Equal(t, 10, w.concurrency)
}

func TestWorker(t *testing.T) {
	testLogger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	readyCh := make(chan bool)
	msgCh := make(chan *Msg)
	ackCh := make(chan *Msg)
	fetchCh := make(chan bool)

	var dfClosedLock sync.Mutex
	var dfClosed bool
	df := dummyFetcher{
		inProgressQueue: func() string { return "inprog-q" },
		queue:           func() string { return "q" },
		fetch:           func() { close(fetchCh) },
		acknowledge:     func(m *Msg) { ackCh <- m },
		ready:           func() chan bool { return readyCh },
		messages:        func() chan *Msg { return msgCh },
		close: func() {
			dfClosedLock.Lock()
			defer dfClosedLock.Unlock()
			dfClosed = true
		},
		closed: func() bool {
			dfClosedLock.Lock()
			defer dfClosedLock.Unlock()
			return dfClosed
		},
	}

	cc := NewCallCounter()

	w := newWorker(testLogger, "q", 2, cc.F)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		w.start(&df)
		wg.Done()
	}()

	// This block delays until the entire worker is started.
	// In order for a message to be consumed, at least one task runner
	// must be started. We consume the message off of ackCh for sanity.
	// Acquiring and then releasing the runnersLock ensures that start
	// has finished its setup work

	<-fetchCh // We should be sure that Fetch got called before providing any messages
	msgCh <- cc.msg()
	<-ackCh
	w.runnersLock.Lock()
	w.runnersLock.Unlock()

	assert.True(t, w.running)
	assert.Len(t, w.runners, 2)
	assert.Equal(t, w.inProgressQueue, df.InProgressQueue())

	t.Run("cannot start while running", func(t *testing.T) {
		w.start(&df)
		// This test would time out if w.start doesn't return immediately
	})

	t.Run(".inProgressMessages", func(t *testing.T) {

		// None running
		msgs := w.inProgressMessages()
		assert.Empty(t, msgs)

		// Enqueue one
		msgCh <- cc.syncMsg()
		<-cc.syncCh
		msgs = w.inProgressMessages()
		assert.Len(t, msgs, 1)

		// Enqueue another
		msgCh <- cc.syncMsg()
		<-cc.syncCh
		msgs = w.inProgressMessages()
		assert.Len(t, msgs, 2)

		// allow one to finish
		cc.ackSyncCh <- true
		<-ackCh
		msgs = w.inProgressMessages()
		assert.Len(t, msgs, 1)

		// alow the other to finish
		cc.ackSyncCh <- true
		<-ackCh
		msgs = w.inProgressMessages()
		assert.Empty(t, msgs)
	})

	w.quit()
	wg.Wait()

}

func TestWorkerProcessesAndAcksMessages(t *testing.T) {
	testLogger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	readyCh := make(chan bool)
	msgCh := make(chan *Msg)
	ackCh := make(chan *Msg)
	closeCh := make(chan bool)

	df := dummyFetcher{
		queue:           func() string { return "q" },
		inProgressQueue: func() string { return "inprog-q" },
		fetch:           func() { <-closeCh },
		acknowledge:     func(m *Msg) { ackCh <- m },
		ready:           func() chan bool { return readyCh },
		messages:        func() chan *Msg { return msgCh },
		close:           func() { close(closeCh) },
		closed: func() bool {
			select {
			case <-closeCh:
				return true
			default:
				return false
			}
		},
	}

	cc := NewCallCounter()
	w := newWorker(testLogger, "q", 1, cc.F)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		w.start(&df)
		wg.Done()
	}()

	// since we have concurrency 1, messages _must_ be processed in order

	msgCh <- cc.msg()
	ackedMsg := <-ackCh
	assert.True(t, ackedMsg.ack)
	assert.NotZero(t, ackedMsg.startedAt)
	assert.Equal(t, 1, cc.count)

	noAck := cc.noAckMsg()
	msgCh <- noAck
	msgCh <- cc.msg()
	ackedMsg = <-ackCh
	assert.False(t, noAck.ack)
	assert.NotZero(t, noAck.startedAt)
	assert.True(t, ackedMsg.ack)
	assert.NotZero(t, ackedMsg.startedAt)
	assert.Equal(t, 3, cc.count)

	w.quit()
	wg.Wait()
}

type ackCountingFetcher struct {
	Fetcher
	acks chan *Msg
}

func (f *ackCountingFetcher) Acknowledge(m *Msg) {
	f.Fetcher.Acknowledge(m)
	f.acks <- m
}

func TestWorkerRetryDisabledFailureCallsRetriesExhaustedHandlerAndAcksOnce(t *testing.T) {
	ctx := context.Background()

	mgr, err := newTestManager(testOptionsWithNamespace("prod"), true)
	assert.NoError(t, err)
	rc := mgr.opts.client

	exhausted := make(chan *Msg, 10)
	mgr.SetRetriesExhaustedHandlers(func(queue string, message *Msg, err error) {
		exhausted <- message
	})
	jobCalls := make(chan *Msg, 10)
	mgr.AddWorker("retrydisabled", 1, func(m *Msg) error {
		jobCalls <- m
		return errors.New(errorText)
	})

	jid, err := mgr.Producer().EnqueueWithOptions("retrydisabled", "clazz", []int{1}, EnqueueOptions{Retry: false})
	assert.NoError(t, err)

	w := mgr.workers[0]
	fetcher := &ackCountingFetcher{
		Fetcher: newSimpleFetcher(w.queue, *mgr.Opts(), true),
		acks:    make(chan *Msg, 10),
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		w.start(fetcher)
		wg.Done()
	}()

	select {
	case <-fetcher.acks:
	case <-time.After(10 * time.Second):
		assert.FailNow(t, "message was never acknowledged")
	}

	w.quit()
	wg.Wait()

	assert.Len(t, fetcher.acks, 0)
	assert.Len(t, jobCalls, 1)
	if assert.Len(t, exhausted, 1) {
		assert.Equal(t, jid, (<-exhausted).Jid())
	}

	inProgress, err := rc.LLen(ctx, "prod:queue:"+fetcher.InProgressQueue()).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), inProgress)

	queued, err := rc.LLen(ctx, "prod:queue:retrydisabled").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), queued)
}
