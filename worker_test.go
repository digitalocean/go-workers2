package workers

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

type dummyFetcher struct {
	queue       func() string
	fetch       func()
	acknowledge func(*Msg)
	ready       func() chan bool
	messages    func() chan *Msg
	close       func()
	closed      func() bool
}

func (d dummyFetcher) Queue() string       { return d.queue() }
func (d dummyFetcher) Fetch()              { d.fetch() }
func (d dummyFetcher) Acknowledge(m *Msg)  { d.acknowledge(m) }
func (d dummyFetcher) Ready() chan bool    { return d.ready() }
func (d dummyFetcher) Messages() chan *Msg { return d.messages() }
func (d dummyFetcher) Close()              { d.close() }
func (d dummyFetcher) Closed() bool        { return d.closed() }

func TestNewWorker(t *testing.T) {
	cc := newCallCounter()
	w := newWorker("q", 0, cc.F)
	assert.Equal(t, "q", w.queue)
	assert.Equal(t, 1, w.concurrency)
	assert.NotNil(t, w.stop)

	assert.NotNil(t, w.handler)
	w.handler(nil)
	assert.Equal(t, 1, cc.count)

	w = newWorker("q", -5, cc.F)
	assert.Equal(t, 1, w.concurrency)

	w = newWorker("q", 10, cc.F)
	assert.Equal(t, 10, w.concurrency)
}

func TestWorker(t *testing.T) {
	readyCh := make(chan bool)
	msgCh := make(chan *Msg)
	ackCh := make(chan *Msg)
	fetchCh := make(chan bool)

	var dfClosedLock sync.Mutex
	var dfClosed bool
	df := dummyFetcher{
		queue:       func() string { return "q" },
		fetch:       func() { close(fetchCh) },
		acknowledge: func(m *Msg) { ackCh <- m },
		ready:       func() chan bool { return readyCh },
		messages:    func() chan *Msg { return msgCh },
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

	cc := newCallCounter()

	w := newWorker("q", 2, cc.F)

	var wg sync.WaitGroup
	go func() {
		wg.Add(1)
		w.start(df)
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

	t.Run("cannot start while running", func(t *testing.T) {
		w.start(df)
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
	readyCh := make(chan bool)
	msgCh := make(chan *Msg)
	ackCh := make(chan *Msg)
	closeCh := make(chan bool)

	df := dummyFetcher{
		queue:       func() string { return "q" },
		fetch:       func() { <-closeCh },
		acknowledge: func(m *Msg) { ackCh <- m },
		ready:       func() chan bool { return readyCh },
		messages:    func() chan *Msg { return msgCh },
		close:       func() { close(closeCh) },
		closed: func() bool {
			select {
			case <-closeCh:
				return true
			default:
				return false
			}
		},
	}

	cc := newCallCounter()
	w := newWorker("q", 1, cc.F)

	var wg sync.WaitGroup

	go func() {
		wg.Add(1)
		w.start(df)
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
