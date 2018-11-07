package workers

import (
	"sync"
)

type worker struct {
	queue       string
	handler     JobFunc
	concurrency int
	runners     []*taskRunner
	runnersLock sync.Mutex
	stop        chan bool
	running     bool
}

func newWorker(queue string, concurrency int, handler JobFunc) *worker {
	if concurrency <= 0 {
		concurrency = 1
	}
	w := &worker{
		queue:       queue,
		handler:     handler,
		concurrency: concurrency,
		stop:        make(chan bool),
	}
	return w
}

func (w *worker) start(fetcher Fetcher) {
	w.runnersLock.Lock()
	if w.running {
		w.runnersLock.Unlock()
		return
	}
	w.running = true
	defer func() {
		w.runnersLock.Lock()
		w.running = false
		w.runnersLock.Unlock()
	}()

	var wg sync.WaitGroup
	wg.Add(w.concurrency)

	go fetcher.Fetch()

	done := make(chan *Msg)
	w.runners = make([]*taskRunner, w.concurrency)
	for i := 0; i < w.concurrency; i++ {
		r := newTaskRunner(w.handler)
		w.runners[i] = r
		go func() {
			r.work(fetcher.Messages(), done, fetcher.Ready())
			wg.Done()
		}()
	}
	exit := make(chan bool)
	go func() {
		wg.Wait()
		close(exit)
	}()

	// Now that we're all set up, unlock so that stats can check.
	w.runnersLock.Unlock()

	for {
		select {
		case msg := <-done:
			if msg.ack {
				fetcher.Acknowledge(msg)
			}
		case <-w.stop:
			if !fetcher.Closed() {
				fetcher.Close()

				// we need to relock the runners so we can shut this down
				w.runnersLock.Lock()
				for _, r := range w.runners {
					r.quit()
				}
				w.runnersLock.Unlock()
			}
		case <-exit:
			return
		}
	}
}

func (w *worker) quit() {
	w.runnersLock.Lock()
	defer w.runnersLock.Unlock()
	if w.running {
		w.stop <- true
	}
}

func (w *worker) inProgressMessages() []*Msg {
	w.runnersLock.Lock()
	defer w.runnersLock.Unlock()
	var res []*Msg
	for _, r := range w.runners {
		if m := r.inProgressMessage(); m != nil {
			res = append(res, m)
		}
	}
	return res
}
