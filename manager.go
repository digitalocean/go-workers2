package workers

import (
	"os"
	"sync"

	"github.com/digitalocean/go-workers2/storage"
	"github.com/go-redis/redis"
	"github.com/google/uuid"
)

// Manager coordinates work, workers, and signaling needed for job processing
type Manager struct {
	uuid     string
	opts     Options
	schedule *scheduledWorker
	workers  []*worker
	lock     sync.Mutex
	signal   chan os.Signal
	running  bool

	beforeStartHooks []func()
	duringDrainHooks []func()
}

// NewManager creates a new manager with provide options
func NewManager(options Options) (*Manager, error) {
	options, err := processOptions(options)
	if err != nil {
		return nil, err
	}

	return &Manager{
		uuid: uuid.New().String(),
		opts: options,
	}, nil
}

// NewManagerWithRedisClient creates a new manager with provide options and pre-configured Redis client
func NewManagerWithRedisClient(options Options, client *redis.Client) (*Manager, error) {
	options, err := processOptionsWithRedisClient(options, client)
	if err != nil {
		return nil, err
	}

	return &Manager{
		uuid: uuid.New().String(),
		opts: options,
	}, nil
}

// GetRedisClient returns the Redis client used by the manager
func (m *Manager) GetRedisClient() *redis.Client {
	return m.opts.client
}

// AddWorker adds a new job processing worker
func (m *Manager) AddWorker(queue string, concurrency int, job JobFunc, mids ...MiddlewareFunc) {
	m.lock.Lock()
	defer m.lock.Unlock()

	middlewareQueueName := m.opts.Namespace + queue
	if len(mids) == 0 {
		job = DefaultMiddlewares().build(middlewareQueueName, m, job)
	} else {
		job = NewMiddlewares(mids...).build(middlewareQueueName, m, job)
	}
	m.workers = append(m.workers, newWorker(queue, concurrency, job))
}

// AddBeforeStartHooks adds functions to be executed before the manager starts
func (m *Manager) AddBeforeStartHooks(hooks ...func()) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.beforeStartHooks = append(m.beforeStartHooks, hooks...)
}

// AddDuringDrainHooks adds function to be execute during a drain operation
func (m *Manager) AddDuringDrainHooks(hooks ...func()) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.duringDrainHooks = append(m.duringDrainHooks, hooks...)
}

// Run starts all workers under this Manager and blocks until they exit.
func (m *Manager) Run() {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.running {
		return // Can't start if we're already running!
	}
	m.running = true

	for _, h := range m.beforeStartHooks {
		h()
	}

	globalApiServer.registerManager(m)

	var wg sync.WaitGroup

	wg.Add(1)
	m.signal = make(chan os.Signal, 1)
	go func() {
		m.handleSignals()
		wg.Done()
	}()

	wg.Add(len(m.workers))
	for i := range m.workers {
		w := m.workers[i]
		go func() {
			w.start(newSimpleFetcher(w.queue, m.opts))
			wg.Done()
		}()
	}
	m.schedule = newScheduledWorker(m.opts)

	wg.Add(1)
	go func() {
		m.schedule.run()
		wg.Done()
	}()

	// Release the lock so that Stop can acquire it
	m.lock.Unlock()
	wg.Wait()
	// Regain the lock
	m.lock.Lock()
	globalApiServer.deregisterManager(m)
	m.running = false
}

// Stop all workers under this Manager and returns immediately.
func (m *Manager) Stop() {
	m.lock.Lock()
	defer m.lock.Unlock()
	if !m.running {
		return
	}
	for _, w := range m.workers {
		w.quit()
	}
	m.schedule.quit()
	for _, h := range m.duringDrainHooks {
		h()
	}
	m.stopSignalHandler()
}

func (m *Manager) inProgressMessages() map[string][]*Msg {
	m.lock.Lock()
	defer m.lock.Unlock()
	res := map[string][]*Msg{}
	for _, w := range m.workers {
		res[w.queue] = append(res[w.queue], w.inProgressMessages()...)
	}
	return res
}

// Producer creates a new work producer with configuration identical to the manager
func (m *Manager) Producer() *Producer {
	return &Producer{opts: m.opts}
}

// GetStats returns the set of stats for the manager
func (m *Manager) GetStats() (Stats, error) {
	stats := Stats{
		Jobs:     map[string][]JobStatus{},
		Enqueued: map[string]int64{},
		Name:     m.opts.ManagerDisplayName,
	}
	var q []string

	inProgress := m.inProgressMessages()
	ns := m.opts.Namespace

	for queue, msgs := range inProgress {
		var jobs []JobStatus
		for _, m := range msgs {
			jobs = append(jobs, JobStatus{
				Message:   m,
				StartedAt: m.startedAt,
			})
		}
		stats.Jobs[ns+queue] = jobs
		q = append(q, queue)
	}

	storeStats, err := m.opts.store.GetAllStats(q)

	if err != nil {
		return stats, err
	}

	stats.Processed = storeStats.Processed
	stats.Failed = storeStats.Failed
	stats.RetryCount = storeStats.RetryCount

	for q, l := range stats.Enqueued {
		stats.Enqueued[q] = l
	}

	return stats, nil
}

// GetRetries returns the set of retry jobs for the manager
func (m *Manager) GetRetries(page uint64, page_size int64, match string) (Retries, error) {
	retries := Retries{}

	storeRetries, err := m.opts.store.GetAllRetries()
	if err != nil {
		return retries, err
	}
	retryStats := m.opts.client.ZScan(m.opts.Namespace+storage.RetryKey, page, match, page_size).Iterator()

	var messages []*Msg

	for retryStats.Next() {
		msg, err := NewMsg(retryStats.Val())
		if err != nil {
			break
		}
		retryStats.Next()
		messages = append(messages, msg)
	}

	var retryJobStats []RetryJobStats

	for i := 0; i < len(messages); i++ {
		// Get the values for each field
		class, err := messages[i].Get("class").String()
		if err != nil {
			return retries, err
		}
		error_msg, err := messages[i].Get("error_message").String()
		if err != nil {
			return retries, err
		}
		failed_at, err := messages[i].Get("failed_at").String()
		if err != nil {
			return retries, err
		}
		job_id, err := messages[i].Get("jid").String()
		if err != nil {
			return retries, err
		}
		queue, err := messages[i].Get("queue").String()
		if err != nil {
			return retries, err
		}
		retry_count, err := messages[i].Get("retry_count").Int64()
		if err != nil {
			return retries, err
		}

		retryJobStats = append(retryJobStats, RetryJobStats{
			Class:        class,
			ErrorMessage: error_msg,
			FailedAt:     failed_at,
			JobID:        job_id,
			Queue:        queue,
			RetryCount:   retry_count,
		})
	}

	retries.TotalRetryCount = storeRetries.TotalRetryCount
	retries.RetryJobs = retryJobStats

	return retries, nil
}
