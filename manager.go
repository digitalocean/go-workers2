package workers

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

// Manager coordinates work, workers, and signaling needed for job processing
type Manager struct {
	uuid             string
	opts             Options
	schedule         *scheduledWorker
	workers          []*worker
	lock             sync.Mutex
	signal           chan os.Signal
	running          bool
	logger           *log.Logger
	startedAt        time.Time
	processNonce     string
	heartbeatChannel chan bool

	beforeStartHooks []func()
	duringDrainHooks []func()

	retriesExhaustedHandlers []RetriesExhaustedFunc
}

// NewManager creates a new manager with provide options
func NewManager(options Options) (*Manager, error) {
	options, err := processOptions(options)
	if err != nil {
		return nil, err
	}

	processNonce, err := GenerateProcessNonce()
	if err != nil {
		return nil, err
	}

	return &Manager{
		uuid:         uuid.New().String(),
		logger:       options.Logger,
		opts:         options,
		processNonce: processNonce,
	}, nil
}

// NewManagerWithRedisClient creates a new manager with provide options and pre-configured Redis client
func NewManagerWithRedisClient(options Options, client *redis.Client) (*Manager, error) {
	options, err := processOptionsWithRedisClient(options, client)
	if err != nil {
		return nil, err
	}

	processNonce, err := GenerateProcessNonce()
	if err != nil {
		return nil, err
	}

	return &Manager{
		uuid:         uuid.New().String(),
		logger:       options.Logger,
		opts:         options,
		processNonce: processNonce,
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
	m.workers = append(m.workers, newWorker(m.logger, queue, concurrency, job))
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

// SetRetriesExhaustedHandlers sets function(s) that will be sequentially executed when retries are exhausted for a job.
func (m *Manager) SetRetriesExhaustedHandlers(handlers ...RetriesExhaustedFunc) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.retriesExhaustedHandlers = handlers
}

// AddRetriesExhaustedHandlers adds function(s) to be executed when retries are exhausted for a job.
func (m *Manager) AddRetriesExhaustedHandlers(handlers ...RetriesExhaustedFunc) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.retriesExhaustedHandlers = append(m.retriesExhaustedHandlers, handlers...)
}

// Run starts all workers under this Manager and blocks until they exit.
func (m *Manager) Run() {
	m.startedAt = time.Now()

	m.lock.Lock()
	defer m.lock.Unlock()
	if m.running {
		return // Can't start if we're already running!
	}
	m.running = true

	for _, h := range m.beforeStartHooks {
		h()
	}

	globalAPIServer.registerManager(m)

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

	if m.opts.Heartbeat {
		go m.startHeartbeat()
	}

	// Release the lock so that Stop can acquire it
	m.lock.Unlock()
	wg.Wait()
	// Regain the lock
	m.lock.Lock()
	globalAPIServer.deregisterManager(m)
	m.running = false
}

// Stop all workers under this Manager and returns immediately.
func (m *Manager) Stop() {
	m.lock.Lock()
	defer m.lock.Unlock()
	if !m.running {
		return
	}
	if m.opts.Heartbeat {
		m.removeHeartbeat()
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

	storeStats, err := m.opts.store.GetAllStats(context.Background(), q)

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
func (m *Manager) GetRetries(page uint64, pageSize int64, match string) (Retries, error) {
	// TODO: add back pagination and filtering

	storeRetries, err := m.opts.store.GetAllRetries(context.Background())
	if err != nil {
		return Retries{}, err
	}

	var retryJobs []*Msg
	for _, r := range storeRetries.RetryJobs {
		// parse json from string of retry data
		retryJob, err := NewMsg(r)
		if err != nil {
			return Retries{}, err
		}

		retryJobs = append(retryJobs, retryJob)
	}

	return Retries{
		TotalRetryCount: storeRetries.TotalRetryCount,
		RetryJobs:       retryJobs,
	}, nil
}

func (m *Manager) startHeartbeat() error {
	err := m.sendHeartbeat()
	if err != nil {
		m.logger.Println("Failed to send heartbeat", err)
		return err
	}

	heartbeatTicker := time.NewTicker(5 * time.Second)
	m.heartbeatChannel = make(chan bool, 1)

	for {
		select {
		case <-heartbeatTicker.C:
			err := m.sendHeartbeat()
			if err != nil {
				m.logger.Println("Failed to send heartbeat", err)
				return err
			}
		case <-m.heartbeatChannel:
			return nil
		}
	}
	return nil
}

func (m *Manager) removeHeartbeat() error {
	m.heartbeatChannel <- true
	heartbeat, err := m.buildHeartbeat()
	if err != nil {
		return err
	}
	err = m.opts.store.RemoveHeartbeat(context.Background(), heartbeat)
	return err
}

func (m *Manager) sendHeartbeat() error {
	heartbeat, err := m.buildHeartbeat()
	if err != nil {
		return err
	}

	err = m.opts.store.SendHeartbeat(context.Background(), heartbeat)
	return err
}
