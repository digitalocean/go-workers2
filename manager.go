package workers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/friesencr/go-workers2/storage"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
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
	stopping         bool
	stop             chan bool
	active           bool
	logger           *slog.Logger
	startedAt        time.Time
	processNonce     string
	heartbeatChannel chan bool

	beforeStartHooks       []func()
	duringDrainHooks       []func()
	afterActiveChangeHooks []AfterActiveChangeFunc

	afterHeartbeatHooks []afterHeartbeatFunc

	retriesExhaustedHandlers []RetriesExhaustedFunc

	classOptions map[string]ClassOptions
	queueOptions map[string]WorkerOptions
}

// ClassOptions configures behavior for jobs with a given class name.
type ClassOptions struct {
	// RetainExhausted controls whether exhausted jobs of this class are kept in the dead set.
	// nil falls through to queue/default. false discards after exhaustion.
	RetainExhausted *bool
}

// WorkerOptions configures behavior for a queue registered via AddWorkerWithOptions.
type WorkerOptions struct {
	// RetainExhausted controls whether exhausted jobs on this queue are kept in the dead set.
	// nil falls through to default true. false discards after exhaustion.
	RetainExhausted *bool
}

type staleMessageUpdate struct {
	Queue           string
	InprogressQueue string
	RequeuedMsgs    []string
}

type AfterActiveChangeFunc func(manager *Manager, activateManager, deactivateManager bool)

type UpdateActiveManager struct {
	ActivateManager   bool
	DeactivateManager bool
}

// NewManager creates a new manager with provide options
func NewManager(options Options) (*Manager, error) {
	options, err := processOptions(options)
	if err != nil {
		return nil, err
	}
	return newManager(options)
}

// GetRedisClient returns the Redis client used by the manager
func (m *Manager) GetRedisClient() *redis.Client {
	return m.opts.client
}

// NewManagerWithRedisClient creates a new manager with provide options and pre-configured Redis client
func NewManagerWithRedisClient(options Options, client *redis.Client) (*Manager, error) {
	options, err := processOptionsWithRedisClient(options, client)
	if err != nil {
		return nil, err
	}
	return newManager(options)
}

func newManager(processedOptions Options) (*Manager, error) {
	processNonce, err := GenerateProcessNonce()
	if err != nil {
		return nil, err
	}

	manager := &Manager{
		uuid:         uuid.New().String(),
		logger:       processedOptions.Logger,
		opts:         processedOptions,
		processNonce: processNonce,
		active:       !processedOptions.ManagerStartInactive,
	}
	if processedOptions.Heartbeat != nil && processedOptions.Heartbeat.PrioritizedManager != nil {
		manager.addAfterHeartbeatHooks(activateManagerByPriority)
	}
	return manager, nil
}

// AddWorker adds a new job processing worker
func (m *Manager) AddWorker(queue string, concurrency int, job JobFunc, mids ...MiddlewareFunc) {
	m.addWorker(queue, concurrency, job, nil, mids...)
}

// AddWorkerWithOptions adds a new job processing worker with queue-level options.
func (m *Manager) AddWorkerWithOptions(queue string, concurrency int, job JobFunc, opts WorkerOptions, mids ...MiddlewareFunc) {
	m.addWorker(queue, concurrency, job, &opts, mids...)
}

func (m *Manager) addWorker(queue string, concurrency int, job JobFunc, opts *WorkerOptions, mids ...MiddlewareFunc) {
	m.lock.Lock()
	defer m.lock.Unlock()

	middlewareQueueName := m.opts.Namespace + queue
	if len(mids) == 0 {
		job = DefaultMiddlewares().build(middlewareQueueName, m, job)
	} else {
		job = NewMiddlewares(mids...).build(middlewareQueueName, m, job)
	}
	if opts != nil {
		if m.queueOptions == nil {
			m.queueOptions = make(map[string]WorkerOptions)
		}
		m.queueOptions[queue] = *opts
	}
	m.workers = append(m.workers, newWorker(m.logger, queue, concurrency, job))
}

// SetClassOptions sets options for jobs with the given class name.
// Options replace any previous options for that class.
func (m *Manager) SetClassOptions(class string, opts ClassOptions) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.classOptions == nil {
		m.classOptions = make(map[string]ClassOptions)
	}
	m.classOptions[class] = opts
}

func (m *Manager) classRetainExhausted(class string) (value bool, set bool) {
	m.lock.Lock()
	defer m.lock.Unlock()
	opts, ok := m.classOptions[class]
	if !ok || opts.RetainExhausted == nil {
		return false, false
	}
	return *opts.RetainExhausted, true
}

func (m *Manager) queueRetainExhausted(queue string) (value bool, set bool) {
	m.lock.Lock()
	defer m.lock.Unlock()
	opts, ok := m.queueOptions[queue]
	if !ok || opts.RetainExhausted == nil {
		return false, false
	}
	return *opts.RetainExhausted, true
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

func (m *Manager) addAfterHeartbeatHooks(hooks ...afterHeartbeatFunc) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.afterHeartbeatHooks = append(m.afterHeartbeatHooks, hooks...)
}

func (m *Manager) AddAfterActiveChangeHooks(hooks ...AfterActiveChangeFunc) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.afterActiveChangeHooks = append(m.afterActiveChangeHooks, hooks...)
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

// The returned slice can be ranged over after the lock is released only because writers replace
// the slice or append past its length, and never modify an element in place.
func (m *Manager) getRetriesExhaustedHandlers() []RetriesExhaustedFunc {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.retriesExhaustedHandlers
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
	m.stopping = false

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
			fetcher := newSimpleFetcher(w.queue, *m.Opts(), m.IsActive())
			w.start(fetcher)
			wg.Done()
		}()
	}
	m.schedule = newScheduledWorker(m.opts)

	wg.Add(1)
	go func() {
		m.schedule.run()
		wg.Done()
	}()

	if m.opts.Heartbeat != nil {
		// Created before the lock is released so that a Stop arriving before the goroutine runs
		// still has a channel to signal, rather than blocking forever on a nil one.
		m.heartbeatChannel = make(chan bool, 1)
		go m.startHeartbeat(m.heartbeatChannel)
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
	// running stays true until Run has drained, so it cannot tell a second Stop from the first.
	if !m.running || m.stopping {
		return
	}
	m.stopping = true
	if m.opts.Heartbeat != nil {
		m.stopHeartbeat()
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

func (m *Manager) Opts() *Options {
	return &m.opts
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

	for q, l := range storeStats.Enqueued {
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

// GetDead returns a page of dead jobs for the manager, most recently dead first
func (m *Manager) GetDead(page uint64, pageSize int64) (Dead, error) {
	if pageSize <= 0 {
		return Dead{}, errors.New("page size must be positive")
	}
	if page >= uint64(math.MaxInt64/pageSize) {
		return Dead{}, errors.New("page out of range")
	}

	start := int64(page) * pageSize
	storeDead, err := m.opts.store.GetDead(context.Background(), start, start+pageSize-1)
	if err != nil {
		return Dead{}, err
	}

	var deadJobs []*Msg
	for _, d := range storeDead.DeadJobs {
		deadJob, err := NewMsg(d)
		if err != nil {
			return Dead{}, err
		}

		deadJobs = append(deadJobs, deadJob)
	}

	return Dead{
		TotalDeadCount: storeDead.TotalDeadCount,
		DeadJobs:       deadJobs,
	}, nil
}

var globEscaper = strings.NewReplacer(`\`, `\\`, `*`, `\*`, `?`, `\?`, `[`, `\[`, `]`, `\]`)

// RequeueDead moves the dead job with the given jid back onto the queue it failed on.
// It returns storage.NoMessage when no dead job has that jid.
func (m *Manager) RequeueDead(jid string) error {
	if jid == "" {
		return errors.New("jid is required")
	}

	encodedJid, err := json.Marshal(jid)
	if err != nil {
		return err
	}
	match := "*" + globEscaper.Replace(string(encodedJid)) + "*"

	ctx := context.Background()
	var cursor uint64
	for {
		deadMessages, next, err := m.opts.store.ScanDeadMessages(ctx, cursor, match, 100)
		if err != nil {
			return err
		}

		for _, deadMessage := range deadMessages {
			message, err := NewMsg(deadMessage)
			if err != nil {
				return err
			}
			if message.Jid() != jid {
				continue
			}

			queue, _ := message.Get("queue").String()
			queue = strings.TrimPrefix(queue, m.opts.Namespace)
			if queue == "" {
				return fmt.Errorf("dead job %s has no queue", jid)
			}
			message.Set("enqueued_at", nowToSecondsWithNanoPrecision())

			return m.opts.store.RequeueDeadMessage(ctx, queue, deadMessage, message.ToJson())
		}

		if next == 0 {
			return storage.NoMessage
		}
		cursor = next
	}
}

func (m *Manager) startHeartbeat(stop <-chan bool) error {
	heartbeatTicker := time.NewTicker(m.opts.Heartbeat.Interval)

	for {
		select {
		case <-heartbeatTicker.C:
			heartbeatTime, err := m.opts.store.GetTime(context.Background())
			if err != nil {
				m.logger.Error("failed to get heartbeat time", "error", err)
				return err
			}
			heartbeat, err := m.sendHeartbeat(heartbeatTime)
			if err != nil {
				m.logger.Error("failed to send heartbeat", "error", err)
				return err
			}
			expireTS := heartbeatTime.Add(-m.opts.Heartbeat.HeartbeatTTL).Unix()
			staleMessageUpdates, err := m.handleAllExpiredHeartbeats(context.Background(), expireTS)
			if err != nil {
				m.logger.Error("error expiring heartbeat identities", "error", err)
				return err
			}
			for _, afterHeartbeatHook := range m.afterHeartbeatHooks {
				err := afterHeartbeatHook(heartbeat, m, staleMessageUpdates)
				if err != nil {
					m.logger.Error("failed to execute after heartbeat hook", "error", err)
					return err
				}
			}
		case <-stop:
			return nil
		}
	}
}

func (m *Manager) handleAllExpiredHeartbeats(ctx context.Context, expireTS int64) ([]*staleMessageUpdate, error) {
	heartbeats, err := m.opts.store.GetAllHeartbeats(ctx)
	if err != nil && err != redis.Nil {
		m.logger.Error("get all heartbeats failed", "error", err)
		return nil, err
	}

	var staleMessageUpdates []*staleMessageUpdate
	for _, heartbeat := range heartbeats {
		if heartbeat.Beat > expireTS {
			continue
		}

		// requeue worker in-progress queues back to the queues
		requeuedInProgressQueues := make(map[string]bool)
		for _, workerHeartbeat := range heartbeat.WorkerHeartbeats {
			var requeuedMsgs []string
			if _, exists := requeuedInProgressQueues[workerHeartbeat.InProgressQueue]; exists {
				continue
			}
			requeuedMsgs, err = m.opts.store.RequeueMessagesFromInProgressQueue(ctx, workerHeartbeat.InProgressQueue, workerHeartbeat.Queue)
			if err != nil {
				m.logger.Error("requeue in-progress messages failed",
					"queue", workerHeartbeat.Queue,
					"inprogress_queue", workerHeartbeat.InProgressQueue,
					"error", err,
				)
				return nil, err
			}
			requeuedInProgressQueues[workerHeartbeat.InProgressQueue] = true
			if len(requeuedMsgs) == 0 {
				continue
			}
			updatedStaleMessage := &staleMessageUpdate{
				Queue:           workerHeartbeat.Queue,
				InprogressQueue: workerHeartbeat.InProgressQueue,
				RequeuedMsgs:    requeuedMsgs,
			}
			staleMessageUpdates = append(staleMessageUpdates, updatedStaleMessage)
		}
		err = m.opts.store.RemoveHeartbeat(ctx, heartbeat.Identity)
		if err != nil {
			m.logger.Error("remove heartbeat failed", "identity", heartbeat.Identity, "error", err)
			return nil, err
		}

	}
	return staleMessageUpdates, nil
}

func (m *Manager) IsActive() bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.active
}

func (m *Manager) Active(active bool) {
	isActive := m.IsActive()
	activateManager := !isActive && active
	deactivateManager := isActive && !active
	if activateManager || deactivateManager {
		m.lock.Lock()
		m.active = active
		for _, worker := range m.workers {
			worker.fetcher.SetActive(active)
		}
		m.lock.Unlock()
		for _, hook := range m.afterActiveChangeHooks {
			hook(m, activateManager, deactivateManager)
		}
	}
}

func (m *Manager) stopHeartbeat() {
	select {
	case m.heartbeatChannel <- true:
	default:
		// The buffer is full, so a stop is already pending.
	}
}

func (m *Manager) sendHeartbeat(heartbeatTime time.Time) (*storage.Heartbeat, error) {
	heartbeat, err := m.buildHeartbeat(heartbeatTime, m.opts.Heartbeat.HeartbeatTTL)
	if err != nil {
		return heartbeat, err
	}

	err = m.opts.store.SendHeartbeat(context.Background(), heartbeat)
	return heartbeat, err
}

func activateManagerByPriority(heartbeat *storage.Heartbeat, manager *Manager, staleMessageUpdates []*staleMessageUpdate) error {
	ctx := context.Background()
	heartbeats, err := manager.opts.store.GetAllHeartbeats(ctx)
	if err != nil {
		manager.logger.Error("get all heartbeats failed", "error", err)
		return err
	}
	if len(heartbeats) == 0 {
		return nil
	}
	// order active heartbeats by manager priority descending
	sort.Slice(heartbeats, func(i, j int) bool {
		return heartbeats[i].ManagerPriority > heartbeats[j].ManagerPriority
	})

	// if current manager's priority is high enough to be within total active manager threshold, set manager as active
	activeManager := false
	for i := 0; i < manager.opts.Heartbeat.PrioritizedManager.TotalActiveManagers; i++ {
		if heartbeats[i].Identity == heartbeat.Identity {
			activeManager = true
			break
		}
	}
	manager.Active(activeManager)
	return nil
}
