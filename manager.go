package workers

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/digitalocean/go-workers2/storage"
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
	stop             chan bool
	active           bool
	logger           *log.Logger
	startedAt        time.Time
	processNonce     string
	heartbeatChannel chan bool

	beforeStartHooks    []func()
	duringDrainHooks    []func()
	afterHeartbeatHooks []afterHeartbeatFunc

	retriesExhaustedHandlers []RetriesExhaustedFunc
}

type updateActiveClusterStatus struct {
	activeClusterID   string
	activateManager   bool
	deactivateManager bool
}

type requeuedTaskRunnerStatus struct {
	pid             int
	tid             string
	queue           string
	inprogressQueue string
	requeuedMsgs    []string
}

// NewManager creates a new manager with provide options
func NewManager(options Options) (*Manager, error) {
	options, err := processOptions(options)
	if err != nil {
		return nil, err
	}
	return newManager(options)
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
	active := false
	// if no active/passive failover is configured, manager is initialized as active for backwards compatibility
	if processedOptions.ActivePassiveFailover == nil {
		active = true
	}

	return &Manager{
		uuid:         uuid.New().String(),
		logger:       processedOptions.Logger,
		opts:         processedOptions,
		processNonce: processNonce,
		active:       active,
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

func (m *Manager) AddAfterHeartbeatHooks(hooks ...afterHeartbeatFunc) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.afterHeartbeatHooks = append(m.afterHeartbeatHooks, hooks...)
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
			fetcher := newSimpleFetcher(w.queue, m.opts, m.active)
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
	if m.opts.Heartbeat != nil {
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
	heartbeatTicker := time.NewTicker(m.opts.Heartbeat.Interval)
	m.heartbeatChannel = make(chan bool, 1)

	for {
		select {
		case <-heartbeatTicker.C:
			heartbeatTime, err := m.opts.store.GetTime(context.Background())
			if err != nil {
				m.logger.Println("ERR: Failed to get heartbeat time", err)
				return err
			}
			heartbeat, err := m.sendHeartbeat(heartbeatTime)
			if err != nil {
				m.logger.Println("ERR: Failed to send heartbeat", err)
				return err
			}
			updateActiveClusterStatus, err := m.updateActiveCluster(heartbeatTime)
			if err != nil {
				m.logger.Println("ERR: Failed to update active cluster", err)
				return err
			}
			requeuedTaskRunnersStatus, err := m.handleExpiredTaskRunners(heartbeatTime)
			if err != nil {
				m.logger.Println("ERR: Failed to handle expired task runners", err)
				return err
			}
			for _, afterHeartbeatHook := range m.afterHeartbeatHooks {
				afterHeartbeatHook(heartbeat, updateActiveClusterStatus, requeuedTaskRunnersStatus)
			}
		case <-m.heartbeatChannel:
			return nil
		}
	}
}

func (m *Manager) IsActive() bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.active
}

func (m *Manager) Active(active bool) (bool, bool) {
	m.lock.Lock()
	activateManager := !m.active && active
	deactivateManager := m.active && !active
	if activateManager || deactivateManager {
		m.active = active
		for _, worker := range m.workers {
			worker.fetcher.Active(active)
		}
	}
	m.lock.Unlock()
	return activateManager, deactivateManager
}

func (m *Manager) removeHeartbeat() error {
	m.heartbeatChannel <- true
	heartbeatID, err := m.getHeartbeatID()
	if err != nil {
		return err
	}
	err = m.opts.store.RemoveHeartbeat(context.Background(), heartbeatID)
	return err
}

func (m *Manager) sendHeartbeat(heartbeatTime time.Time) (*storage.Heartbeat, error) {
	heartbeat, err := m.buildHeartbeat(heartbeatTime)
	if err != nil {
		return heartbeat, err
	}

	err = m.opts.store.SendHeartbeat(context.Background(), heartbeat, m.opts.Heartbeat.ManagerTTL)
	return heartbeat, err
}

func (m *Manager) handleExpiredTaskRunners(now time.Time) ([]requeuedTaskRunnerStatus, error) {
	expiredTaskRunnerKeys, err := m.opts.store.GetExpiredTaskRunnerKeys(context.Background(), now.Add(-m.opts.Heartbeat.TaskRunnerEvictInterval).Unix())
	if err != nil {
		return nil, err
	}

	var requeudTaskRunners []requeuedTaskRunnerStatus
	requeuedInProgressQueues := make(map[string]bool)
	for _, expiredTaskRunnerKey := range expiredTaskRunnerKeys {
		taskRunnerInfo, err := m.opts.store.GetTaskRunnerInfo(context.Background(), expiredTaskRunnerKey)
		if err != nil {
			return nil, err
		}
		var requeuedMsgs []string
		if _, exists := requeuedInProgressQueues[taskRunnerInfo.InProgressQueue]; exists {
			continue
		}
		requeuedMsgs, err = m.opts.store.RequeueMessagesFromInProgressQueue(context.Background(), taskRunnerInfo.InProgressQueue, taskRunnerInfo.Queue)
		if err != nil {
			return nil, err
		}
		expiredTaskRunnerStatus := requeuedTaskRunnerStatus{
			pid:             taskRunnerInfo.Pid,
			tid:             taskRunnerInfo.Tid,
			queue:           taskRunnerInfo.Queue,
			inprogressQueue: taskRunnerInfo.InProgressQueue,
			requeuedMsgs:    requeuedMsgs,
		}
		err = m.opts.store.EvictTaskRunnerInfo(context.Background(), taskRunnerInfo.Pid, taskRunnerInfo.Tid)
		if err != nil {
			return requeudTaskRunners, err
		}
		requeudTaskRunners = append(requeudTaskRunners, expiredTaskRunnerStatus)
		requeuedInProgressQueues[taskRunnerInfo.InProgressQueue] = true
	}
	return requeudTaskRunners, nil
}

func (m *Manager) updateActiveCluster(now time.Time) (*updateActiveClusterStatus, error) {
	if m.opts.ActivePassiveFailover == nil {
		return nil, nil
	}
	ctx := context.Background()
	err := m.opts.store.AddActiveCluster(ctx, m.opts.ActivePassiveFailover.ClusterID, m.opts.ActivePassiveFailover.ClusterPriority)
	if err != nil {
		return nil, err
	}
	err = m.opts.store.EvictExpiredClusters(ctx, now.Add(-m.opts.Heartbeat.ClusterEvictInterval).Unix())
	activeClusterID, err := m.opts.store.GetHighestPriorityActiveClusterID(ctx)
	activeManager := m.opts.ActivePassiveFailover.ClusterID == activeClusterID
	var updateClusterStatus *updateActiveClusterStatus
	activateManager, deactivateManager := m.Active(activeManager)
	updateClusterStatus = &updateActiveClusterStatus{activeClusterID: activeClusterID, activateManager: activateManager, deactivateManager: deactivateManager}

	if err != nil {
		return updateClusterStatus, err
	}
	return updateClusterStatus, nil
}

func (m *Manager) debugLogHeartbeat(heartbeat *storage.Heartbeat, updateActiveCluster *updateActiveClusterStatus, requeuedTaskRunnersStatus []requeuedTaskRunnerStatus) {
	if updateActiveCluster != nil {
		m.logger.Println(fmt.Sprintf("[%s] Updated active cluster: {'clusterID': %s, 'activeClusterID': %s, 'activateManager': %t, 'deactivateManager': %t }",
			heartbeat.Beat.String(), m.opts.ActivePassiveFailover.ClusterID, updateActiveCluster.activeClusterID, updateActiveCluster.activateManager, updateActiveCluster.deactivateManager))
	}
	if len(requeuedTaskRunnersStatus) > 0 {
		for _, requeuedTaskRunnerStatus := range requeuedTaskRunnersStatus {
			for _, msg := range requeuedTaskRunnerStatus.requeuedMsgs {
				m.logger.Println(fmt.Sprintf("[%s] Requeued message: {'inProgressQueue': '%s', 'queue': %s', 'message': '%s'}",
					heartbeat.Beat.String(), requeuedTaskRunnerStatus.inprogressQueue, requeuedTaskRunnerStatus.queue, msg))
			}
		}
	}
}
