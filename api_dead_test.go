package workers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/friesencr/go-workers2/storage"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func deadSet(namespace string) string {
	return namespace + storage.DeadKey
}

func deadJids(d Dead) []string {
	var jids []string
	for _, m := range d.DeadJobs {
		jids = append(jids, m.Jid())
	}
	return jids
}

func TestDead_Empty(t *testing.T) {
	a := apiServer{}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest("GET", "/dead", nil)
	a.Dead(recorder, request)

	assert.Equal(t, "[]\n", recorder.Body.String())
}

func TestRetryExhaustionSendsToDeadSetAndRequeues(t *testing.T) {
	ctx := context.Background()

	opts, err := SetupDefaultTestOptionsWithNamespace("prod")
	assert.NoError(t, err)
	rc := opts.client

	mgr := &Manager{opts: opts}

	message, _ := NewMsg("{\"class\":\"clazz\",\"jid\":\"2\",\"retry\":true,\"retry_count\":25}")

	wares.build("prod:myqueue", mgr, panickingFunc)(message)

	retries, _ := rc.ZCard(ctx, retryQueue(opts.Namespace)).Result()
	assert.Equal(t, int64(0), retries)
	dead, _ := rc.ZCard(ctx, deadSet(opts.Namespace)).Result()
	assert.Equal(t, int64(1), dead)

	listed, err := mgr.GetDead(0, 10)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), listed.TotalDeadCount)
	if assert.Len(t, listed.DeadJobs, 1) {
		deadJob := listed.DeadJobs[0]
		queue, _ := deadJob.Get("queue").String()
		errorMessage, _ := deadJob.Get("error_message").String()

		assert.Equal(t, "2", deadJob.Jid())
		assert.Equal(t, "clazz", deadJob.Class())
		assert.Equal(t, "prod:myqueue", queue)
		assert.Equal(t, errorText, errorMessage)
	}

	err = mgr.RequeueDead("2")
	assert.NoError(t, err)

	dead, _ = rc.ZCard(ctx, deadSet(opts.Namespace)).Result()
	assert.Equal(t, int64(0), dead)

	queued, err := rc.LRange(ctx, "prod:queue:myqueue", 0, -1).Result()
	assert.NoError(t, err)
	if assert.Len(t, queued, 1) {
		requeued, err := NewMsg(queued[0])
		assert.NoError(t, err)
		retryCount, _ := requeued.Get("retry_count").Int()
		enqueuedAt, _ := requeued.Get("enqueued_at").Float64()

		assert.Equal(t, "2", requeued.Jid())
		assert.Equal(t, "clazz", requeued.Class())
		assert.Equal(t, 25, retryCount)
		assert.InDelta(t, nowToSecondsWithNanoPrecision(), enqueuedAt, 1)

		// a requeued job that still exhausts its retries is attempted once more, then dies again
		wares.build("prod:myqueue", mgr, panickingFunc)(requeued)

		retries, _ = rc.ZCard(ctx, retryQueue(opts.Namespace)).Result()
		assert.Equal(t, int64(0), retries)
		dead, _ = rc.ZCard(ctx, deadSet(opts.Namespace)).Result()
		assert.Equal(t, int64(1), dead)
	}

	assert.Equal(t, storage.NoMessage, mgr.RequeueDead("3"))
	queuedCount, _ := rc.LLen(ctx, "prod:queue:myqueue").Result()
	assert.Equal(t, int64(1), queuedCount)
}

func TestGetDeadPaginatesNewestFirst(t *testing.T) {
	ctx := context.Background()

	opts, err := SetupDefaultTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	mgr := &Manager{opts: opts}

	for i, jid := range []string{"oldest", "middle", "newest"} {
		message, _ := NewMsg(fmt.Sprintf("{\"jid\":%q,\"queue\":\"prod:myqueue\"}", jid))
		err := opts.store.EnqueueDeadMessage(ctx, float64(i), message.ToJson())
		assert.NoError(t, err)
	}

	first, err := mgr.GetDead(0, 2)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), first.TotalDeadCount)
	assert.Equal(t, []string{"newest", "middle"}, deadJids(first))

	second, err := mgr.GetDead(1, 2)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), second.TotalDeadCount)
	assert.Equal(t, []string{"oldest"}, deadJids(second))

	beyond, err := mgr.GetDead(2, 2)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), beyond.TotalDeadCount)
	assert.Empty(t, beyond.DeadJobs)

	_, err = mgr.GetDead(0, 0)
	assert.Error(t, err)

	_, err = mgr.GetDead(math.MaxUint64, 2)
	assert.Error(t, err)
}

func TestEnqueueDeadMessageKeepsNewestDeadMaxJobs(t *testing.T) {
	ctx := context.Background()

	opts, err := SetupDefaultTestOptionsWithNamespace("prod")
	assert.NoError(t, err)
	rc := opts.client

	const maxJobs int64 = 3
	const overflow = 2
	store := storage.NewRedisStore(opts.Namespace, rc, opts.Logger, maxJobs, 0)

	now := nowToSecondsWithNanoPrecision()
	seeded := make([]redis.Z, maxJobs+overflow-1)
	for i := range seeded {
		// member names sort opposite to their scores, so only a trim by score keeps the newest
		seeded[i] = redis.Z{
			Score:  now - float64(len(seeded)-i),
			Member: fmt.Sprintf("job-%05d", len(seeded)-i),
		}
	}
	_, err = rc.ZAdd(ctx, deadSet(opts.Namespace), seeded...).Result()
	assert.NoError(t, err)

	err = store.EnqueueDeadMessage(ctx, now, "job-newest")
	assert.NoError(t, err)

	count, _ := rc.ZCard(ctx, deadSet(opts.Namespace)).Result()
	assert.Equal(t, maxJobs, count)

	for _, trimmed := range seeded[:overflow] {
		_, err := rc.ZScore(ctx, deadSet(opts.Namespace), trimmed.Member.(string)).Result()
		assert.Equal(t, redis.Nil, err, trimmed.Member)
	}

	oldest, _ := rc.ZRange(ctx, deadSet(opts.Namespace), 0, 0).Result()
	assert.Equal(t, []string{seeded[overflow].Member.(string)}, oldest)
	newest, _ := rc.ZRevRange(ctx, deadSet(opts.Namespace), 0, 0).Result()
	assert.Equal(t, []string{"job-newest"}, newest)
}

func TestEnqueueDeadMessageDropsEntriesOlderThanDeadMaxAge(t *testing.T) {
	ctx := context.Background()

	opts, err := SetupDefaultTestOptionsWithNamespace("prod")
	assert.NoError(t, err)
	rc := opts.client

	maxAge := time.Hour
	store := storage.NewRedisStore(opts.Namespace, rc, opts.Logger, 0, maxAge)

	now := nowToSecondsWithNanoPrecision()
	age := maxAge.Seconds()
	_, err = rc.ZAdd(ctx, deadSet(opts.Namespace),
		redis.Z{Score: now - age - 3600, Member: "expired-by-an-hour"},
		redis.Z{Score: now - age - 1, Member: "expired-by-a-second"},
		redis.Z{Score: now - age + 60, Member: "a-minute-from-expiry"},
	).Result()
	assert.NoError(t, err)

	err = store.EnqueueDeadMessage(ctx, now, "newest")
	assert.NoError(t, err)

	remaining, _ := rc.ZRange(ctx, deadSet(opts.Namespace), 0, -1).Result()
	assert.Equal(t, []string{"a-minute-from-expiry", "newest"}, remaining)
}

func TestRequeueDeadConcurrentlyEnqueuesOnce(t *testing.T) {
	ctx := context.Background()

	opts, err := SetupDefaultTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	mgr := &Manager{opts: opts}

	message, _ := NewMsg("{\"class\":\"clazz\",\"jid\":\"2\",\"retry\":false}")
	wares.build("prod:myqueue", mgr, panickingFunc)(message)

	const requeuers = 10
	errs := make(chan error, requeuers)
	var wg sync.WaitGroup
	wg.Add(requeuers)
	for i := 0; i < requeuers; i++ {
		go func() {
			defer wg.Done()
			errs <- mgr.RequeueDead("2")
		}()
	}
	wg.Wait()
	close(errs)

	var requeued, notFound int
	for err := range errs {
		switch err {
		case nil:
			requeued++
		case storage.NoMessage:
			notFound++
		default:
			assert.NoError(t, err)
		}
	}
	assert.Equal(t, 1, requeued)
	assert.Equal(t, requeuers-1, notFound)

	queued, _ := opts.client.LLen(ctx, "prod:queue:myqueue").Result()
	assert.Equal(t, int64(1), queued)
	dead, _ := opts.client.ZCard(ctx, deadSet(opts.Namespace)).Result()
	assert.Equal(t, int64(0), dead)
}

func TestDead_ListsOverHTTP(t *testing.T) {
	a := &apiServer{
		logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
	}

	opts, err := SetupDefaultTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	mgr := &Manager{opts: opts}
	a.registerManager(mgr)

	message, _ := NewMsg("{\"class\":\"clazz\",\"jid\":\"2\",\"retry\":false}")
	wares.build("prod:myqueue", mgr, panickingFunc)(message)

	type listedDead struct {
		TotalDeadCount int64                    `json:"total_dead_count"`
		DeadJobs       []map[string]interface{} `json:"dead_jobs"`
	}

	recorder := httptest.NewRecorder()
	a.Dead(recorder, httptest.NewRequest("GET", "/dead?page=0&page_size=5", nil))
	assert.Equal(t, http.StatusOK, recorder.Code)

	var listed []listedDead
	err = json.Unmarshal(recorder.Body.Bytes(), &listed)
	assert.NoError(t, err)
	if assert.Len(t, listed, 1) {
		assert.Equal(t, int64(1), listed[0].TotalDeadCount)
		if assert.Len(t, listed[0].DeadJobs, 1) {
			assert.Equal(t, "2", listed[0].DeadJobs[0]["jid"])
		}
	}

	recorder = httptest.NewRecorder()
	a.Dead(recorder, httptest.NewRequest("GET", "/dead?page_size=0", nil))
	assert.Equal(t, http.StatusBadRequest, recorder.Code)

	err = mgr.RequeueDead("2")
	assert.NoError(t, err)

	recorder = httptest.NewRecorder()
	a.Dead(recorder, httptest.NewRequest("GET", "/dead", nil))
	assert.Equal(t, http.StatusOK, recorder.Code)

	var relisted []listedDead
	err = json.Unmarshal(recorder.Body.Bytes(), &relisted)
	assert.NoError(t, err)
	if assert.Len(t, relisted, 1) {
		assert.Equal(t, int64(0), relisted[0].TotalDeadCount)
		assert.Empty(t, relisted[0].DeadJobs)
	}
}

func TestRegisterAPIEndpointsDoesNotExposeDeadRequeue(t *testing.T) {
	mux := http.NewServeMux()
	RegisterAPIEndpoints(mux)

	for _, path := range []string{"/stats", "/retries", "/dead"} {
		_, pattern := mux.Handler(httptest.NewRequest("GET", path, nil))
		assert.Equal(t, path, pattern)
	}

	_, pattern := mux.Handler(httptest.NewRequest("POST", "/dead/requeue?jid=2", nil))
	assert.Equal(t, "", pattern)
}
