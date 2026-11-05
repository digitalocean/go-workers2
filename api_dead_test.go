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

	"github.com/friesencr/go-workers2/storage"
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

func TestDead_ListsAndRequeuesOverHTTP(t *testing.T) {
	ctx := context.Background()

	a := &apiServer{
		logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
	}

	opts, err := SetupDefaultTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	mgr := &Manager{opts: opts}
	a.registerManager(mgr)

	message, _ := NewMsg("{\"class\":\"clazz\",\"jid\":\"2\",\"retry\":false}")
	wares.build("prod:myqueue", mgr, panickingFunc)(message)

	recorder := httptest.NewRecorder()
	a.Dead(recorder, httptest.NewRequest("GET", "/dead?page=0&page_size=5", nil))
	assert.Equal(t, http.StatusOK, recorder.Code)

	var listed []struct {
		TotalDeadCount int64                    `json:"total_dead_count"`
		DeadJobs       []map[string]interface{} `json:"dead_jobs"`
	}
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

	recorder = httptest.NewRecorder()
	a.RequeueDead(recorder, httptest.NewRequest("GET", "/dead/requeue?jid=2", nil))
	assert.Equal(t, http.StatusMethodNotAllowed, recorder.Code)

	recorder = httptest.NewRecorder()
	a.RequeueDead(recorder, httptest.NewRequest("POST", "/dead/requeue", nil))
	assert.Equal(t, http.StatusBadRequest, recorder.Code)

	recorder = httptest.NewRecorder()
	a.RequeueDead(recorder, httptest.NewRequest("POST", "/dead/requeue?jid=2", nil))
	assert.Equal(t, http.StatusNoContent, recorder.Code)

	queued, _ := opts.client.LLen(ctx, "prod:queue:myqueue").Result()
	assert.Equal(t, int64(1), queued)

	recorder = httptest.NewRecorder()
	a.RequeueDead(recorder, httptest.NewRequest("POST", "/dead/requeue?jid=2", nil))
	assert.Equal(t, http.StatusNotFound, recorder.Code)

	queued, _ = opts.client.LLen(ctx, "prod:queue:myqueue").Result()
	assert.Equal(t, int64(1), queued)
}
