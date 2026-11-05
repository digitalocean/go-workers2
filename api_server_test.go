package workers

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAPIServer_HandlersDuringManagerRegistration(t *testing.T) {
	a := &apiServer{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	opts, err := SetupDefaultTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	a.registerManager(&Manager{uuid: "stable", opts: opts})

	done := make(chan struct{})
	var churn sync.WaitGroup
	churn.Add(1)
	go func() {
		defer churn.Done()
		churning := &Manager{uuid: "churning", opts: opts}
		for {
			select {
			case <-done:
				return
			default:
				a.registerManager(churning)
				a.deregisterManager(churning)
			}
		}
	}()

	handlers := map[string]http.HandlerFunc{
		"/stats":   a.Stats,
		"/retries": a.Retries,
		"/dead":    a.Dead,
	}
	for i := 0; i < 100; i++ {
		for path, handler := range handlers {
			recorder := httptest.NewRecorder()
			handler(recorder, httptest.NewRequest("GET", path, nil))
			assert.Equal(t, http.StatusOK, recorder.Code, path)
		}
	}
	close(done)
	churn.Wait()
}
