package workers

import (
	"context"
	"errors"
	"log"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRetries_Empty(t *testing.T) {
	a := apiServer{}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest("GET", "/retries", nil)
	a.Retries(recorder, request)

	assert.Equal(t, "[]\n", recorder.Body.String())
}

func TestRetries_NotEmpty(t *testing.T) {
	a := &apiServer{
		logger: log.New(os.Stdout, "go-workers2: ", log.Ldate|log.Lmicroseconds),
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest("GET", "/retries", nil)
	a.Retries(recorder, request)

	assert.Equal(t, "[]\n", recorder.Body.String())

	ctx := context.Background()

	//puts messages in retry queue when they fail
	message, _ := NewMsg("{\"jid\":\"2\",\"retry\":true}")

	tests := []struct {
		name string
		f    JobFunc
	}{
		{
			name: "retry on panic",
			f:    panickingFunc,
		},
		{
			name: "retry on error",
			f: func(m *Msg) error {
				return errors.New("ERROR")
			},
		},
	}
	for _, tt := range tests {
		opts, err := setupTestOptionsWithNamespace("prod")
		assert.NoError(t, err)

		mgr := &Manager{opts: opts}

		a.registerManager(mgr)

		// Test panic
		wares.build("myqueue", mgr, tt.f)(message)

		retries, _ := opts.client.ZRange(ctx, retryQueue(opts.Namespace), 0, 1).Result()
		assert.Len(t, retries, 1)
		assert.Equal(t, message.ToJson(), retries[0])
	}

	recorder = httptest.NewRecorder()
	a.Retries(recorder, request)

	assert.NotEqual(t, "[]\n", recorder.Body.String())
}
