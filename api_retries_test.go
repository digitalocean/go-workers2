package workers

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/stretchr/testify/assert"
	"log"
	"net/http/httptest"
	"os"
	"testing"
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

	// test API replies without registered workers
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest("GET", "/retries", nil)
	a.Retries(recorder, request)

	assert.Equal(t, "[]\n", recorder.Body.String())

	ctx := context.Background()

	// test API replies with registered workers
	opts, err := SetupDefaultTestOptionsWithNamespace("prod")
	assert.NoError(t, err)

	mgr := &Manager{opts: opts}
	a.registerManager(mgr)

	recorder = httptest.NewRecorder()
	request = httptest.NewRequest("GET", "/retries", nil)
	a.Retries(recorder, request)

	actualWithManagerBytes := recorder.Body.Bytes()
	actualReplyParsed := []*Retries{}
	err = json.Unmarshal(actualWithManagerBytes, &actualReplyParsed)
	assert.NoError(t, err)
	assert.Equal(t, []*Retries{{}}, actualReplyParsed)

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

	ctx = context.Background()
	var messages []string
	for index, test := range tests {
		// Test panic
		wares.build("myqueue", mgr, test.f)(message)

		// retries order is not guaranteed
		retries, err := opts.client.ZRange(ctx, retryQueue(opts.Namespace), 0, -1).Result()
		assert.NoError(t, err)
		assert.Len(t, retries, index+1)
		messages = append(messages, message.ToJson())
		assert.ElementsMatch(t, messages, retries)
	}

	recorder = httptest.NewRecorder()
	request = httptest.NewRequest("GET", "/retries", nil)
	a.Retries(recorder, request)
	assert.NoError(t, err)
	assert.NotEqual(t, "[]\n", recorder.Body.String())
	assert.NotEqual(t, string(actualWithManagerBytes), recorder.Body.String())
}
