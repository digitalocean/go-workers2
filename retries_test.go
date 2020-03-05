package workers

import (
	"net/http/httptest"
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
