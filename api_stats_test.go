package workers

import (
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStats_Empty(t *testing.T) {
	a := apiServer{}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest("GET", "/stats", nil)
	a.Stats(recorder, request)

	assert.Equal(t, "[]\n", recorder.Body.String())
}
