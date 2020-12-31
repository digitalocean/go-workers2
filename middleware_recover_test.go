package workers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRecover(t *testing.T) {
	wares := NewMiddlewares(RecoverMiddleware)
	message, _ := NewMsg("{\"jid\":\"2\"}")

	t.Run("recovers", func(t *testing.T) {
		opts, err := setupTestOptionsWithNamespace("prod")
		assert.NoError(t, err)

		mgr := &Manager{opts: opts}

		// Test panic
		err = wares.build("myqueue", mgr, panickingFunc)(message)
		assert.Error(t, err)
	})
}
