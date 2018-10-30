package workers

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTaskRunner_process(t *testing.T) {
	msg, _ := NewMsg(`{}`)

	t.Run("handles-panic", func(t *testing.T) {
		tr := newTaskRunner(func(m *Msg) error {
			panic("task-test-panic")
		})
		err := tr.process(msg)
		assert.EqualError(t, err, "task-test-panic")

	})

	t.Run("returns-error", func(t *testing.T) {
		var errorToRet error
		tr := newTaskRunner(func(m *Msg) error {
			return errorToRet
		})
		err := tr.process(msg)
		assert.NoError(t, err)

		errorToRet = errors.New("ret me")
		err = tr.process(msg)
		assert.EqualError(t, err, errorToRet.Error())
	})
}

func TestTaskRunner(t *testing.T) {
	msgCh := make(chan *Msg)
	doneCh := make(chan *Msg)
	readyCh := make(chan bool)

	syncCh := make(chan bool)
	noSyncMsg := func() *Msg {
		m, _ := NewMsg(`{}`)
		return m
	}
	syncMsg := func() *Msg {
		m, _ := NewMsg(`{"sync": true}`)
		return m
	}

	tr := newTaskRunner(func(m *Msg) error {
		if m.Get("sync").MustBool() {
			syncCh <- true
			<-syncCh
		}
		return nil
	})

	var wg sync.WaitGroup

	go func() {
		wg.Add(1)
		tr.work(msgCh, doneCh, readyCh)
		wg.Done()
	}()

	t.Run("consumes-messages", func(t *testing.T) {
		msgCh <- noSyncMsg()
		doneMsg := <-doneCh
		assert.NotNil(t, doneMsg)
		assert.NotZero(t, doneMsg.startedAt)

		msgCh <- noSyncMsg()
		doneMsg = <-doneCh
		assert.NotNil(t, doneMsg)
		assert.NotZero(t, doneMsg.startedAt)
	})

	t.Run("sends-to-ready-when-no-message", func(t *testing.T) {
		<-readyCh
	})

	t.Run(".inProgressMessage", func(t *testing.T) {
		msgCh <- syncMsg()
		<-syncCh
		ipm := tr.inProgressMessage()
		assert.NotNil(t, ipm)
		assert.NotZero(t, ipm.startedAt)

		syncCh <- true
		doneMsg := <-doneCh
		assert.NotNil(t, doneMsg)
		assert.NotZero(t, doneMsg.startedAt)

		ipm = tr.inProgressMessage()
		assert.Nil(t, ipm)
	})

	t.Run(".quit", func(t *testing.T) {
		tr.quit()
		// wg.Wait will cause the test to timeout if tr.quit() doesn't shut down the taskRunner
		wg.Wait()
	})

}
