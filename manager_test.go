package workers

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type customMid struct {
	trace      []string
	base       string
	mutex      sync.Mutex
	timesBuilt int
}

func (m *customMid) AsMiddleware() MiddlewareFunc {
	return func(queue string, next JobFunc) JobFunc {
		m.mutex.Lock()
		defer m.mutex.Unlock()
		m.timesBuilt += 1
		return func(message *Msg) (err error) {
			m.mutex.Lock()
			defer m.mutex.Unlock()

			m.trace = append(m.trace, m.base+"1")
			err = next(message)
			m.trace = append(m.trace, m.base+"2")
			return
		}
	}
}

func (m *customMid) Trace() []string {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	t := make([]string, len(m.trace))
	copy(t, m.trace)

	return t
}

func TestNewManager(t *testing.T) {
	namespace := "prod"
	setupTestConfigWithNamespace(namespace)

	processed := make(chan *Args)
	testJob := (func(message *Msg) error {
		processed <- message.Args()
		return nil
	})

	//sets queue with namespace
	manager := newManager("myqueue", testJob, 10)
	assert.Equal(t, "prod:queue:myqueue", manager.queue)

	//sets job function
	manager = newManager("myqueue", testJob, 10, NopMiddleware)

	f1 := reflect.ValueOf(manager.handler)
	f2 := reflect.ValueOf(testJob)
	assert.Equal(t, f1.Pointer(), f2.Pointer())

	//sets worker concurrency
	manager = newManager("myqueue", testJob, 10)
	assert.Equal(t, 10, manager.concurrency)

	mid1 := &customMid{base: "0"}
	oldMiddlewares := defaultMiddlewares
	defer func() {
		defaultMiddlewares = oldMiddlewares
	}()
	defaultMiddlewares = NewMiddlewares(mid1.AsMiddleware())

	//no per-manager middleware means 'use global Middleware object
	manager = newManager("myqueue", testJob, 10)
	assert.Equal(t, mid1.timesBuilt, 1)

	//per-manager middlewares create separate middleware chains
	mid2 := &customMid{base: "0"}
	manager = newManager("myqueue", testJob, 10, mid2.AsMiddleware())
	assert.Equal(t, mid1.timesBuilt, 1) // Make sure it doesn't use the defaults
	assert.Equal(t, mid2.timesBuilt, 1)
}

var message, _ = NewMsg("{\"foo\":\"bar\",\"args\":[\"foo\",\"bar\"]}")
var message2, _ = NewMsg("{\"foo\":\"bar2\",\"args\":[\"foo\",\"bar2\"]}")

func TestQueueProcessing(t *testing.T) {
	namespace := "prod"
	setupTestConfigWithNamespace(namespace)
	rc := Config.Client

	processed := make(chan *Args)
	testJob := (func(message *Msg) error {
		processed <- message.Args()
		return nil
	})

	manager := newManager("manager1", testJob, 1)

	rc.LPush("prod:queue:manager1", message.ToJson()).Result()
	rc.LPush("prod:queue:manager1", message2.ToJson()).Result()

	manager.start()

	actual := <-processed
	assert.Equal(t, message.Args().ToJson(), actual.ToJson())
	actual = <-processed
	assert.Equal(t, message2.Args().ToJson(), actual.ToJson())

	manager.quit()

	len, _ := rc.LLen("prod:queue:manager1").Result()
	assert.Equal(t, int64(0), len)
}

func TestDrainQueueOnExit(t *testing.T) {
	namespace := "prod"
	setupTestConfigWithNamespace(namespace)
	rc := Config.Client

	processed := make(chan *Args)

	sentinel, _ := NewMsg("{\"foo\":\"bar2\",\"args\":\"sentinel\"}")

	drained := false

	slowJob := (func(message *Msg) error {
		if message.ToJson() == sentinel.ToJson() {
			drained = true
		} else {
			processed <- message.Args()
		}

		time.Sleep(1 * time.Second)

		return nil
	})
	manager := newManager("manager1", slowJob, 10)

	for i := 0; i < 9; i++ {
		rc.LPush("prod:queue:manager1", message.ToJson()).Result()
	}
	rc.LPush("prod:queue:manager1", sentinel.ToJson()).Result()

	manager.start()
	for i := 0; i < 9; i++ {
		<-processed
	}
	manager.quit()

	len, _ := rc.LLen("prod:queue:manager1").Result()
	assert.Equal(t, int64(0), len)
	assert.True(t, drained)
}

func TestMultiMiddleware(t *testing.T) {
	//per-manager middlwares are called separately, global middleware is called in each manager
	namespace := "prod"
	setupTestConfigWithNamespace(namespace)
	rc := Config.Client

	processed := make(chan *Args)

	testJob := (func(message *Msg) error {
		processed <- message.Args()
		return nil
	})

	mid1 := &customMid{base: "1"}
	mid2 := &customMid{base: "2"}
	mid3 := &customMid{base: "3"}

	oldMiddlewares := defaultMiddlewares
	defer func() {
		defaultMiddlewares = oldMiddlewares
	}()
	defaultMiddlewares = NewMiddlewares(mid1.AsMiddleware())

	manager1 := newManager("manager1", testJob, 10)
	manager2 := newManager("manager2", testJob, 10, mid2.AsMiddleware())
	manager3 := newManager("manager3", testJob, 10, mid3.AsMiddleware())

	rc.LPush("prod:queue:manager1", message.ToJson()).Result()
	rc.LPush("prod:queue:manager2", message.ToJson()).Result()
	rc.LPush("prod:queue:manager3", message.ToJson()).Result()

	manager1.start()
	manager2.start()
	manager3.start()

	<-processed
	<-processed
	<-processed

	assert.Equal(t, []string{"11", "12"}, mid1.Trace())
	assert.Equal(t, []string{"21", "22"}, mid2.Trace())
	assert.Equal(t, []string{"31", "32"}, mid3.Trace())

	manager1.quit()
	manager2.quit()
	manager3.quit()
}

func TestStopFetching(t *testing.T) {
	//prepare stops fetching new messages from queue
	namespace := "prodstop:"
	setupTestConfigWithNamespace(namespace)
	rc := Config.Client

	processed := make(chan *Args)
	testJob := (func(message *Msg) error {
		processed <- message.Args()
		return nil
	})

	manager := newManager("manager2", testJob, 10)
	manager.start()

	manager.prepare()

	rc.LPush("prodstop:queue:manager2", message.ToJson()).Result()
	rc.LPush("prodstop:queue:manager2", message2.ToJson()).Result()

	manager.quit()

	len, _ := rc.LLen("prodstop:queue:manager2").Result()
	assert.Equal(t, int64(2), len)
}
