package workers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func arrayCompare(a1, a2 []string) bool {
	if len(a1) != len(a2) {
		return false
	}

	for i := 0; i < len(a1); i++ {
		if a1[i] != a2[i] {
			return false
		}
	}

	return true
}

type orderMiddleware struct {
	name  string
	order *[]string
}

func (m *orderMiddleware) f() MiddlewareFunc {
	return func(queue string, mgr *Manager, next JobFunc) JobFunc {
		return func(message *Msg) (result error) {
			*m.order = append(*m.order, m.name+" enter")
			result = next(message)
			*m.order = append(*m.order, m.name+" leave")
			return
		}
	}
}

func TestNewMiddlewares(t *testing.T) {
	//no middleware
	middlewares := NewMiddlewares()
	assert.Equal(t, 0, len(middlewares))

	//middleware set when initializing
	order := make([]string, 0)
	first := orderMiddleware{"m1", &order}
	second := orderMiddleware{"m2", &order}
	middlewares = NewMiddlewares(first.f(), second.f())

	message, _ := NewMsg("{\"foo\":\"bar\"}")
	middlewares.build("myqueue", nil, func(message *Msg) error {
		order = append(order, "job")
		return nil
	})(message)

	expectedOrder := []string{
		"m1 enter",
		"m2 enter",
		"job",
		"m2 leave",
		"m1 leave",
	}

	assert.Equal(t, expectedOrder, order)
}

func TestAppendMiddleware(t *testing.T) {
	order := make([]string, 0)
	first := orderMiddleware{"m1", &order}
	second := orderMiddleware{"m2", &order}
	middleware := NewMiddlewares().Append(first.f()).Append(second.f())

	message, _ := NewMsg("{\"foo\":\"bar\"}")
	middleware.build("myqueue", nil, func(message *Msg) error {
		order = append(order, "job")
		return nil
	})(message)

	expectedOrder := []string{
		"m1 enter",
		"m2 enter",
		"job",
		"m2 leave",
		"m1 leave",
	}

	assert.Equal(t, expectedOrder, order)
}

func TestPrependMiddleware(t *testing.T) {
	order := make([]string, 0)
	first := orderMiddleware{"m1", &order}
	second := orderMiddleware{"m2", &order}

	middleware := NewMiddlewares().Prepend(first.f()).Prepend(second.f())

	message, _ := NewMsg("{\"foo\":\"bar\"}")
	middleware.build("myqueue", nil, func(message *Msg) error {
		order = append(order, "job")
		return nil
	})(message)

	expectedOrder := []string{
		"m2 enter",
		"m1 enter",
		"job",
		"m1 leave",
		"m2 leave",
	}

	assert.Equal(t, expectedOrder, order)
}
