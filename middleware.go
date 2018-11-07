package workers

type JobFunc func(message *Msg) error

type MiddlewareFunc func(queue string, m *Manager, next JobFunc) JobFunc

type Middlewares []MiddlewareFunc

func (m Middlewares) Append(mid MiddlewareFunc) Middlewares {
	return append(m, mid)
}

func (m Middlewares) Prepend(mid MiddlewareFunc) Middlewares {
	return append(Middlewares{mid}, m...)
}

func (ms Middlewares) build(queue string, mgr *Manager, final JobFunc) JobFunc {
	for i := len(ms) - 1; i >= 0; i-- {
		final = ms[i](queue, mgr, final)
	}
	return final
}

func NewMiddlewares(mids ...MiddlewareFunc) Middlewares {
	return Middlewares(mids)
}

// This is a variable for testing reasons
var defaultMiddlewares = NewMiddlewares(
	LogMiddleware,
	RetryMiddleware,
	StatsMiddleware,
)

func DefaultMiddlewares() Middlewares {
	return defaultMiddlewares
}

// NopMiddleware does nothing
func NopMiddleware(queue string, mgr *Manager, final JobFunc) JobFunc {
	return final
}
