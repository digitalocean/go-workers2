package workers

// JobFunc is a message processor
type JobFunc func(message *Msg) error

// MiddlewareFunc is an extra function on the processing pipeline
type MiddlewareFunc func(queue string, m *Manager, next JobFunc) JobFunc

// Middlewares contains the lists of all configured middleware functions
type Middlewares []MiddlewareFunc

// Append adds middleware to the end of the processing pipeline
func (m Middlewares) Append(mid MiddlewareFunc) Middlewares {
	return append(m, mid)
}

// Prepend adds middleware to the front of the processing pipeline
func (m Middlewares) Prepend(mid MiddlewareFunc) Middlewares {
	return append(Middlewares{mid}, m...)
}

func (m Middlewares) build(queue string, mgr *Manager, final JobFunc) JobFunc {
	for i := len(m) - 1; i >= 0; i-- {
		final = m[i](queue, mgr, final)
	}
	return final
}

// NewMiddlewares creates the processing pipeline given the list of middleware funcs
func NewMiddlewares(mids ...MiddlewareFunc) Middlewares {
	return Middlewares(mids)
}

// This is a variable for testing reasons
var defaultMiddlewares = NewMiddlewares(
	LogMiddleware,
	RetryMiddleware,
	StatsMiddleware,
)

// DefaultMiddlewares creates the default middleware pipeline
func DefaultMiddlewares() Middlewares {
	return defaultMiddlewares
}

// NopMiddleware does nothing
func NopMiddleware(queue string, mgr *Manager, final JobFunc) JobFunc {
	return final
}
