package workers

import (
	"fmt"
)

// RecoverMiddleware is the default panic/recover middleware.
func RecoverMiddleware(queue string, mgr *Manager, next JobFunc) JobFunc {
	return func(message *Msg) (err error) {
		defer func() {
			if e := recover(); e != nil {
				var ok bool
				if err, ok = e.(error); !ok {
					err = fmt.Errorf("%v", e)
				}
			}
		}()

		return next(message)
	}
}
