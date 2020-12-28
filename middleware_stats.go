package workers

import (
	"fmt"
)

// StatsMiddleware middleware to collect stats on processed messages
func StatsMiddleware(queue string, mgr *Manager, next JobFunc) JobFunc {
	return func(message *Msg) (err error) {
		defer func() {
			if e := recover(); e != nil {
				var ok bool
				if err, ok = e.(error); !ok {
					err = fmt.Errorf("%v", e)
				}

				if err != nil {
					incrementStats(mgr, "failed")
				}
			}

		}()

		err = next(message)
		if err != nil {
			incrementStats(mgr, "failed")
		} else {
			incrementStats(mgr, "processed")
		}

		return
	}
}

func incrementStats(mgr *Manager, metric string) {
	err := mgr.opts.store.IncrementStats(metric)

	if err != nil {
		mgr.logger.Println("couldn't save stats:", err)
	}
}
