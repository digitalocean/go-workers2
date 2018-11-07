package workers

import (
	"fmt"
	"time"
)

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
	rc := mgr.opts.client

	today := time.Now().UTC().Format("2006-01-02")

	pipe := rc.Pipeline()
	pipe.Incr(mgr.opts.Namespace + "stat:" + metric)
	pipe.Incr(mgr.opts.Namespace + "stat:" + metric + ":" + today)

	if _, err := pipe.Exec(); err != nil {
		Logger.Println("couldn't save stats:", err)
	}
}
