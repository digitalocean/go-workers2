package workers

import (
	"fmt"
	"time"
)

func StatsMiddleware(queue string, next JobFunc) JobFunc {
	return func(message *Msg) (err error) {
		defer func() {
			if e := recover(); e != nil {
				var ok bool
				if err, ok = e.(error); !ok {
					err = fmt.Errorf("%v", e)
				}

				if err != nil {
					incrementStats("failed")
				}
			}

		}()

		err = next(message)
		if err != nil {
			incrementStats("failed")
		} else {
			incrementStats("processed")
		}

		return
	}
}

func incrementStats(metric string) {
	rc := Config.Client

	today := time.Now().UTC().Format("2006-01-02")

	pipe := rc.Pipeline()
	pipe.Incr(Config.Namespace + "stat:" + metric)
	pipe.Incr(Config.Namespace + "stat:" + metric + ":" + today)

	if _, err := pipe.Exec(); err != nil {
		Logger.Println("couldn't save stats:", err)
	}
}
