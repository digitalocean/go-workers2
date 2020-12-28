package workers

import (
	"fmt"
	"log"
	"runtime"
	"time"
)

// LogMiddleware is the default logging middleware
func LogMiddleware(queue string, mgr *Manager, next JobFunc) JobFunc {
	return func(message *Msg) (err error) {
		prefix := fmt.Sprint(queue, " JID-", message.Jid())

		start := time.Now()
		mgr.logger.Println(prefix, "start")
		mgr.logger.Println(prefix, "args:", message.Args().ToJson())

		defer func() {
			if e := recover(); e != nil {
				var ok bool
				if err, ok = e.(error); !ok {
					err = fmt.Errorf("%v", e)
				}

				if err != nil {
					logProcessError(mgr.logger, prefix, start, err)
				}
			}

		}()

		err = next(message)
		if err != nil {
			logProcessError(mgr.logger, prefix, start, err)
		} else {
			mgr.logger.Println(prefix, "done:", time.Since(start))
		}

		return
	}

}

func logProcessError(logger *log.Logger, prefix string, start time.Time, err error) {
	logger.Println(prefix, "fail:", time.Since(start))

	buf := make([]byte, 4096)
	buf = buf[:runtime.Stack(buf, false)]
	logger.Printf("%s error: %v\n%s", prefix, err, buf)
}
