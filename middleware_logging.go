package workers

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"time"
)

var Logger WorkersLogger = log.New(os.Stdout, "workers: ", log.Ldate|log.Lmicroseconds)

func LogMiddleware(queue string, mgr *Manager, next JobFunc) JobFunc {
	return func(message *Msg) (err error) {
		prefix := fmt.Sprint(queue, " JID-", message.Jid())

		start := time.Now()
		Logger.Println(prefix, "start")
		Logger.Println(prefix, "args:", message.Args().ToJson())

		defer func() {
			if e := recover(); e != nil {
				var ok bool
				if err, ok = e.(error); !ok {
					err = fmt.Errorf("%v", e)
				}

				if err != nil {
					logProcessError(prefix, start, err)
				}
			}

		}()

		err = next(message)
		if err != nil {
			logProcessError(prefix, start, err)
		} else {
			Logger.Println(prefix, "done:", time.Since(start))
		}

		return
	}

}

func logProcessError(prefix string, start time.Time, err error) {
	Logger.Println(prefix, "fail:", time.Since(start))

	buf := make([]byte, 4096)
	buf = buf[:runtime.Stack(buf, false)]
	Logger.Printf("%s error: %v\n%s", prefix, err, buf)
}
