[![Build Status](https://travis-ci.org/digitalocean/go-workers2.png)](https://travis-ci.org/digitalocean/go-workers2)
[![GoDoc](https://godoc.org/github.com/digitalocean/go-workers2?status.png)](https://godoc.org/github.com/digitalocean/go-workers2)

[Sidekiq](http://sidekiq.org/) compatible
background workers in [golang](http://golang.org/).

* reliable queueing for all queues using [brpoplpush](http://redis.io/commands/brpoplpush)
* handles retries
* support custom middleware
* customize concurrency per queue
* responds to Unix signals to safely wait for jobs to finish before exiting.
* provides stats on what jobs are currently running
* redis sentinel support
* well tested

Example usage:

```go
package main

import (
	"github.com/digitalocean/go-workers2"
)

func myJob(message *workers.Msg) error {
  // do something with your message
  // message.Jid()
  // message.Args() is a wrapper around go-simplejson (http://godoc.org/github.com/bitly/go-simplejson)
  return nil
}

type myMiddleware struct{}

func (r *myMiddleware) Call(queue string, message *workers.Msg, next func() error) (err error) {
  // do something before each message is processed
  err = next()
  // do something after each message is processed
  return
} 

func main() {
  workers.Configure(map[string]string{
    // location of redis instance
    "server":  "localhost:6379",
    // instance of the database
    "database":  "0",
    // number of connections to keep open with redis
    "pool":    "30",
    // unique process id for this instance of workers (for proper recovery of inprogress jobs on crash)
    "process": "1",
  })

  workers.Middleware.Append(&myMiddleware{})

  // pull messages from "myqueue" with concurrency of 10
  workers.Process("myqueue", myJob, 10)

  // pull messages from "myqueue2" with concurrency of 20
  workers.Process("myqueue2", myJob, 20)

  // Add a job to a queue
  workers.Enqueue("myqueue3", "Add", []int{1, 2})

  // Add a job to a queue with retry
  workers.EnqueueWithOptions("myqueue3", "Add", []int{1, 2}, workers.EnqueueOptions{Retry: true})

  // stats will be available at http://localhost:8080/stats
  go workers.StatsServer(8080)

  // Blocks until process is told to exit via unix signal
  workers.Run()
}
```

Development sponsored by DigitalOcean. Code forked from [github/jrallison/go-workers](https://github.com/jrallison/go-workers). Initial development sponsored by [Customer.io](http://customer.io).
