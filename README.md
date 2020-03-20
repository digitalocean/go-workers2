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
  "fmt"

  workers "github.com/digitalocean/go-workers2"
)

func myJob(message *workers.Msg) error {
  // do something with your message
  // message.Jid()
  // message.Args() is a wrapper around go-simplejson (http://godoc.org/github.com/bitly/go-simplejson)
  return nil
}

func myMiddleware(queue string, mgr *workers.Manager, next workers.JobFunc) workers.JobFunc {
  return func(message *workers.Msg) (err error) {
    // do something before each message is processed
    err = next(message)
    // do something after each message is processed
    return
  }
}

func main() {
  // Create a manager, which manages workers
  manager, err := workers.NewManager(workers.Options{
    // location of redis instance
    ServerAddr: "localhost:6379",
    // instance of the database
    Database:   0,
    // number of connections to keep open with redis
    PoolSize:   30,
    // unique process id for this instance of workers (for proper recovery of inprogress jobs on crash)
    ProcessID:  "1",
  })

  if err != nil {
    fmt.Println(err)
  }

  // create a middleware chain with the default middlewares, and append myMiddleware
  mids := workers.DefaultMiddlewares().Append(myMiddleware)

  // pull messages from "myqueue" with concurrency of 10
  // this worker will not run myMiddleware, but will run the default middlewares
  manager.AddWorker("myqueue", 10, myJob)

  // pull messages from "myqueue2" with concurrency of 20
  // this worker will run the default middlewares and myMiddleware
  manager.AddWorker("myqueue2", 20, myJob, mids...)

  // pull messages from "myqueue3" with concurrency of 20
  // this worker will only run myMiddleware
  manager.AddWorker("myqueue3", 20, myJob, myMiddleware)

  // If you already have a manager and want to enqueue
  // to the same place:
  producer := manager.Producer()

  // Alternatively, if you want to create a producer to enqueue messages
  // producer, err := workers.NewProducer(Options{
  //   // location of redis instance
  //   ServerAddr: "localhost:6379",
  //   // instance of the database
  //   Database:   0,
  //   // number of connections to keep open with redis
  //   PoolSize:   30,
  //   // unique process id for this instance of workers (for proper recovery of inprogress jobs on crash)
  //   ProcessID:  "1",
  // })

  // Add a job to a queue
  producer.Enqueue("myqueue3", "Add", []int{1, 2})

  // Add a job to a queue with retry
  producer.EnqueueWithOptions("myqueue3", "Add", []int{1, 2}, workers.EnqueueOptions{Retry: true})

  // stats will be available at http://localhost:8080/stats
  go workers.StartStatsServer(8080)

  // Blocks until process is told to exit via unix signal
  manager.Run()
}
```

When running the above code example, it will produce the following output at `localhost:8080/stats`:

```json
[
  {
    "manager_name": "",
    "processed": 5,
    "failed": 57,
    "jobs": {
      "myqueue": null,
      "myqueue2": null,
      "myqueue3": null
    },
    "enqueued": {
      "myqueue": 0,
      "myqueue2": 0,
      "myqueue3": 0
    },
    "retry_count": 4
  }
]
```

Development sponsored by DigitalOcean. Code forked from [github/jrallison/go-workers](https://github.com/jrallison/go-workers). Initial development sponsored by [Customer.io](http://customer.io).
