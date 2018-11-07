package workers

func setupTestOptions() (Options, error) {
	return setupTestOptionsWithNamespace("")
}

func setupTestOptionsWithNamespace(namespace string) (Options, error) {
	opts, err := processOptions(testOptionsWithNamespace(namespace))
	if opts.client != nil {
		opts.client.FlushDB().Result()
	}
	return opts, err
}

func testOptionsWithNamespace(namespace string) Options {
	return Options{
		ServerAddr: "localhost:6379",
		ProcessID:  "1",
		Database:   15,
		PoolSize:   1,
		Namespace:  namespace,
	}
}

type callCounter struct {
	count     int
	syncCh    chan *Msg
	ackSyncCh chan bool
}

func newCallCounter() *callCounter {
	return &callCounter{
		syncCh:    make(chan *Msg),
		ackSyncCh: make(chan bool),
	}
}

func (j *callCounter) getOpt(m *Msg, opt string) bool {
	if m == nil {
		return false
	}
	return m.Args().GetIndex(0).Get(opt).MustBool()
}

func (j *callCounter) F(m *Msg) error {
	j.count++
	if m != nil {
		if j.getOpt(m, "sync") {
			j.syncCh <- m
			<-j.ackSyncCh
		}
		m.ack = !j.getOpt(m, "noack")
	}
	return nil
}

func (j *callCounter) syncMsg() *Msg {
	m, _ := NewMsg(`{"args": [{"sync": true}]}`)
	return m
}

func (j *callCounter) msg() *Msg {
	m, _ := NewMsg(`{"args": []}`)
	return m
}

func (j *callCounter) noAckMsg() *Msg {
	m, _ := NewMsg(`{"args": [{"noack": true}]}`)
	return m
}
