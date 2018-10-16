package workers

type JobFunc func(message *Msg) error
