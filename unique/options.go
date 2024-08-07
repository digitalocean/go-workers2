package unique

import "time"

type UntilOption string

var UntilStart = UntilOption("start")
var UntilSuccess = UntilOption("success")

type Options struct {
	UniqueFor   time.Duration `json:"unique_for,omitempty"`
	UniqueUntil UntilOption   `json:"unique_until,omitempty"`
}

func (options Options) IsUnique() bool {
	return options.UniqueFor > 0
}

func NewOptions() Options {
	return Options{
		UniqueFor:   time.Duration(-1),
		UniqueUntil: UntilSuccess,
	}
}
