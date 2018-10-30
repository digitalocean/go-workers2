package workers

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/go-redis/redis"
)

const (
	NanoSecondPrecision = 1000000000.0
)

type Producer struct {
	opts Options
}

func NewProducer(options Options) (*Producer, error) {
	options, err := processOptions(options)
	if err != nil {
		return nil, err
	}
	return &Producer{
		opts: options,
	}, nil
}

type EnqueueData struct {
	Queue      string      `json:"queue,omitempty"`
	Class      string      `json:"class"`
	Args       interface{} `json:"args"`
	Jid        string      `json:"jid"`
	EnqueuedAt float64     `json:"enqueued_at"`
	EnqueueOptions
}

type EnqueueOptions struct {
	RetryCount int     `json:"retry_count,omitempty"`
	Retry      bool    `json:"retry,omitempty"`
	At         float64 `json:"at,omitempty"`
}

func generateJid() string {
	// Return 12 random bytes as 24 character hex
	b := make([]byte, 12)
	_, err := io.ReadFull(rand.Reader, b)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%x", b)
}

func (p *Producer) Enqueue(queue, class string, args interface{}) (string, error) {
	return p.EnqueueWithOptions(queue, class, args, EnqueueOptions{At: nowToSecondsWithNanoPrecision()})
}

func (p *Producer) EnqueueIn(queue, class string, in float64, args interface{}) (string, error) {
	return p.EnqueueWithOptions(queue, class, args, EnqueueOptions{At: nowToSecondsWithNanoPrecision() + in})
}

func (p *Producer) EnqueueAt(queue, class string, at time.Time, args interface{}) (string, error) {
	return p.EnqueueWithOptions(queue, class, args, EnqueueOptions{At: timeToSecondsWithNanoPrecision(at)})
}

func (p *Producer) EnqueueWithOptions(queue, class string, args interface{}, opts EnqueueOptions) (string, error) {
	now := nowToSecondsWithNanoPrecision()
	data := EnqueueData{
		Queue:          queue,
		Class:          class,
		Args:           args,
		Jid:            generateJid(),
		EnqueuedAt:     now,
		EnqueueOptions: opts,
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		return "", err
	}

	if now < opts.At {
		err := p.enqueueAt(data.At, bytes)
		return data.Jid, err
	}

	rc := p.opts.client

	_, err = rc.SAdd(p.opts.Namespace+"queues", queue).Result()
	if err != nil {
		return "", err
	}
	queue = p.opts.Namespace + "queue:" + queue
	_, err = rc.LPush(queue, bytes).Result()
	if err != nil {
		return "", err
	}

	return data.Jid, nil
}

func (p *Producer) enqueueAt(at float64, bytes []byte) error {
	_, err := p.opts.client.ZAdd(p.opts.Namespace+scheduledJobsKey, redis.Z{Score: at, Member: bytes}).Result()
	return err
}

func timeToSecondsWithNanoPrecision(t time.Time) float64 {
	return float64(t.UnixNano()) / NanoSecondPrecision
}

func durationToSecondsWithNanoPrecision(d time.Duration) float64 {
	return float64(d.Nanoseconds()) / NanoSecondPrecision
}

func nowToSecondsWithNanoPrecision() float64 {
	return timeToSecondsWithNanoPrecision(time.Now())
}
