package workers

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// NanoSecondPrecision is a constant for the number of nanoseconds in a second
	NanoSecondPrecision = 1000000000.0
)

// Producer is used to enqueue new work
type Producer struct {
	opts Options
}

// EnqueueData stores data and configuration for new work
type EnqueueData struct {
	Queue      string      `json:"queue,omitempty"`
	Class      string      `json:"class"`
	Args       interface{} `json:"args"`
	Jid        string      `json:"jid"`
	EnqueuedAt float64     `json:"enqueued_at"`
	EnqueueOptions
}

// EnqueueOptions stores configuration for new work
type EnqueueOptions struct {
	RetryCount int     `json:"retry_count,omitempty"`
	RetryMax   int     `json:"retry_max,omitempty"`
	Retry      bool    `json:"retry,omitempty"`
	At         float64 `json:"at,omitempty"`
}

// NewProducer creates a new producer with the given options
func NewProducer(options Options) (*Producer, error) {
	options, err := processOptions(options)
	if err != nil {
		return nil, err
	}

	return &Producer{
		opts: options,
	}, nil
}

// NewProducerWithRedisClient creates a new producer with the given options and Redis client
func NewProducerWithRedisClient(options Options, client *redis.Client) (*Producer, error) {
	options, err := processOptionsWithRedisClient(options, client)
	if err != nil {
		return nil, err
	}

	return &Producer{
		opts: options,
	}, nil
}

// GetRedisClient returns the Redis client used by the producer
// Deprecated: the Redis client is an internal implementation and access will be removed
func (p *Producer) GetRedisClient() *redis.Client {
	return p.opts.client
}

// Enqueue enqueues new work for immediate processing
func (p *Producer) Enqueue(queue, class string, args interface{}) (string, error) {
	return p.EnqueueWithOptions(queue, class, args, EnqueueOptions{At: nowToSecondsWithNanoPrecision()})
}

// EnqueueIn enqueues new work for delayed processing
func (p *Producer) EnqueueIn(queue, class string, in float64, args interface{}) (string, error) {
	return p.EnqueueWithOptions(queue, class, args, EnqueueOptions{At: nowToSecondsWithNanoPrecision() + in})
}

// EnqueueAt enqueues new work for processing at a specific time
func (p *Producer) EnqueueAt(queue, class string, at time.Time, args interface{}) (string, error) {
	return p.EnqueueWithOptions(queue, class, args, EnqueueOptions{At: timeToSecondsWithNanoPrecision(at)})
}

// EnqueueWithOptions enqueues new work for processing with the given options
func (p *Producer) EnqueueWithOptions(queue, class string, args interface{}, opts EnqueueOptions) (string, error) {
	return p.EnqueueWithContext(context.Background(), queue, class, args, opts)
}

// EnqueueWithContext enqueues new work for processing with the given options and context
func (p *Producer) EnqueueWithContext(ctx context.Context, queue, class string, args interface{}, opts EnqueueOptions) (string, error) {
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
		err = p.opts.store.EnqueueScheduledMessage(ctx, data.At, string(bytes))
		return data.Jid, err
	}

	err = p.opts.store.CreateQueue(ctx, queue)
	if err != nil {
		return "", err
	}

	err = p.opts.store.EnqueueMessageNow(ctx, queue, string(bytes))
	if err != nil {
		return "", err
	}

	return data.Jid, nil
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

func generateJid() string {
	// Return 12 random bytes as 24 character hex
	b := make([]byte, 12)
	_, err := io.ReadFull(rand.Reader, b)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%x", b)
}
