package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrBusClosed       = errors.New("event bus is closed")
	ErrTopicNotFound   = errors.New("topic does not exist")
	ErrHandlerTimeout  = errors.New("handler exceeded deadline")
	ErrDuplicateID     = errors.New("subscriber ID already registered")
	ErrBackpressure    = errors.New("subscriber channel is full, dropping event")
)

// Event is the unit of data published through the bus.
type Event struct {
	ID        string
	Topic     string
	Payload   json.RawMessage
	Source    string
	Timestamp time.Time
	Metadata  map[string]string
	sequence  uint64
}

// Priority determines the order in which subscribers receive events when
// multiple subscribers exist on the same topic.
type Priority int

const (
	PriorityLow    Priority = 0
	PriorityNormal Priority = 50
	PriorityHigh   Priority = 100
)

// HandlerFunc processes a single event. Returning an error causes the bus to
// invoke the subscriber's configured error strategy.
type HandlerFunc func(ctx context.Context, evt Event) error

// ErrorStrategy controls what happens when a handler returns an error.
type ErrorStrategy int

const (
	ErrorDrop ErrorStrategy = iota
	ErrorRetry
	ErrorDeadLetter
)

// SubscriberConfig holds per-subscriber settings.
type SubscriberConfig struct {
	ID            string
	Topics        []string
	Handler       HandlerFunc
	Priority      Priority
	BufferSize    int
	MaxRetries    int
	RetryDelay    time.Duration
	Timeout       time.Duration
	ErrorStrategy ErrorStrategy
	Filter        func(Event) bool
}

func (sc *SubscriberConfig) defaults() {
	if sc.BufferSize <= 0 {
		sc.BufferSize = 256
	}
	if sc.MaxRetries <= 0 {
		sc.MaxRetries = 3
	}
	if sc.RetryDelay <= 0 {
		sc.RetryDelay = 500 * time.Millisecond
	}
	if sc.Timeout <= 0 {
		sc.Timeout = 10 * time.Second
	}
}

type subscriber struct {
	config  SubscriberConfig
	ch      chan Event
	done    chan struct{}
	dropped atomic.Int64
}

// TopicStats exposes per-topic metrics.
type TopicStats struct {
	Published   int64
	Subscribers int
	LastEventAt time.Time
}

// Bus is a lightweight in-process publish/subscribe message bus.
type Bus struct {
	mu          sync.RWMutex
	subscribers map[string][]*subscriber // topic -> subscribers
	allSubs     map[string]*subscriber   // subscriber ID -> subscriber
	stats       map[string]*TopicStats
	seq         atomic.Uint64
	closed      atomic.Bool
	deadLetter  chan Event
	logger      *log.Logger
}

// BusOption configures the Bus.
type BusOption func(*Bus)

func WithLogger(l *log.Logger) BusOption {
	return func(b *Bus) { b.logger = l }
}

func WithDeadLetterSize(size int) BusOption {
	return func(b *Bus) { b.deadLetter = make(chan Event, size) }
}

// NewBus creates a Bus ready for use.
func NewBus(opts ...BusOption) *Bus {
	b := &Bus{
		subscribers: make(map[string][]*subscriber),
		allSubs:     make(map[string]*subscriber),
		stats:       make(map[string]*TopicStats),
		deadLetter:  make(chan Event, 1024),
		logger:      log.Default(),
	}
	for _, o := range opts {
		o(b)
	}
	return b
}

// Subscribe registers a new subscriber. The subscriber's goroutine starts
// immediately and runs until Unsubscribe is called or the bus is closed.
func (b *Bus) Subscribe(cfg SubscriberConfig) error {
	if b.closed.Load() {
		return ErrBusClosed
	}

	cfg.defaults()

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.allSubs[cfg.ID]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicateID, cfg.ID)
	}

	sub := &subscriber{
		config: cfg,
		ch:     make(chan Event, cfg.BufferSize),
		done:   make(chan struct{}),
	}

	for _, topic := range cfg.Topics {
		b.subscribers[topic] = append(b.subscribers[topic], sub)
		// Sort by priority so high-priority subscribers are notified first.
		sort.Slice(b.subscribers[topic], func(i, j int) bool {
			return b.subscribers[topic][i].config.Priority > b.subscribers[topic][j].config.Priority
		})
		if _, ok := b.stats[topic]; !ok {
			b.stats[topic] = &TopicStats{}
		}
		b.stats[topic].Subscribers++
	}
	b.allSubs[cfg.ID] = sub

	go b.runSubscriber(sub)
	return nil
}

func (b *Bus) runSubscriber(sub *subscriber) {
	defer close(sub.done)

	for evt := range sub.ch {
		if sub.config.Filter != nil && !sub.config.Filter(evt) {
			continue
		}
		b.dispatch(sub, evt)
	}
}

func (b *Bus) dispatch(sub *subscriber, evt Event) {
	var lastErr error
	attempts := sub.config.MaxRetries
	if sub.config.ErrorStrategy != ErrorRetry {
		attempts = 1
	}

	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			time.Sleep(sub.config.RetryDelay * time.Duration(attempt))
		}

		ctx, cancel := context.WithTimeout(context.Background(), sub.config.Timeout)
		err := sub.config.Handler(ctx, evt)
		cancel()

		if err == nil {
			return
		}
		lastErr = err
		b.logger.Printf("event_bus: subscriber %s failed (attempt %d/%d): %v",
			sub.config.ID, attempt+1, attempts, err)
	}

	if lastErr != nil && sub.config.ErrorStrategy == ErrorDeadLetter {
		select {
		case b.deadLetter <- evt:
		default:
			b.logger.Printf("event_bus: dead letter queue full, dropping event %s", evt.ID)
		}
	}
}

// Publish sends an event to all subscribers of the given topic.
func (b *Bus) Publish(topic string, payload json.RawMessage, opts ...PublishOption) error {
	if b.closed.Load() {
		return ErrBusClosed
	}

	evt := Event{
		Topic:     topic,
		Payload:   payload,
		Timestamp: time.Now(),
		sequence:  b.seq.Add(1),
		Metadata:  make(map[string]string),
	}
	for _, o := range opts {
		o(&evt)
	}
	if evt.ID == "" {
		evt.ID = fmt.Sprintf("%s-%d", topic, evt.sequence)
	}

	b.mu.RLock()
	subs := b.subscribers[topic]
	stat := b.stats[topic]
	b.mu.RUnlock()

	if stat != nil {
		b.mu.Lock()
		stat.Published++
		stat.LastEventAt = evt.Timestamp
		b.mu.Unlock()
	}

	for _, sub := range subs {
		select {
		case sub.ch <- evt:
		default:
			sub.dropped.Add(1)
			b.logger.Printf("event_bus: backpressure on subscriber %s for topic %s",
				sub.config.ID, topic)
		}
	}
	return nil
}

// PublishOption customizes a single published event.
type PublishOption func(*Event)

func WithEventID(id string) PublishOption {
	return func(e *Event) { e.ID = id }
}

func WithSource(src string) PublishOption {
	return func(e *Event) { e.Source = src }
}

func WithMetadata(key, value string) PublishOption {
	return func(e *Event) { e.Metadata[key] = value }
}

// Unsubscribe removes a subscriber by ID.
func (b *Bus) Unsubscribe(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	sub, ok := b.allSubs[id]
	if !ok {
		return fmt.Errorf("subscriber %q not found", id)
	}

	close(sub.ch)
	delete(b.allSubs, id)

	for _, topic := range sub.config.Topics {
		subs := b.subscribers[topic]
		for i, s := range subs {
			if s == sub {
				b.subscribers[topic] = append(subs[:i], subs[i+1:]...)
				break
			}
		}
		if stat, ok := b.stats[topic]; ok {
			stat.Subscribers--
		}
	}
	return nil
}

// Topics returns a sorted list of all topics that have at least one subscriber.
func (b *Bus) Topics() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]string, 0, len(b.subscribers))
	for topic, subs := range b.subscribers {
		if len(subs) > 0 {
			out = append(out, topic)
		}
	}
	sort.Strings(out)
	return out
}

// TopicInfo returns stats for a specific topic.
func (b *Bus) TopicInfo(topic string) (TopicStats, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	stat, ok := b.stats[topic]
	if !ok {
		return TopicStats{}, fmt.Errorf("%w: %s", ErrTopicNotFound, topic)
	}
	return *stat, nil
}

// DroppedEvents returns the total number of events dropped due to backpressure
// across all subscribers.
func (b *Bus) DroppedEvents() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var total int64
	for _, sub := range b.allSubs {
		total += sub.dropped.Load()
	}
	return total
}

// DeadLetterChan returns the channel where failed events are sent when a
// subscriber uses ErrorDeadLetter strategy.
func (b *Bus) DeadLetterChan() <-chan Event {
	return b.deadLetter
}

// Close shuts down the bus, closing all subscriber channels and waiting for
// in-flight handlers to complete.
func (b *Bus) Close() error {
	if b.closed.Swap(true) {
		return ErrBusClosed
	}

	b.mu.Lock()
	subs := make([]*subscriber, 0, len(b.allSubs))
	for _, sub := range b.allSubs {
		subs = append(subs, sub)
	}
	b.mu.Unlock()

	for _, sub := range subs {
		close(sub.ch)
	}

	// Wait for all subscriber goroutines to finish.
	for _, sub := range subs {
		<-sub.done
	}
	close(b.deadLetter)
	return nil
}

// PublishJSON is a convenience wrapper that marshals v to JSON before publishing.
func (b *Bus) PublishJSON(topic string, v interface{}, opts ...PublishOption) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	return b.Publish(topic, data, opts...)
}

// WaitForDrain blocks until all subscriber channels are empty or the context
// is canceled. Useful in graceful shutdown sequences.
func (b *Bus) WaitForDrain(ctx context.Context) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			b.mu.RLock()
			allEmpty := true
			for _, sub := range b.allSubs {
				if len(sub.ch) > 0 {
					allEmpty = false
					break
				}
			}
			b.mu.RUnlock()
			if allEmpty {
				return nil
			}
		}
	}
}

// SubscriberInfo returns runtime information about a subscriber.
type SubscriberInfo struct {
	ID       string
	Topics   []string
	Pending  int
	Dropped  int64
	Priority Priority
}

func (b *Bus) SubscriberInfo(id string) (SubscriberInfo, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	sub, ok := b.allSubs[id]
	if !ok {
		return SubscriberInfo{}, fmt.Errorf("subscriber %q not found", id)
	}
	return SubscriberInfo{
		ID:       sub.config.ID,
		Topics:   sub.config.Topics,
		Pending:  len(sub.ch),
		Dropped:  sub.dropped.Load(),
		Priority: sub.config.Priority,
	}, nil
}

// TopicMatcher supports wildcard topic subscriptions. A pattern like
// "orders.*" matches "orders.created" and "orders.shipped".
func TopicMatcher(pattern, topic string) bool {
	if pattern == topic {
		return true
	}
	if !strings.Contains(pattern, "*") {
		return false
	}
	parts := strings.Split(pattern, "*")
	if len(parts) != 2 {
		return false
	}
	return strings.HasPrefix(topic, parts[0]) && strings.HasSuffix(topic, parts[1])
}

// PublishToMatching publishes to all topics matching a pattern. This is
// useful for fanout scenarios where you want to notify all order-related
// subscribers at once.
func (b *Bus) PublishToMatching(pattern string, payload json.RawMessage, opts ...PublishOption) (int, error) {
	if b.closed.Load() {
		return 0, ErrBusClosed
	}

	b.mu.RLock()
	topics := make([]string, 0)
	for topic := range b.subscribers {
		if TopicMatcher(pattern, topic) {
			topics = append(topics, topic)
		}
	}
	b.mu.RUnlock()

	published := 0
	for _, topic := range topics {
		if err := b.Publish(topic, payload, opts...); err != nil {
			return published, err
		}
		published++
	}
	return published, nil
}
