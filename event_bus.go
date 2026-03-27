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

// EventRouter provides content-based routing on top of the Bus. Instead of
// requiring publishers to know which topic to use, the router inspects the
// event payload and dispatches it to the correct topic based on configurable
// routing rules.
type EventRouter struct {
	bus   *Bus
	rules []RoutingRule
	mu    sync.RWMutex
	stats RouterStats
}

// RoutingRule maps a predicate over event fields to a destination topic.
type RoutingRule struct {
	Name        string
	SourceTopic string
	DestTopic   string
	Condition   func(Event) bool
	Transform   func(Event) Event
	Priority    int
	Enabled     bool
}

type RouterStats struct {
	Routed     int64
	Dropped    int64
	Errors     int64
	ByRule     map[string]int64
	LastRouted time.Time
}

func NewEventRouter(bus *Bus) *EventRouter {
	return &EventRouter{
		bus: bus,
		stats: RouterStats{
			ByRule: make(map[string]int64),
		},
	}
}

func (er *EventRouter) AddRule(rule RoutingRule) {
	er.mu.Lock()
	defer er.mu.Unlock()

	if !rule.Enabled {
		rule.Enabled = true
	}
	er.rules = append(er.rules, rule)

	// Keep rules sorted by priority descending so higher-priority rules
	// are evaluated first.
	sort.Slice(er.rules, func(i, j int) bool {
		return er.rules[i].Priority > er.rules[j].Priority
	})
}

func (er *EventRouter) RemoveRule(name string) bool {
	er.mu.Lock()
	defer er.mu.Unlock()

	for i, r := range er.rules {
		if r.Name == name {
			er.rules = append(er.rules[:i], er.rules[i+1:]...)
			return true
		}
	}
	return false
}

// Route evaluates all rules against the event and publishes to matching
// destination topics. If a rule includes a Transform function, the event is
// transformed before publishing to that rule's destination.
func (er *EventRouter) Route(evt Event) (int, error) {
	er.mu.RLock()
	rules := make([]RoutingRule, len(er.rules))
	copy(rules, er.rules)
	er.mu.RUnlock()

	matched := 0
	for _, rule := range rules {
		if !rule.Enabled {
			continue
		}
		if rule.SourceTopic != "" && rule.SourceTopic != evt.Topic {
			continue
		}
		if rule.Condition != nil && !rule.Condition(evt) {
			continue
		}

		target := evt
		if rule.Transform != nil {
			target = rule.Transform(evt)
		}
		target.Topic = rule.DestTopic

		if err := er.bus.Publish(rule.DestTopic, target.Payload,
			WithEventID(target.ID),
			WithSource(target.Source),
		); err != nil {
			er.mu.Lock()
			er.stats.Errors++
			er.mu.Unlock()
			return matched, fmt.Errorf("route %s: %w", rule.Name, err)
		}
		matched++

		er.mu.Lock()
		er.stats.Routed++
		er.stats.ByRule[rule.Name]++
		er.stats.LastRouted = time.Now()
		er.mu.Unlock()
	}
	return matched, nil
}

func (er *EventRouter) Stats() RouterStats {
	er.mu.RLock()
	defer er.mu.RUnlock()

	cp := RouterStats{
		Routed:     er.stats.Routed,
		Dropped:    er.stats.Dropped,
		Errors:     er.stats.Errors,
		LastRouted: er.stats.LastRouted,
		ByRule:     make(map[string]int64, len(er.stats.ByRule)),
	}
	for k, v := range er.stats.ByRule {
		cp.ByRule[k] = v
	}
	return cp
}

// EventReplay records events for a topic and replays them to late-joining
// subscribers. This is useful for subscribers that need to catch up on events
// published before they started.
type EventReplay struct {
	mu       sync.RWMutex
	buffer   map[string][]Event
	maxPerTopic int
}

func NewEventReplay(maxPerTopic int) *EventReplay {
	return &EventReplay{
		buffer:      make(map[string][]Event),
		maxPerTopic: maxPerTopic,
	}
}

// Record stores an event in the replay buffer for its topic.
func (er *EventReplay) Record(evt Event) {
	er.mu.Lock()
	defer er.mu.Unlock()

	buf := er.buffer[evt.Topic]
	if len(buf) >= er.maxPerTopic {
		// Evict oldest event to make room.
		copy(buf, buf[1:])
		buf = buf[:len(buf)-1]
	}
	er.buffer[evt.Topic] = append(buf, evt)
}

// Replay sends all buffered events for the given topic to the handler.
func (er *EventReplay) Replay(ctx context.Context, topic string, handler HandlerFunc) (int, error) {
	er.mu.RLock()
	events := make([]Event, len(er.buffer[topic]))
	copy(events, er.buffer[topic])
	er.mu.RUnlock()

	for _, evt := range events {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if err := handler(ctx, evt); err != nil {
			return 0, fmt.Errorf("replay event %s: %w", evt.ID, err)
		}
	}
	return len(events), nil
}

// Size returns the total number of events buffered across all topics.
func (er *EventReplay) Size() int {
	er.mu.RLock()
	defer er.mu.RUnlock()

	total := 0
	for _, buf := range er.buffer {
		total += len(buf)
	}
	return total
}

// Clear removes all buffered events for the specified topic, or all topics
// if an empty string is passed.
func (er *EventReplay) Clear(topic string) {
	er.mu.Lock()
	defer er.mu.Unlock()

	if topic == "" {
		er.buffer = make(map[string][]Event)
		return
	}
	delete(er.buffer, topic)
}

// EventAggregator collects events over a time window and emits a single
// aggregated event when the window closes. Useful for batching high-frequency
// events like metrics or audit logs before persisting.
type EventAggregator struct {
	mu        sync.Mutex
	windows   map[string]*aggregationWindow
	windowDur time.Duration
	flushFn   func(topic string, events []Event) error
	closed    chan struct{}
}

type aggregationWindow struct {
	events    []Event
	openedAt  time.Time
}

func NewEventAggregator(windowDur time.Duration, flushFn func(string, []Event) error) *EventAggregator {
	return &EventAggregator{
		windows:   make(map[string]*aggregationWindow),
		windowDur: windowDur,
		flushFn:   flushFn,
		closed:    make(chan struct{}),
	}
}

func (ea *EventAggregator) Add(evt Event) {
	ea.mu.Lock()
	defer ea.mu.Unlock()

	w, ok := ea.windows[evt.Topic]
	if !ok {
		w = &aggregationWindow{openedAt: time.Now()}
		ea.windows[evt.Topic] = w
	}
	w.events = append(w.events, evt)
}

// Flush checks all windows and emits aggregated events for any that have
// exceeded the configured duration.
func (ea *EventAggregator) Flush() error {
	ea.mu.Lock()
	var toFlush []struct {
		topic  string
		events []Event
	}
	now := time.Now()
	for topic, w := range ea.windows {
		if now.Sub(w.openedAt) >= ea.windowDur {
			toFlush = append(toFlush, struct {
				topic  string
				events []Event
			}{topic, w.events})
			delete(ea.windows, topic)
		}
	}
	ea.mu.Unlock()

	for _, batch := range toFlush {
		if err := ea.flushFn(batch.topic, batch.events); err != nil {
			return fmt.Errorf("flush topic %s: %w", batch.topic, err)
		}
	}
	return nil
}

// RunFlushLoop periodically flushes aggregation windows until the context
// is canceled.
func (ea *EventAggregator) RunFlushLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Final flush on shutdown.
			ea.Flush()
			return
		case <-ticker.C:
			if err := ea.Flush(); err != nil {
				log.Printf("event_aggregator: flush error: %v", err)
			}
		}
	}
}

// PendingCount returns the number of events waiting to be flushed.
func (ea *EventAggregator) PendingCount() int {
	ea.mu.Lock()
	defer ea.mu.Unlock()

	total := 0
	for _, w := range ea.windows {
		total += len(w.events)
	}
	return total
}

// EventFilter provides chainable filter predicates for events. Filters are
// composed using And/Or combinators and can be reused across subscribers.
type EventFilter struct {
	predicate func(Event) bool
}

func NewTopicFilter(topics ...string) *EventFilter {
	set := make(map[string]bool, len(topics))
	for _, t := range topics {
		set[t] = true
	}
	return &EventFilter{
		predicate: func(e Event) bool { return set[e.Topic] },
	}
}

func NewSourceFilter(sources ...string) *EventFilter {
	set := make(map[string]bool, len(sources))
	for _, s := range sources {
		set[s] = true
	}
	return &EventFilter{
		predicate: func(e Event) bool { return set[e.Source] },
	}
}

func NewMetadataFilter(key, value string) *EventFilter {
	return &EventFilter{
		predicate: func(e Event) bool { return e.Metadata[key] == value },
	}
}

func (f *EventFilter) And(other *EventFilter) *EventFilter {
	return &EventFilter{
		predicate: func(e Event) bool {
			return f.predicate(e) && other.predicate(e)
		},
	}
}

func (f *EventFilter) Or(other *EventFilter) *EventFilter {
	return &EventFilter{
		predicate: func(e Event) bool {
			return f.predicate(e) || other.predicate(e)
		},
	}
}

func (f *EventFilter) Not() *EventFilter {
	return &EventFilter{
		predicate: func(e Event) bool { return !f.predicate(e) },
	}
}

func (f *EventFilter) Matches(e Event) bool {
	return f.predicate(e)
}

func (f *EventFilter) AsFunc() func(Event) bool {
	return f.predicate
}

// EventInspector captures a snapshot of all bus internals for diagnostics.
type EventInspector struct {
	bus *Bus
}

func NewEventInspector(bus *Bus) *EventInspector {
	return &EventInspector{bus: bus}
}

func (ei *EventInspector) Snapshot() map[string]interface{} {
	topics := ei.bus.Topics()
	result := map[string]interface{}{
		"topics":         topics,
		"dropped_events": ei.bus.DroppedEvents(),
	}
	topicDetails := make(map[string]interface{})
	for _, t := range topics {
		if info, err := ei.bus.TopicInfo(t); err == nil {
			topicDetails[t] = map[string]interface{}{
				"published":    info.Published,
				"subscribers":  info.Subscribers,
				"last_event_at": info.LastEventAt.Format(time.RFC3339),
			}
		}
	}
	result["topic_details"] = topicDetails
	return result
}

func (ei *EventInspector) HealthCheck() error {
	if ei.bus.closed.Load() {
		return ErrBusClosed
	}
	dropped := ei.bus.DroppedEvents()
	if dropped > 10000 {
		return fmt.Errorf("excessive dropped events: %d", dropped)
	}
	return nil
}

// BatchPublisher accumulates events and publishes them in a single burst,
// reducing lock contention when many goroutines produce events concurrently.
type BatchPublisher struct {
	mu       sync.Mutex
	bus      *Bus
	pending  map[string][]json.RawMessage
	maxBatch int
	opts     []PublishOption
}

func NewBatchPublisher(bus *Bus, maxBatch int, opts ...PublishOption) *BatchPublisher {
	return &BatchPublisher{
		bus:      bus,
		pending:  make(map[string][]json.RawMessage),
		maxBatch: maxBatch,
		opts:     opts,
	}
}

func (bp *BatchPublisher) Enqueue(topic string, payload json.RawMessage) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	bp.pending[topic] = append(bp.pending[topic], payload)
}

func (bp *BatchPublisher) Flush() (int, error) {
	bp.mu.Lock()
	snapshot := bp.pending
	bp.pending = make(map[string][]json.RawMessage)
	bp.mu.Unlock()

	published := 0
	for topic, payloads := range snapshot {
		for i := 0; i < len(payloads); i += bp.maxBatch {
			end := i + bp.maxBatch
			if end > len(payloads) {
				end = len(payloads)
			}
			chunk := payloads[i:end]

			wrapped, err := json.Marshal(chunk)
			if err != nil {
				return published, fmt.Errorf("marshal batch for %s: %w", topic, err)
			}
			if err := bp.bus.Publish(topic, wrapped, bp.opts...); err != nil {
				return published, err
			}
			published += len(chunk)
		}
	}
	return published, nil
}

func (bp *BatchPublisher) PendingCount() int {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	total := 0
	for _, payloads := range bp.pending {
		total += len(payloads)
	}
	return total
}

// CircuitState tracks whether event delivery to a topic should be halted.
type CircuitState int

const (
	CircuitClosed   CircuitState = iota // Normal operation
	CircuitOpen                         // Errors exceeded threshold, blocking
	CircuitHalfOpen                     // Testing if recovery is possible
)

func (cs CircuitState) String() string {
	switch cs {
	case CircuitClosed:
		return "closed"
	case CircuitOpen:
		return "open"
	case CircuitHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// TopicCircuitBreaker wraps a Bus and halts publishing to a topic when the
// error rate exceeds a threshold, allowing the downstream to recover.
type TopicCircuitBreaker struct {
	mu             sync.Mutex
	bus            *Bus
	states         map[string]CircuitState
	errorCounts    map[string]int
	successCounts  map[string]int
	lastStateChange map[string]time.Time
	threshold      int
	resetAfter     time.Duration
	halfOpenMax    int
}

func NewTopicCircuitBreaker(bus *Bus, threshold int, resetAfter time.Duration) *TopicCircuitBreaker {
	return &TopicCircuitBreaker{
		bus:             bus,
		states:          make(map[string]CircuitState),
		errorCounts:     make(map[string]int),
		successCounts:   make(map[string]int),
		lastStateChange: make(map[string]time.Time),
		threshold:       threshold,
		resetAfter:      resetAfter,
		halfOpenMax:     5,
	}
}

func (cb *TopicCircuitBreaker) Publish(topic string, payload json.RawMessage, opts ...PublishOption) error {
	cb.mu.Lock()
	state := cb.states[topic]

	switch state {
	case CircuitOpen:
		if time.Since(cb.lastStateChange[topic]) > cb.resetAfter {
			cb.states[topic] = CircuitHalfOpen
			cb.successCounts[topic] = 0
			cb.lastStateChange[topic] = time.Now()
			state = CircuitHalfOpen
		} else {
			cb.mu.Unlock()
			return fmt.Errorf("circuit open for topic %s", topic)
		}
	}
	cb.mu.Unlock()

	err := cb.bus.Publish(topic, payload, opts...)

	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err != nil {
		cb.errorCounts[topic]++
		if cb.errorCounts[topic] >= cb.threshold {
			cb.states[topic] = CircuitOpen
			cb.lastStateChange[topic] = time.Now()
			cb.errorCounts[topic] = 0
		}
		return err
	}

	if state == CircuitHalfOpen {
		cb.successCounts[topic]++
		if cb.successCounts[topic] >= cb.halfOpenMax {
			cb.states[topic] = CircuitClosed
			cb.lastStateChange[topic] = time.Now()
			cb.errorCounts[topic] = 0
		}
	} else {
		// Successful publish resets error count in closed state.
		cb.errorCounts[topic] = 0
	}
	return nil
}

func (cb *TopicCircuitBreaker) State(topic string) CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.states[topic]
}

func (cb *TopicCircuitBreaker) Reset(topic string) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.states[topic] = CircuitClosed
	cb.errorCounts[topic] = 0
	cb.successCounts[topic] = 0
	cb.lastStateChange[topic] = time.Now()
}

// EventSerializer handles encoding and decoding of events to and from
// persistent storage or network transport.
type EventSerializer struct {
	compressionEnabled bool
}

func NewEventSerializer(compress bool) *EventSerializer {
	return &EventSerializer{compressionEnabled: compress}
}

func (es *EventSerializer) Marshal(evt Event) ([]byte, error) {
	wrapper := struct {
		ID        string            `json:"id"`
		Topic     string            `json:"topic"`
		Payload   json.RawMessage   `json:"payload"`
		Source    string            `json:"source,omitempty"`
		Timestamp string            `json:"timestamp"`
		Metadata  map[string]string `json:"metadata,omitempty"`
		Sequence  uint64            `json:"sequence"`
	}{
		ID:        evt.ID,
		Topic:     evt.Topic,
		Payload:   evt.Payload,
		Source:    evt.Source,
		Timestamp: evt.Timestamp.Format(time.RFC3339Nano),
		Metadata:  evt.Metadata,
		Sequence:  evt.sequence,
	}
	return json.Marshal(wrapper)
}

func (es *EventSerializer) Unmarshal(data []byte) (Event, error) {
	var wrapper struct {
		ID        string            `json:"id"`
		Topic     string            `json:"topic"`
		Payload   json.RawMessage   `json:"payload"`
		Source    string            `json:"source,omitempty"`
		Timestamp string            `json:"timestamp"`
		Metadata  map[string]string `json:"metadata,omitempty"`
		Sequence  uint64            `json:"sequence"`
	}
	if err := json.Unmarshal(data, &wrapper); err != nil {
		return Event{}, fmt.Errorf("unmarshal event: %w", err)
	}

	ts, err := time.Parse(time.RFC3339Nano, wrapper.Timestamp)
	if err != nil {
		return Event{}, fmt.Errorf("parse timestamp: %w", err)
	}

	return Event{
		ID:        wrapper.ID,
		Topic:     wrapper.Topic,
		Payload:   wrapper.Payload,
		Source:    wrapper.Source,
		Timestamp: ts,
		Metadata:  wrapper.Metadata,
		sequence:  wrapper.Sequence,
	}, nil
}

func (es *EventSerializer) MarshalBatch(events []Event) ([]byte, error) {
	encoded := make([]json.RawMessage, 0, len(events))
	for _, evt := range events {
		data, err := es.Marshal(evt)
		if err != nil {
			return nil, err
		}
		encoded = append(encoded, data)
	}
	return json.Marshal(encoded)
}

func (es *EventSerializer) UnmarshalBatch(data []byte) ([]Event, error) {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("unmarshal batch: %w", err)
	}

	events := make([]Event, 0, len(raw))
	for _, r := range raw {
		evt, err := es.Unmarshal(r)
		if err != nil {
			return nil, err
		}
		events = append(events, evt)
	}
	return events, nil
}

// RetentionPolicy defines how long events should be kept in a replay buffer
// based on age, count, or total size constraints.
type RetentionPolicy struct {
	MaxAge    time.Duration
	MaxCount  int
	MaxBytes  int64
}

// RetentionEnforcer periodically trims replay buffers according to the policy.
type RetentionEnforcer struct {
	mu       sync.Mutex
	replay   *EventReplay
	policy   RetentionPolicy
	serializer *EventSerializer
}

func NewRetentionEnforcer(replay *EventReplay, policy RetentionPolicy) *RetentionEnforcer {
	return &RetentionEnforcer{
		replay:     replay,
		policy:     policy,
		serializer: NewEventSerializer(false),
	}
}

func (re *RetentionEnforcer) Enforce() (int, error) {
	re.mu.Lock()
	defer re.mu.Unlock()

	re.replay.mu.Lock()
	defer re.replay.mu.Unlock()

	totalRemoved := 0
	now := time.Now()

	for topic, events := range re.replay.buffer {
		originalLen := len(events)

		// Trim by age first.
		if re.policy.MaxAge > 0 {
			cutoff := now.Add(-re.policy.MaxAge)
			trimIdx := 0
			for trimIdx < len(events) && events[trimIdx].Timestamp.Before(cutoff) {
				trimIdx++
			}
			if trimIdx > 0 {
				events = events[trimIdx:]
			}
		}

		// Trim by count.
		if re.policy.MaxCount > 0 && len(events) > re.policy.MaxCount {
			excess := len(events) - re.policy.MaxCount
			events = events[excess:]
		}

		// Trim by total serialized size.
		if re.policy.MaxBytes > 0 {
			var totalSize int64
			keepFrom := 0
			for i := len(events) - 1; i >= 0; i-- {
				data, err := re.serializer.Marshal(events[i])
				if err != nil {
					continue
				}
				totalSize += int64(len(data))
				if totalSize > re.policy.MaxBytes {
					keepFrom = i + 1
					break
				}
			}
			if keepFrom > 0 {
				events = events[keepFrom:]
			}
		}

		re.replay.buffer[topic] = events
		totalRemoved += originalLen - len(events)
	}
	return totalRemoved, nil
}

func (re *RetentionEnforcer) RunLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			removed, err := re.Enforce()
			if err != nil {
				log.Printf("retention_enforcer: error: %v", err)
			} else if removed > 0 {
				log.Printf("retention_enforcer: trimmed %d events", removed)
			}
		}
	}
}

// MetricsCollector gathers operational metrics from the bus and exposes them
// in a format suitable for Prometheus or similar monitoring systems.
type MetricsCollector struct {
	bus         *Bus
	router      *EventRouter
	aggregator  *EventAggregator
	interval    time.Duration
	snapshots   []MetricsSnapshot
	mu          sync.RWMutex
	maxHistory  int
}

type MetricsSnapshot struct {
	Timestamp       time.Time
	ActiveTopics    int
	TotalPublished  int64
	TotalDropped    int64
	PendingEvents   int
	RouterRouted    int64
	RouterErrors    int64
}

func NewMetricsCollector(bus *Bus, router *EventRouter, aggregator *EventAggregator, maxHistory int) *MetricsCollector {
	return &MetricsCollector{
		bus:        bus,
		router:     router,
		aggregator: aggregator,
		maxHistory: maxHistory,
	}
}

func (mc *MetricsCollector) Collect() MetricsSnapshot {
	snap := MetricsSnapshot{
		Timestamp:    time.Now(),
		ActiveTopics: len(mc.bus.Topics()),
		TotalDropped: mc.bus.DroppedEvents(),
	}

	for _, topic := range mc.bus.Topics() {
		if info, err := mc.bus.TopicInfo(topic); err == nil {
			snap.TotalPublished += info.Published
		}
	}

	if mc.router != nil {
		rs := mc.router.Stats()
		snap.RouterRouted = rs.Routed
		snap.RouterErrors = rs.Errors
	}

	if mc.aggregator != nil {
		snap.PendingEvents = mc.aggregator.PendingCount()
	}

	mc.mu.Lock()
	mc.snapshots = append(mc.snapshots, snap)
	if len(mc.snapshots) > mc.maxHistory {
		mc.snapshots = mc.snapshots[len(mc.snapshots)-mc.maxHistory:]
	}
	mc.mu.Unlock()

	return snap
}

func (mc *MetricsCollector) History() []MetricsSnapshot {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	out := make([]MetricsSnapshot, len(mc.snapshots))
	copy(out, mc.snapshots)
	return out
}

func (mc *MetricsCollector) RunLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			mc.Collect()
		}
	}
}
