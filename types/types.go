package types

import "sync"

// Message represents a message to be routed through exchanges and queues.
type Message struct {
	Body       string // The actual message content
	RoutingKey string // The routing key used for routing the message
}

// Queue represents a message queue where messages are stored.
type Queue struct {
	Name     string
	Messages chan Message // Channel to hold messages in the queue
}

// ExchangeType defines types of exchanges (e.g., Direct, Topic, Fanout).
type ExchangeType string

const (
	Direct ExchangeType = "direct"
	Topic  ExchangeType = "topic"
	Fanout ExchangeType = "fanout"
)

// Exchange routes messages to queues based on type and routing key.
type Exchange struct {
	Name        string
	Type        ExchangeType
	Bindings    map[string][]*Queue // Maps routing keys to queues
	BindingsMux sync.RWMutex        // RWMutex for safe concurrent access
}

func (q *Queue) Consume() <-chan Message {
	return q.Messages
}
