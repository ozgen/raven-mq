package types

import "sync"

// Message represents the message structure to be routed through the server.
type Message struct {
	Body       string // The actual message content
	RoutingKey string // Key used to route the message to the right queue
}

// Queue holds messages and supports basic queue operations.
type Queue struct {
	Name     string
	Messages chan Message // Channel to hold messages in the queue
}

// ExchangeType defines the types of exchanges (similar to RabbitMQ).
type ExchangeType int

const (
	Direct ExchangeType = iota
	Topic
	Fanout
)

// Exchange routes messages to queues based on exchange type and routing key.
type Exchange struct {
	Name        string
	Type        ExchangeType
	Bindings    map[string][]*Queue // Maps routing keys to queues
	BindingsMux sync.Mutex          // Mutex to safely update bindings
}

func (q *Queue) Consume() <-chan Message {
	return q.Messages
}
