package broker

import (
	"fmt"
	"github.com/ozgen/raven-mq/internal/types"
	"sync"
)

// Broker manages exchanges, queues, and routing of messages.
type Broker struct {
	Queues    map[string]*types.Queue    // Collection of queues
	Exchanges map[string]*types.Exchange // Collection of exchanges
	mux       sync.RWMutex               // RWMutex to safely read/write broker maps
}

// NewBroker initializes a new Broker with empty queues and exchanges.
func NewBroker() *Broker {
	return &Broker{
		Queues:    make(map[string]*types.Queue),
		Exchanges: make(map[string]*types.Exchange),
	}
}

// CreateQueue initializes a new queue and adds it to the broker.
// Returns an error if a queue with the same name already exists.
func (b *Broker) CreateQueue(name string) (*types.Queue, error) {
	b.mux.Lock()
	defer b.mux.Unlock()

	if _, exists := b.Queues[name]; exists {
		return nil, fmt.Errorf("queue %s already exists", name)
	}

	queue := &types.Queue{
		Name:     name,
		Messages: make(chan types.Message, 100), // Adjustable buffer size
	}
	b.Queues[name] = queue
	return queue, nil
}

// CreateExchange initializes an exchange based on type (direct, topic, fanout).
// Returns an error if an exchange with the same name already exists.
func (b *Broker) CreateExchange(name string, exType types.ExchangeType) (*types.Exchange, error) {
	b.mux.Lock()
	defer b.mux.Unlock()

	if _, exists := b.Exchanges[name]; exists {
		return nil, fmt.Errorf("exchange %s already exists", name)
	}

	exchange := &types.Exchange{
		Name:     name,
		Type:     exType,
		Bindings: make(map[string][]*types.Queue),
	}
	b.Exchanges[name] = exchange
	return exchange, nil
}

// BindQueue binds a queue to an exchange with a specific routing key.
// Returns an error if the exchange or queue does not exist.
func (b *Broker) BindQueue(exchangeName, routingKey string, queueName string) error {
	b.mux.RLock()
	exchange, exchangeExists := b.Exchanges[exchangeName]
	queue, queueExists := b.Queues[queueName]
	b.mux.RUnlock()

	if !exchangeExists {
		return fmt.Errorf("exchange %s does not exist", exchangeName)
	}
	if !queueExists {
		return fmt.Errorf("queue %s does not exist", queueName)
	}

	exchange.BindingsMux.Lock()
	defer exchange.BindingsMux.Unlock()

	exchange.Bindings[routingKey] = append(exchange.Bindings[routingKey], queue)
	return nil
}

// PublishMessage publishes a message to an exchange and routes it to bound queues.
// Returns an error if the exchange does not exist.
func (b *Broker) PublishMessage(exchangeName, routingKey string, message types.Message) error {
	b.mux.RLock()
	exchange, exists := b.Exchanges[exchangeName]
	b.mux.RUnlock()
	if !exists {
		return fmt.Errorf("exchange %s does not exist", exchangeName)
	}

	// Use Lock and Unlock here, as we may be appending to the Bindings map
	exchange.BindingsMux.Lock()
	defer exchange.BindingsMux.Unlock()

	// Route the message based on the exchange type
	switch exchange.Type {
	case types.Fanout:
		// Fanout exchange: Send to all bound queues
		for _, queues := range exchange.Bindings {
			for _, queue := range queues {
				b.sendMessage(queue, message)
			}
		}
	case types.Direct:
		// Direct exchange: Route based on exact match of routing key
		if queues, ok := exchange.Bindings[routingKey]; ok {
			for _, queue := range queues {
				b.sendMessage(queue, message)
			}
		}
	case types.Topic:
		// Topic exchange: Match routing key patterns (simple matching logic here)
		for key, queues := range exchange.Bindings {
			if matchesTopic3(routingKey, key) {
				for _, queue := range queues {
					b.sendMessage(queue, message)
				}
			}
		}
	default:
		return fmt.Errorf("unsupported exchange type: %v", exchange.Type)
	}
	return nil
}

// sendMessage safely sends a message to a queue.
func (b *Broker) sendMessage(queue *types.Queue, message types.Message) {
	select {
	case queue.Messages <- message:
		// Message sent successfully
	default:
		fmt.Printf("Queue %s is full. Message dropped.\n", queue.Name) // Handle full queue scenario
	}
}

// matchesTopic checks if a routing key matches a topic pattern.
func matchesTopic3(routingKey, pattern string) bool {
	// Basic implementation: can be expanded with wildcards if needed
	return routingKey == pattern
}
