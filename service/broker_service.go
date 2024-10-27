package service

import (
	"encoding/json"
	"fmt"
	"github.com/ozgen/raven-mq/broker"
	"github.com/ozgen/raven-mq/types"
	"net/http"
)

// BrokerService manages HTTP endpoints for interacting with the Broker.
type BrokerService struct {
	broker *broker.Broker
}

// NewBrokerService initializes and returns a BrokerService.
func NewBrokerService(b *broker.Broker) *BrokerService {
	return &BrokerService{broker: b}
}

// RegisterRoutes sets up the HTTP endpoints for the broker service.
func (s *BrokerService) RegisterRoutes() {
	http.HandleFunc("/create", s.createHandler)
	http.HandleFunc("/add_publisher", s.addPublisherHandler)
	http.HandleFunc("/add_consumer", s.addConsumerHandler)
	http.HandleFunc("/publish", s.publishMessageHandler)
	http.HandleFunc("/consume", s.consumeHandler)
}

// createHandler allows dynamic creation of exchanges and queues.
func (s *BrokerService) createHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Exchange     string `json:"exchange"`
		ExchangeType string `json:"exchange_type"`
		Queue        string `json:"queue"`
		RoutingKey   string `json:"routing_key"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	// Create the exchange if it does not exist
	exchangeType := types.Direct
	if req.ExchangeType == "fanout" {
		exchangeType = types.Fanout
	} else if req.ExchangeType == "topic" {
		exchangeType = types.Topic
	}

	_, err := s.broker.CreateExchange(req.Exchange, exchangeType)
	if err != nil && err.Error() != "exchange already exists" {
		http.Error(w, fmt.Sprintf("Failed to create exchange: %v", err), http.StatusInternalServerError)
		return
	}

	// Create the queue if it does not exist
	_, err = s.broker.CreateQueue(req.Queue)
	if err != nil && err.Error() != "queue already exists" {
		http.Error(w, fmt.Sprintf("Failed to create queue: %v", err), http.StatusInternalServerError)
		return
	}

	// Bind the queue to the exchange with the specified routing key
	err = s.broker.BindQueue(req.Exchange, req.RoutingKey, req.Queue)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to bind queue: %v", err), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "Exchange, queue, and binding created successfully")
}

// addPublisherHandler allows adding a new publisher dynamically.
func (s *BrokerService) addPublisherHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Exchange   string `json:"exchange"`
		RoutingKey string `json:"routing_key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	// Check if exchange exists
	if _, exists := s.broker.Exchanges[req.Exchange]; !exists {
		http.Error(w, "Exchange not found", http.StatusNotFound)
		return
	}

	fmt.Fprintf(w, "Publisher added for exchange %s with routing key %s\n", req.Exchange, req.RoutingKey)
}

// addConsumerHandler allows adding a new consumer dynamically.
func (s *BrokerService) addConsumerHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Queue string `json:"queue"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	// Check if queue exists
	queue, exists := s.broker.Queues[req.Queue]
	if !exists {
		http.Error(w, "Queue not found", http.StatusNotFound)
		return
	}

	// Start a new consumer on the specified queue
	go func(q *types.Queue) {
		for msg := range q.Messages {
			fmt.Printf("Dynamically consumed message from queue %s: %s\n", q.Name, msg.Body)
		}
	}(queue)

	fmt.Fprintf(w, "Consumer added for queue %s\n", req.Queue)
}

// publishMessageHandler allows publishing a new message dynamically.
func (s *BrokerService) publishMessageHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Exchange   string `json:"exchange"`
		RoutingKey string `json:"routing_key"`
		Body       string `json:"body"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	// Publish the message
	message := types.Message{Body: req.Body, RoutingKey: req.RoutingKey}
	err := s.broker.PublishMessage(req.Exchange, req.RoutingKey, message)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "Message published to exchange %s with routing key %s\n", req.Exchange, req.RoutingKey)
}

// consumeHandler streams messages from a specified queue to the client.
func (s *BrokerService) consumeHandler(w http.ResponseWriter, r *http.Request) {
	// Get the queue name from the URL query
	queueName := r.URL.Query().Get("queue")
	queue, exists := s.broker.Queues[queueName]
	if !exists {
		http.Error(w, "Queue not found", http.StatusNotFound)
		return
	}

	// Set response headers for streaming
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// Flusher for sending events
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	// Stream messages to the client
	for msg := range queue.Messages {
		fmt.Fprintf(w, "data: %s\n\n", msg.Body)
		flusher.Flush() // Flush the response buffer to send the event
	}
}
