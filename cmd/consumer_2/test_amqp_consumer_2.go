package main

import (
	"fmt"
	client2 "github.com/ozgen/raven-mq/internal/client"
	"log"
	"net/http"
	"sync"
	"time"
)

// Test2ConsumerService manages the consumer's state and operations.
type Test2ConsumerService struct {
	client          *client2.RavenMQAmqpConsumerClient
	consumerStarted bool
	mu              sync.Mutex
}

// NewTest2ConsumerService initializes and returns a new TestConsumerService.
func NewTest2ConsumerService(brokerAddr string) (*Test2ConsumerService, error) {
	// Define a custom reconnection policy
	reconnectPolicy := client2.NewReconnectionPolicy(10, 1*time.Second, 10*time.Second)

	// Initialize the RavenMQAmqpConsumerClient with the broker's TCP address and custom policy
	amqpClient, err := client2.NewAmqpConsumerClientWithPolicy(brokerAddr, reconnectPolicy)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize RavenMQAmqpConsumerClient: %w", err)
	}

	return &Test2ConsumerService{
		client: amqpClient,
	}, nil
}

// Start initializes the queue and exchange, then begins consuming messages if not already running.
func (s *Test2ConsumerService) Start2() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.consumerStarted {
		return fmt.Errorf("consumer is already running")
	}

	// Define the necessary exchange and queue on RavenMQ every time Start is called
	err := s.client.DefineQueueAndExchange("example_exchange", "fanout", "example_queue2", "example_key")
	if err != nil {
		log.Printf("Failed to define exchange and queue: %v", err)
		return err
	}
	log.Println("Exchange and queue defined successfully")

	// Start consuming messages in a goroutine
	go func() {
		err := s.client.Consume("example_exchange", "fanout", "example_queue2", "example_key", func(message string) error {
			fmt.Println("Consumed message:", message)
			return nil
		})

		if err != nil {
			log.Printf("Failed to consume messages: %v", err)
		}
	}()

	s.consumerStarted = true
	return nil
}

func main() {
	// Initialize the consumer service with the RavenMQ server address
	consumerService, err := NewTest2ConsumerService("localhost:2122")
	if err != nil {
		log.Fatalf("Failed to initialize ConsumerService: %v", err)
	}
	defer consumerService.client.Close()

	// Define the /start handler
	http.HandleFunc("/start", func(w http.ResponseWriter, r *http.Request) {
		err := consumerService.Start2()
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.Write([]byte("Consumer started"))
		log.Println("Consumer started and listening for messages")
	})

	// Start the consumer server on port 8081
	log.Println("Consumer server running on port 8082...")
	log.Fatal(http.ListenAndServe(":8082", nil))
}
