package main

import (
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/ozgen/raven-mq/client"
)

// TestConsumerService manages the consumer's state and operations.
type TestConsumerService struct {
	client          *client.RavenMQAmqpConsumerClient
	consumerStarted bool
	mu              sync.Mutex
}

// NewTestConsumerService initializes and returns a new TestConsumerService.
func NewTestConsumerService(brokerAddr string) (*TestConsumerService, error) {
	// Define a custom reconnection policy
	reconnectPolicy := client.NewReconnectionPolicy(10, 1*time.Second, 10*time.Second)

	// Initialize the RavenMQAmqpConsumerClient with the broker's TCP address and custom policy
	amqpClient, err := client.NewAmqpConsumerClientWithPolicy(brokerAddr, reconnectPolicy)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize RavenMQAmqpConsumerClient: %w", err)
	}

	return &TestConsumerService{
		client: amqpClient,
	}, nil
}

// Start initializes the queue and exchange, then begins consuming messages if not already running.
func (s *TestConsumerService) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.consumerStarted {
		return fmt.Errorf("consumer is already running")
	}

	// Define the necessary exchange and queue on RavenMQ every time Start is called
	err := s.client.DefineQueueAndExchange("example_exchange", "direct", "example_queue", "example_key")
	if err != nil {
		log.Printf("Failed to define exchange and queue: %v", err)
		return err
	}
	log.Println("Exchange and queue defined successfully")

	// Start consuming messages in a goroutine
	go func() {
		err := s.client.Consume("example_exchange", "direct", "example_queue", "example_key", func(message string) {
			fmt.Println("Consumed message:", message)
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
	consumerService, err := NewTestConsumerService("localhost:2122")
	if err != nil {
		log.Fatalf("Failed to initialize ConsumerService: %v", err)
	}
	defer consumerService.client.Close()

	// Define the /start handler
	http.HandleFunc("/start", func(w http.ResponseWriter, r *http.Request) {
		err := consumerService.Start()
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.Write([]byte("Consumer started"))
		log.Println("Consumer started and listening for messages")
	})

	// Start the consumer server on port 8081
	log.Println("Consumer server running on port 8081...")
	log.Fatal(http.ListenAndServe(":8081", nil))
}
