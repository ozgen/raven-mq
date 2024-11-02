package main

import (
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/ozgen/raven-mq/client"
)

var counter int = 0

// ProducerService manages the producer's operations, including publishing messages.
type ProducerService struct {
	client *client.RavenMQAmqpProducerClient
}

// NewProducerService initializes and returns a new ProducerService with reconnection capabilities.
func NewProducerService(brokerAddr, exchange, exchangeType, queue, routingKey string) (*ProducerService, error) {
	// Define a custom reconnection policy for the producer
	reconnectPolicy := client.NewReconnectionPolicy(10, 1*time.Second, 10*time.Second)

	// Initialize the RavenMQAmqpProducerClient with the broker's TCP address and custom policy
	amqpClient, err := client.NewAmqpProducerClientWithPolicy(brokerAddr, reconnectPolicy)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize RavenMQAmqpProducerClient: %w", err)
	}

	// Define exchange and queue setup once during initialization
	err = amqpClient.DefineQueueAndExchange(exchange, exchangeType, queue, routingKey)
	if err != nil {
		return nil, fmt.Errorf("failed to define exchange and queue during initialization: %w", err)
	}
	log.Println("Exchange and queue defined successfully during initialization")

	return &ProducerService{
		client: amqpClient,
	}, nil
}

// Close closes the connection to the broker.
func (p *ProducerService) Close() error {
	return p.client.Close()
}

// PublishMessage publishes a message to the predefined exchange and queue.
func (p *ProducerService) PublishMessage(w http.ResponseWriter, r *http.Request) {
	counter++
	message := "Hello from Producer! counter: " + strconv.Itoa(counter)
	err := p.client.DefineQueueAndExchange("example_exchange", "direct", "example_queue", "example_key")

	// Publish the message
	err = p.client.Publish("example_exchange", "example_key", message)
	if err != nil {
		http.Error(w, "Failed to publish message", http.StatusInternalServerError)
		log.Printf("Failed to publish message: %v", err)
		return
	}

	w.Write([]byte("Message published successfully"))
	log.Println("Message published to RavenMQ")
}

func main() {
	// Initialize the ProducerService with the broker's TCP address
	producerService, err := NewProducerService("localhost:2122", "example_exchange", "direct", "example_queue", "example_key")
	if err != nil {
		log.Fatalf("Failed to initialize ProducerService: %v", err)
	}
	defer producerService.Close()

	// Define a handler to publish messages
	http.HandleFunc("/send", producerService.PublishMessage)

	// Start the producer server on port 8080
	log.Println("Producer server running on port 8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
