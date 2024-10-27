package main

import (
	"fmt"
	"github.com/ozgen/raven-mq/client"
	"log"
	"net/http"
)

// ProducerService manages the producer's operations, including publishing messages.
type ProducerService struct {
	client *client.AmqpProducerClient
}

// NewProducerService initializes and returns a new ProducerService.
func NewProducerService(brokerAddr string) (*ProducerService, error) {
	// Initialize the AmqpProducerClient with the broker's TCP address
	amqpClient, err := client.NewAmqpProducerClient(brokerAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize AmqpProducerClient: %w", err)
	}
	return &ProducerService{
		client: amqpClient,
	}, nil
}

// Close closes the connection to the broker.
func (p *ProducerService) Close() error {
	return p.client.Close()
}

// PublishMessage defines the exchange and queue, then publishes a message.
func (p *ProducerService) PublishMessage(w http.ResponseWriter, r *http.Request) {

	// Publish the message
	err := p.client.Publish("example_exchange", "example_key", "Hello from Producer!")
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
	producerService, err := NewProducerService("localhost:2122")
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
