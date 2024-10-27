package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

// ProducerClient manages message publishing to the broker.
type ProducerClient struct {
	brokerURL string
}

// NewProducerClient initializes and returns a new ProducerClient.
func NewProducerClient(brokerURL string) *ProducerClient {
	return &ProducerClient{brokerURL: brokerURL}
}

// Message defines the structure for publishing to the broker.
type Message struct {
	Exchange   string `json:"exchange"`
	RoutingKey string `json:"routing_key"`
	Body       string `json:"body"`
}

// DefineQueueAndExchange creates an exchange, queue, and binding on the broker.
func (p *ProducerClient) DefineQueueAndExchange(exchange, exchangeType, queue, routingKey string) error {
	return DefineQueueAndExchange(p.brokerURL, exchange, exchangeType, queue, routingKey)
}

// Publish sends a message to the specified exchange and routing key on the broker.
func (p *ProducerClient) Publish(exchange, routingKey, body string) error {
	// Construct the message
	msg := Message{
		Exchange:   exchange,
		RoutingKey: routingKey,
		Body:       body,
	}

	// Encode the message as JSON
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to encode message: %w", err)
	}

	// Send the message to the broker’s publish endpoint
	resp, err := http.Post(fmt.Sprintf("%s/publish", p.brokerURL), "application/json", bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to publish message: received status %d", resp.StatusCode)
	}

	fmt.Println("Message published:", body)
	return nil
}
