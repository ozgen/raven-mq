package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

// DefineRequest defines the structure for creating an exchange, queue, and binding on the broker.
type DefineRequest struct {
	Exchange     string `json:"exchange"`
	ExchangeType string `json:"exchange_type"`
	Queue        string `json:"queue"`
	RoutingKey   string `json:"routing_key"`
}

// DefineQueueAndExchange sends a request to the broker to create an exchange, queue, and binding.
// This function is shared by both ProducerClient and ConsumerClient.
func DefineQueueAndExchange(brokerURL, exchange, exchangeType, queue, routingKey string) error {
	// Create the request body
	reqBody := DefineRequest{
		Exchange:     exchange,
		ExchangeType: exchangeType,
		Queue:        queue,
		RoutingKey:   routingKey,
	}

	// Encode the request as JSON
	data, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to encode request body: %w", err)
	}

	// Send the request to the broker’s create endpoint
	resp, err := http.Post(fmt.Sprintf("%s/create", brokerURL), "application/json", bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("failed to create exchange and queue: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to create exchange and queue: received status %d", resp.StatusCode)
	}

	fmt.Println("Exchange, queue, and binding created successfully")
	return nil
}
