package client

import (
	"bufio"
	"fmt"
	"net/http"
	"strings"
)

// ConsumerClient manages message consumption from the broker.
type ConsumerClient struct {
	brokerURL string
}

// NewConsumerClient initializes and returns a new ConsumerClient.
func NewConsumerClient(brokerURL string) *ConsumerClient {
	return &ConsumerClient{brokerURL: brokerURL}
}

// DefineQueueAndExchange creates an exchange, queue, and binding on the broker.
func (c *ConsumerClient) DefineQueueAndExchange(exchange, exchangeType, queue, routingKey string) error {
	return DefineQueueAndExchange(c.brokerURL, exchange, exchangeType, queue, routingKey)
}

// Consume connects to the broker and reads messages from the specified queue.
// It accepts a callback function to handle messages as they arrive.
func (c *ConsumerClient) Consume(queueName string, handleMessage func(string)) error {
	// Connect to the broker's consume endpoint
	resp, err := http.Get(fmt.Sprintf("%s/consume?queue=%s", c.brokerURL, queueName))
	if err != nil {
		return fmt.Errorf("failed to connect to broker for consuming messages: %w", err)
	}
	defer resp.Body.Close()

	// Check for a successful response
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to consume messages: received status %d", resp.StatusCode)
	}

	// Continuously read messages from the response stream
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		// Check if the line has the "data: " prefix and trim it
		if strings.HasPrefix(line, "data: ") {
			line = strings.TrimPrefix(line, "data: ")
		}
		handleMessage(line) // Invoke the callback function with the clean message
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading messages: %w", err)
	}
	return nil
}
