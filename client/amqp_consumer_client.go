package client

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

// AmqpConsumerClient manages message consumption and interactions with the AMQP broker.
type AmqpConsumerClient struct {
	brokerAddr string
	conn       net.Conn
}

// NewAmqpConsumerClient initializes and returns a new AmqpConsumerClient.
func NewAmqpConsumerClient(brokerAddr string) (*AmqpConsumerClient, error) {
	conn, err := net.Dial("tcp", brokerAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to broker at %s: %w", brokerAddr, err)
	}
	return &AmqpConsumerClient{brokerAddr: brokerAddr, conn: conn}, nil
}

// Close closes the connection to the broker.
func (c *AmqpConsumerClient) Close() error {
	return c.conn.Close()
}

// DefineQueueAndExchange creates an exchange, queue, and binding on the broker.
func (c *AmqpConsumerClient) DefineQueueAndExchange(exchange, exchangeType, queue, routingKey string) error {
	commands := []string{
		fmt.Sprintf("DECLARE_EXCHANGE %s %s", exchange, exchangeType),
		fmt.Sprintf("DECLARE_QUEUE %s", queue),
		fmt.Sprintf("BIND_QUEUE %s %s %s", queue, exchange, routingKey),
	}
	for _, cmd := range commands {
		if err := c.sendCommand(cmd); err != nil {
			return err
		}
	}
	return nil
}

// sendCommand sends a command to the broker and reads the response.
func (c *AmqpConsumerClient) sendCommand(command string) error {
	_, err := fmt.Fprintf(c.conn, "%s\n", command)
	if err != nil {
		return fmt.Errorf("failed to send command: %w", err)
	}

	// Read the response from the broker
	response, err := bufio.NewReader(c.conn).ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	// Handle responses that indicate success
	if strings.Contains(response, "successfully") || strings.Contains(response, "Message published") {
		return nil
	}

	// Treat "Consumer started" as valid for consume command acknowledgment
	if strings.TrimSpace(response) == "Consumer started" {
		return nil
	}

	// Treat any other response as a broker error
	return fmt.Errorf("broker error: %s", strings.TrimSpace(response))
}

// Consume connects to the broker and reads messages from the specified queue.
func (c *AmqpConsumerClient) Consume(queueName string, handleMessage func(string)) error {
	// Send the consume command
	err := c.sendCommand(fmt.Sprintf("CONSUME %s", queueName))
	if err != nil {
		return err
	}

	// Start reading messages from the broker
	scanner := bufio.NewScanner(c.conn)

	for scanner.Scan() {
		line := scanner.Text()
		handleMessage(line) // Pass each line directly to the callback as a message
	}

	// Handle any scanning error that might occur
	if scanner.Err() != nil {
		return fmt.Errorf("error reading messages: %w", scanner.Err())
	}
	return nil
}
