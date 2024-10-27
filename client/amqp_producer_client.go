package client

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

// AmqpProducerClient manages message publishing and interactions with the AMQP broker.
type AmqpProducerClient struct {
	brokerAddr string
	conn       net.Conn
}

// NewAmqpProducerClient initializes and returns a new AmqpProducerClient.
func NewAmqpProducerClient(brokerAddr string) (*AmqpProducerClient, error) {
	conn, err := net.Dial("tcp", brokerAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to broker at %s: %w", brokerAddr, err)
	}
	return &AmqpProducerClient{brokerAddr: brokerAddr, conn: conn}, nil
}

// Close closes the connection to the broker.
func (p *AmqpProducerClient) Close() error {
	return p.conn.Close()
}

// DefineQueueAndExchange creates an exchange, queue, and binding on the broker.
func (c *AmqpProducerClient) DefineQueueAndExchange(exchange, exchangeType, queue, routingKey string) error {
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

// Publish sends a message to the specified exchange and routing key on the broker.
func (p *AmqpProducerClient) Publish(exchange, routingKey, body string) error {
	// Construct the publish command
	command := fmt.Sprintf("PUBLISH %s %s %s", exchange, routingKey, body)
	return p.sendCommand(command)
}

// sendCommand sends a command to the broker and reads the response.
func (p *AmqpProducerClient) sendCommand(command string) error {
	_, err := fmt.Fprintf(p.conn, "%s\n", command)
	if err != nil {
		return fmt.Errorf("failed to send command: %w", err)
	}

	// Read the response from the broker
	response, err := bufio.NewReader(p.conn).ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}
	if !strings.Contains(response, "successfully") && !strings.Contains(response, "Message published") {
		return fmt.Errorf("broker error: %s", strings.TrimSpace(response))
	}

	return nil
}
