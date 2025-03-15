package client

import (
	"bufio"
	"fmt"
	"github.com/ozgen/raven-mq/internal/log"
	"net"
	"strings"
)

// RavenMQAmqpProducerClient manages message publishing and interactions with the AMQP broker, with reconnection support.
type RavenMQAmqpProducerClient struct {
	brokerAddr      string
	conn            net.Conn
	reconnectPolicy *ReconnectionPolicy
}

// NewAmqpProducerClientWithPolicy initializes a RavenMQAmqpProducerClient with a custom reconnection policy.
func NewAmqpProducerClientWithPolicy(brokerAddr string, reconnectPolicy *ReconnectionPolicy) (*RavenMQAmqpProducerClient, error) {
	client := &RavenMQAmqpProducerClient{
		brokerAddr:      brokerAddr,
		reconnectPolicy: reconnectPolicy,
	}
	err := client.connect()
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	return client, nil
}

// NewAmqpProducerClient initializes a RavenMQAmqpProducerClient with the default reconnection policy.
func NewAmqpProducerClient(brokerAddr string) (*RavenMQAmqpProducerClient, error) {
	return NewAmqpProducerClientWithPolicy(brokerAddr, NewDefaultReconnectionPolicy())
}

// connect establishes a connection using the reconnection policy.
func (p *RavenMQAmqpProducerClient) connect() error {
	log.LogInfo("Attempting to connect to broker at %s...", p.brokerAddr)
	conn, err := p.reconnectPolicy.ExecuteWithReconnect(func() (net.Conn, error) {
		return net.Dial("tcp", p.brokerAddr)
	})
	if err != nil {
		return fmt.Errorf("connection failed: %w", err)
	}
	p.conn = conn
	log.LogInfo("Connected to broker successfully.")
	return nil
}

// Close closes the connection to the broker.
func (p *RavenMQAmqpProducerClient) Close() error {
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}

// DefineQueueAndExchange sets up an exchange, queue, and binding dynamically based on the exchange type.
func (p *RavenMQAmqpProducerClient) DefineQueueAndExchange(exchange, exchangeType, queue, routingKey string) error {
	// Declare the exchange
	command := fmt.Sprintf("DECLARE_EXCHANGE %s %s", exchange, exchangeType)
	log.LogInfo("Sending command to broker: %s", command)
	if err := p.sendCommand(command); err != nil {
		log.LogWarn("Error declaring exchange '%s': %v", exchange, err)
		if reconnectErr := p.reconnect(); reconnectErr != nil {
			return fmt.Errorf("reconnect failed: %w", reconnectErr)
		}
		if err := p.sendCommand(command); err != nil {
			return fmt.Errorf("exchange declaration failed after reconnect: %w", err)
		}
	}
	log.LogInfo("Exchange '%s' of type '%s' declared successfully.", exchange, exchangeType)

	// Only define queue and binding for direct or topic exchanges
	if exchangeType != "fanout" {
		commands := []string{
			fmt.Sprintf("DECLARE_QUEUE %s", queue),
			fmt.Sprintf("BIND_QUEUE %s %s %s", queue, exchange, routingKey),
		}
		for _, cmd := range commands {
			log.LogInfo("Sending command to broker: %s", cmd)
			if err := p.sendCommand(cmd); err != nil {
				log.LogWarn("Error sending command '%s': %v", cmd, err)
				if reconnectErr := p.reconnect(); reconnectErr != nil {
					return fmt.Errorf("reconnect failed: %w", reconnectErr)
				}
				if err := p.sendCommand(cmd); err != nil {
					return fmt.Errorf("command failed after reconnect: %w", err)
				}
			}
		}
		log.LogInfo("Queue '%s' and binding to exchange '%s' with routing key '%s' completed.", queue, exchange, routingKey)
	} else {
		log.LogInfo("Skipping queue and binding setup for fanout exchange '%s'.", exchange)
	}

	return nil
}

// reconnect safely closes the existing connection and re-establishes a new one.
func (p *RavenMQAmqpProducerClient) reconnect() error {
	if p.conn != nil {
		log.LogInfo("Closing existing connection before attempting to reconnect...")
		_ = p.conn.Close()
	}
	log.LogInfo("Attempting to reconnect to broker at %v", p.brokerAddr)
	conn, err := p.reconnectPolicy.ExecuteWithReconnect(func() (net.Conn, error) {
		return net.Dial("tcp", p.brokerAddr)
	})
	if err != nil {
		return fmt.Errorf("reconnect failed: %w", err)
	}
	p.conn = conn
	log.LogInfo("Connected to broker successfully.")
	return nil
}

// Publish sends a message to a specified exchange and routing key on the broker.
func (p *RavenMQAmqpProducerClient) Publish(exchange, routingKey, message string) error {
	command := fmt.Sprintf("PUBLISH %s %s %s", exchange, routingKey, message)
	if err := p.sendCommand(command); err != nil {
		log.LogWarn("Publish failed; attempting reconnection...")
		if reconnectErr := p.reconnect(); reconnectErr != nil {
			return fmt.Errorf("reconnect failed after publish error: %w", reconnectErr)
		}
		// Retry the publish command after reconnecting
		if err := p.sendCommand(command); err != nil {
			return fmt.Errorf("publish command failed after reconnect: %w", err)
		}
	}
	log.LogInfo("Message published to exchange '%s' with routing key '%s': %s", exchange, routingKey, message)
	return nil
}

// sendCommand sends a command to the broker and reads the response.
func (p *RavenMQAmqpProducerClient) sendCommand(command string) error {
	_, err := fmt.Fprintf(p.conn, "%s\n", command)
	if err != nil {
		return fmt.Errorf("failed to send command: %w", err)
	}

	response, err := bufio.NewReader(p.conn).ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}
	if !strings.Contains(response, "successfully") && !strings.Contains(response, "Message published") {
		return fmt.Errorf("broker error: %s", strings.TrimSpace(response))
	}

	log.LogInfo("Broker response for command '%s': %s", command, strings.TrimSpace(response))
	return nil
}
