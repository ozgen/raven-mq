package client

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/ozgen/raven-mq/internal/common"
	"github.com/ozgen/raven-mq/internal/log"
	"net"
	"strings"
)

// RavenMQAmqpConsumerClient manages message consumption from the AMQP broker with reconnection support.
type RavenMQAmqpConsumerClient struct {
	brokerAddr      string
	Conn            net.Conn
	reconnectPolicy *ReconnectionPolicy
	consumeIssued   bool                // Track if the consume command has been issued
	recentMessages  map[string]struct{} // Track recent message hashes for duplicate detection
}

// NewAmqpConsumerClientWithPolicy initializes a RavenMQAmqpConsumerClient with a custom reconnection policy.
func NewAmqpConsumerClientWithPolicy(brokerAddr string, reconnectPolicy *ReconnectionPolicy) (*RavenMQAmqpConsumerClient, error) {
	client := &RavenMQAmqpConsumerClient{
		brokerAddr:      brokerAddr,
		reconnectPolicy: reconnectPolicy,
		recentMessages:  make(map[string]struct{}),
	}
	err := client.connect()
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	return client, nil
}

// NewAmqpConsumerClient initializes a RavenMQAmqpConsumerClient with the default reconnection policy.
func NewAmqpConsumerClient(brokerAddr string) (*RavenMQAmqpConsumerClient, error) {
	return NewAmqpConsumerClientWithPolicy(brokerAddr, NewDefaultReconnectionPolicy())
}

// connect establishes a connection using the reconnection policy.
func (c *RavenMQAmqpConsumerClient) connect() error {
	log.LogInfo("Attempting to connect to broker at %s...", c.brokerAddr)
	conn, err := c.reconnectPolicy.ExecuteWithReconnect(func() (net.Conn, error) {
		return net.Dial("tcp", c.brokerAddr)
	})
	if err != nil {
		return fmt.Errorf("connection failed: %w", err)
	}
	c.Conn = conn
	log.LogInfo("Connected to broker successfully.")
	return nil
}

// Close closes the connection to the broker.
func (c *RavenMQAmqpConsumerClient) Close() error {
	if c.Conn != nil {
		return c.Conn.Close()
	}
	return nil
}

// DefineQueueAndExchange sets up an exchange, queue, and binding on the broker only if they haven't been defined for this session.
func (c *RavenMQAmqpConsumerClient) DefineQueueAndExchange(exchange, exchangeType, queue, routingKey string) error {
	// Validate inputs
	if exchange == "" || exchangeType == "" || queue == "" || routingKey == "" {
		return fmt.Errorf("invalid parameters: exchange='%s', exchangeType='%s', queue='%s', routingKey='%s'", exchange, exchangeType, queue, routingKey)
	}

	commands := []string{
		fmt.Sprintf("DECLARE_EXCHANGE %s %s", exchange, exchangeType),
		fmt.Sprintf("DECLARE_QUEUE %s", queue),
		fmt.Sprintf("BIND_QUEUE %s %s %s", queue, exchange, routingKey),
	}

	for _, cmd := range commands {
		log.LogInfo("Sending command to broker: %s", cmd)

		// Validate command parameters
		if strings.Contains(cmd, "BIND_QUEUE") && (queue == "" || exchange == "" || routingKey == "") {
			return fmt.Errorf("invalid BIND_QUEUE parameters: queue='%s', exchange='%s', routingKey='%s'", queue, exchange, routingKey)
		}

		// Send the command
		if err := c.sendCommand(cmd); err != nil {
			log.LogError("Error sending command '%s': %v", cmd, err)

			// Handle "already exists" error gracefully
			if strings.Contains(err.Error(), "already exists") {
				log.LogInfo("Command '%s' already executed, skipping.", cmd)
				continue
			}

			// Attempt reconnection on transient errors
			if reconnectErr := c.reconnect(); reconnectErr != nil {
				return fmt.Errorf("reconnect failed during setup: %w", reconnectErr)
			}

			// Retry the command after reconnecting
			log.LogInfo("Retrying command '%s' after reconnecting...", cmd)
			if err := c.sendCommand(cmd); err != nil {
				return fmt.Errorf("command failed after reconnect: %w", err)
			}
		}
	}

	log.LogInfo("Exchange, queue, and binding setup complete.")
	return nil
}

// hashMessage creates a hash for a given message to track duplicates.
func hashMessage(message string) string {
	hash := sha256.Sum256([]byte(message))
	return hex.EncodeToString(hash[:])
}

// Consume connects to the broker and reads messages from the specified queue, ensuring redefinition if the queue is missing.
func (c *RavenMQAmqpConsumerClient) Consume(exchange, exchangeType, queue, routingKey string, handleMessage func(string) error) error {
	log.LogInfo("Initiating consumption from queue: %s\n", queue)

	// Define the queue and exchange before starting consumption
	if err := c.DefineQueueAndExchange(exchange, exchangeType, queue, routingKey); err != nil {
		return fmt.Errorf("failed to define queue and exchange: %w", err)
	}

	// Send the consume command
	err := c.sendCommand(fmt.Sprintf("CONSUME %s", queue))
	if err != nil {
		if reconnectErr := c.reconnectWithRedefine(exchange, exchangeType, queue, routingKey); reconnectErr != nil {
			return fmt.Errorf("initial connect error: %w", reconnectErr)
		}
		// Retry consume command post-reconnect
		err = c.sendCommand(fmt.Sprintf("CONSUME %s", queue))
		if err != nil {
			return err
		}
	}

	// Track last message hash to check for duplicates
	lastMessageHash := ""

	// Continuously read messages
	scanner := bufio.NewScanner(c.Conn)
	for {
		if !scanner.Scan() {
			log.LogWarn("Connection lost while consuming. Attempting to reconnect...")
			if err := c.reconnectWithRedefine(exchange, exchangeType, queue, routingKey); err != nil {
				return fmt.Errorf("reconnect error: %w", err)
			}
			// Re-send consume command after reconnect
			log.LogInfo("Resending consume command after reconnect: CONSUME %s\n", queue)
			if err := c.sendCommand(fmt.Sprintf("CONSUME %s", queue)); err != nil {
				return fmt.Errorf("failed to resend consume command after reconnect: %w", err)
			}
			scanner = bufio.NewScanner(c.Conn)
			continue
		}

		line := scanner.Text()
		if strings.HasPrefix(line, "Consumed message: ") {
			message := strings.TrimPrefix(line, "Consumed message: ")
			messageHash := hashMessage(message)

			if messageHash != lastMessageHash {
				lastMessageHash = messageHash
				log.LogInfo("Single instance consumed message: %s\n", message)
				if err := handleMessage(message); err == nil {
					ackCmd := fmt.Sprintf("ACK %s %s", queue, message)
					ackErr := c.sendCommand(ackCmd)
					if ackErr != nil {
						log.LogWarn("Failed to send ACK for message %s: %v", message, ackErr)
					} else {
						log.LogInfo("ACK confirmed for message: %s", message)
					}
				}
			} else {
				log.LogInfo("Duplicate message detected, ignoring: %s\n", message)
			}
		} else if line == common.MsgConsumerStopped {
			log.LogInfo("Consumer explicitly stopped by broker.")
			break
		} else if strings.Contains(line, common.QueueNotFoundPrefix) {
			log.LogInfo("Queue '%s' not found after reconnect. Attempting to redefine...", queue)
			if redefineErr := c.reconnectWithRedefine(exchange, exchangeType, queue, routingKey); redefineErr != nil {
				return fmt.Errorf("redefinition failed after reconnect: %w", redefineErr)
			}
		} else {
			log.LogWarn("Unexpected response: %s\n", line)
			return fmt.Errorf("unexpected response: %s", line)
		}
	}

	log.LogInfo("Finished consuming messages.")
	return scanner.Err()
}

// reconnectWithRedefine reconnects and redefines the queue and exchange if necessary.
func (c *RavenMQAmqpConsumerClient) reconnectWithRedefine(exchange, exchangeType, queue, routingKey string) error {
	if err := c.reconnect(); err != nil {
		return err
	}
	return c.DefineQueueAndExchange(exchange, exchangeType, queue, routingKey)
}

// reconnect safely closes the existing connection and re-establishes a new one.
func (c *RavenMQAmqpConsumerClient) reconnect() error {
	if c.Conn != nil {
		log.LogInfo("Closing existing connection before attempting to reconnect...")
		_ = c.Conn.Close()
	}
	log.LogWarn("Attempting to reconnect to broker at %v", c.brokerAddr)
	conn, err := c.reconnectPolicy.ExecuteWithReconnect(func() (net.Conn, error) {
		return net.Dial("tcp", c.brokerAddr)
	})
	if err != nil {
		return fmt.Errorf("reconnect failed: %w", err)
	}
	c.Conn = conn
	c.consumeIssued = false // Reset the consume issued flag
	log.LogInfo("Reconnected to broker successfully.")
	return nil
}

// sendCommand sends a command to the broker and reads the response.
func (c *RavenMQAmqpConsumerClient) sendCommand(command string) error {
	_, err := fmt.Fprintf(c.Conn, "%s\n", command)
	if err != nil {
		return fmt.Errorf("failed to send command: %w", err)
	}

	response, err := bufio.NewReader(c.Conn).ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}
	if !strings.Contains(response, common.SuccessKeyword) && !strings.Contains(response, common.MsgConsumerStarted) {
		return fmt.Errorf("broker error: %s", strings.TrimSpace(response))
	}

	log.LogInfo("Broker response for command '%s': %s", command, strings.TrimSpace(response))
	return nil
}
