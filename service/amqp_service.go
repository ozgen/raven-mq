package service

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"

	"github.com/ozgen/raven-mq/types"
)

// AMQPService acts as the central broker, managing exchanges, queues, and message routing.
type AMQPService struct {
	queues    map[string]*types.Queue    // Collection of queues
	exchanges map[string]*types.Exchange // Collection of exchanges
	mux       sync.RWMutex               // Mutex for safe access to queues and exchanges
}

// NewAMQPService initializes the AMQPService with empty exchanges and queues.
func NewAMQPService() *AMQPService {
	return &AMQPService{
		queues:    make(map[string]*types.Queue),
		exchanges: make(map[string]*types.Exchange),
	}
}

// HandleConnection listens for commands on a TCP connection and manages exchanges, queues, bindings, and messages.
func (s *AMQPService) HandleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			log.Println("Client disconnected or error reading:", err)
			return
		}

		line = strings.TrimSpace(line)
		parts := strings.Split(line, " ")
		command := parts[0]

		switch command {
		case "DECLARE_QUEUE":
			if len(parts) < 2 {
				fmt.Fprintln(conn, "Usage: DECLARE_QUEUE <queue_name>")
				continue
			}
			queueName := parts[1]
			err := s.declareQueue(queueName)
			if err != nil {
				fmt.Fprintf(conn, "Error: %v\n", err)
			} else {
				fmt.Fprintln(conn, "Queue declared successfully")
			}

		case "DECLARE_EXCHANGE":
			if len(parts) < 3 {
				fmt.Fprintln(conn, "Usage: DECLARE_EXCHANGE <exchange_name> <type>")
				continue
			}
			exName, exType := parts[1], parts[2]
			err := s.declareExchange(exName, exType)
			if err != nil {
				fmt.Fprintf(conn, "Error: %v\n", err)
			} else {
				fmt.Fprintln(conn, "Exchange declared successfully")
			}

		case "BIND_QUEUE":
			if len(parts) < 4 {
				fmt.Fprintln(conn, "Usage: BIND_QUEUE <queue_name> <exchange_name> <routing_key>")
				continue
			}
			queueName, exchangeName, routingKey := parts[1], parts[2], parts[3]
			err := s.bindQueue(queueName, exchangeName, routingKey)
			if err != nil {
				fmt.Fprintf(conn, "Error: %v\n", err)
			} else {
				fmt.Fprintln(conn, "Queue bound to exchange successfully")
			}

		case "PUBLISH":
			if len(parts) < 4 {
				fmt.Fprintln(conn, "Usage: PUBLISH <exchange_name> <routing_key> <message>")
				continue
			}
			exchangeName, routingKey := parts[1], parts[2]
			message := strings.Join(parts[3:], " ")
			err := s.publishMessage(exchangeName, routingKey, types.Message{Body: message, RoutingKey: routingKey})
			if err != nil {
				fmt.Fprintf(conn, "Error: %v\n", err)
			} else {
				fmt.Fprintln(conn, "Message published successfully")
			}

		case "CONSUME":
			if len(parts) < 2 {
				fmt.Fprintln(conn, "Usage: CONSUME <queue_name>")
				continue
			}
			queueName := parts[1]
			go s.consumeQueue(conn, queueName) // Start consuming in a separate goroutine
			fmt.Fprintln(conn, "Consumer started")

		default:
			fmt.Fprintln(conn, "Unknown command")
		}
	}
}

// declareQueue creates a new queue within the service.
func (s *AMQPService) declareQueue(name string) error {
	s.mux.Lock()
	defer s.mux.Unlock()

	if _, exists := s.queues[name]; exists {
		return fmt.Errorf("queue %s already exists", name)
	}

	queue := &types.Queue{
		Name:     name,
		Messages: make(chan types.Message, 100),
	}
	s.queues[name] = queue
	return nil
}

// declareExchange creates a new exchange of a given type within the service.
func (s *AMQPService) declareExchange(name, exType string) error {
	s.mux.Lock()
	defer s.mux.Unlock()

	if _, exists := s.exchanges[name]; exists {
		return fmt.Errorf("exchange %s already exists", name)
	}

	var exchangeType types.ExchangeType
	switch exType {
	case "direct":
		exchangeType = types.Direct
	case "fanout":
		exchangeType = types.Fanout
	case "topic":
		exchangeType = types.Topic
	default:
		return fmt.Errorf("unsupported exchange type: %s", exType)
	}

	s.exchanges[name] = &types.Exchange{
		Name:     name,
		Type:     exchangeType,
		Bindings: make(map[string][]*types.Queue),
	}
	return nil
}

// bindQueue binds a queue to an exchange with a specific routing key.
func (s *AMQPService) bindQueue(queueName, exchangeName, routingKey string) error {
	s.mux.RLock()
	exchange, exExists := s.exchanges[exchangeName]
	queue, qExists := s.queues[queueName]
	s.mux.RUnlock()

	if !exExists {
		return fmt.Errorf("exchange %s does not exist", exchangeName)
	}
	if !qExists {
		return fmt.Errorf("queue %s does not exist", queueName)
	}

	exchange.BindingsMux.Lock()
	defer exchange.BindingsMux.Unlock()

	exchange.Bindings[routingKey] = append(exchange.Bindings[routingKey], queue)
	return nil
}

// publishMessage publishes a message to an exchange and routes it to bound queues.
func (s *AMQPService) publishMessage(exchangeName, routingKey string, message types.Message) error {
	s.mux.RLock()
	exchange, exists := s.exchanges[exchangeName]
	s.mux.RUnlock()
	if !exists {
		return fmt.Errorf("exchange %s does not exist", exchangeName)
	}

	exchange.BindingsMux.RLock()
	defer exchange.BindingsMux.RUnlock()

	switch exchange.Type {
	case types.Fanout:
		for _, queues := range exchange.Bindings {
			for _, queue := range queues {
				s.sendMessage(queue, message)
			}
		}
	case types.Direct:
		if queues, ok := exchange.Bindings[routingKey]; ok {
			for _, queue := range queues {
				s.sendMessage(queue, message)
			}
		}
	case types.Topic:
		for key, queues := range exchange.Bindings {
			if matchesTopic(routingKey, key) {
				for _, queue := range queues {
					s.sendMessage(queue, message)
				}
			}
		}
	default:
		return fmt.Errorf("unsupported exchange type: %v", exchange.Type)
	}
	return nil
}

// consumeQueue reads messages from the specified queue and sends them to the connection.
func (s *AMQPService) consumeQueue(conn net.Conn, queueName string) {
	s.mux.RLock()
	queue, exists := s.queues[queueName]
	s.mux.RUnlock()
	if !exists {
		fmt.Fprintf(conn, "Queue %s does not exist\n", queueName)
		return
	}

	for msg := range queue.Messages {
		fmt.Fprintf(conn, "Consumed message: %s\n", msg.Body)
	}
}

// sendMessage safely sends a message to a queue.
func (s *AMQPService) sendMessage(queue *types.Queue, message types.Message) {
	select {
	case queue.Messages <- message:
	default:
		fmt.Printf("Queue %s is full. Message dropped.\n", queue.Name)
	}
}

// matchesTopic checks if a routing key matches a topic pattern.
func matchesTopic(routingKey, pattern string) bool {
	return routingKey == pattern
}
