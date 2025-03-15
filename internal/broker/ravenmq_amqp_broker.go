package broker

import (
	"bufio"
	"fmt"
	"github.com/ozgen/raven-mq/internal/log"
	"github.com/ozgen/raven-mq/internal/providers/db"
	"github.com/ozgen/raven-mq/internal/types"
	"net"
	"strings"
	"sync"
)

type RavenMQAmqpBroker struct {
	queues    map[string]*types.Queue    // Collection of queues
	exchanges map[string]*types.Exchange // Collection of exchanges
	mux       sync.RWMutex               // Mutex for safe access to queues and exchanges
	store     *db.Storage                // sqlite for storing queues, exchanges and messages
}

func NewAMQPBroker() *RavenMQAmqpBroker {
	store, err := db.NewStorage("ravenmq.db")
	if err != nil {
		log.LogCritical("Failed to initialize SQLite: %v", err)
	}
	broker := &RavenMQAmqpBroker{
		queues:    make(map[string]*types.Queue),
		exchanges: make(map[string]*types.Exchange),
		store:     store,
	}

	// Load exchanges and queues from SQLite
	broker.restoreState()
	return broker
}

// Restore exchanges and queues from SQLite on startup.
func (s *RavenMQAmqpBroker) restoreState() {
	s.mux.Lock()
	defer s.mux.Unlock()

	exchanges, _ := s.store.LoadExchanges()
	for name, exType := range exchanges {
		s.exchanges[name] = &types.Exchange{Name: name, Type: types.ExchangeType(exType), Bindings: make(map[string][]*types.Queue)}
	}

	queues, _ := s.store.LoadQueues()
	for _, name := range queues {
		s.queues[name] = &types.Queue{Name: name, Messages: make(chan types.Message, 100)}
	}
}

func (s *RavenMQAmqpBroker) HandleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			log.LogError("Client disconnected or error reading: %v", err)
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
			go s.consumeQueue(conn, queueName)
			fmt.Fprintln(conn, "Consumer started")

		default:
			fmt.Fprintln(conn, "Unknown command")
		}
	}
}

func (s *RavenMQAmqpBroker) declareQueue(name string) error {
	s.mux.Lock()
	defer s.mux.Unlock()

	if _, exists := s.queues[name]; exists {
		log.LogDebug("Queue %s already exists", name)
		return nil
	}

	queue := &types.Queue{
		Name:     name,
		Messages: make(chan types.Message, 100),
	}
	s.store.SaveQueue(name)
	s.queues[name] = queue
	log.LogInfo("Queue '%s' declared successfully.", name)
	return nil
}

func (s *RavenMQAmqpBroker) declareExchange(name, exType string) error {
	s.mux.Lock()
	defer s.mux.Unlock()

	if _, exists := s.exchanges[name]; exists {
		log.LogDebug("Exchange %s already exists", name)
		return nil
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

	err := s.store.SaveExchange(name, exType)
	if err != nil {
		log.LogError("Failed to store exchange %s: %v", name, err)
	}
	s.exchanges[name] = &types.Exchange{
		Name:     name,
		Type:     exchangeType,
		Bindings: make(map[string][]*types.Queue),
	}
	log.LogInfo("Exchange '%s' of type '%s' declared successfully.", name, exType)
	return nil
}

func (s *RavenMQAmqpBroker) bindQueue(queueName, exchangeName, routingKey string) error {
	s.mux.RLock()
	exchange, exExists := s.exchanges[exchangeName]
	queue, qExists := s.queues[queueName]
	s.mux.RUnlock()

	if !exExists {
		return fmt.Errorf("Exchange %s does not exist", exchangeName)
	}
	if !qExists {
		return fmt.Errorf("Queue %s does not exist", queueName)
	}

	exchange.BindingsMux.Lock()
	defer exchange.BindingsMux.Unlock()

	// Check if the binding already exists to avoid redundant bindings
	for _, boundQueue := range exchange.Bindings[routingKey] {
		if boundQueue == queue {
			log.LogDebug("Queue '%s' is already bound to exchange '%s' with routing key '%s'", queueName, exchangeName, routingKey)
			return nil
		}
	}

	exchange.Bindings[routingKey] = append(exchange.Bindings[routingKey], queue)
	log.LogInfo("Queue '%s' bound to exchange '%s' with routing key '%s'", queueName, exchangeName, routingKey)
	return nil
}

func (s *RavenMQAmqpBroker) publishMessage(exchangeName, routingKey string, message types.Message) error {
	s.mux.RLock()
	exchange, exists := s.exchanges[exchangeName]
	s.mux.RUnlock()
	if !exists {
		return fmt.Errorf("Exchange %s does not exist", exchangeName)
	}

	exchange.BindingsMux.RLock()
	defer exchange.BindingsMux.RUnlock()

	switch exchange.Type {
	case types.Fanout:
		for _, queues := range exchange.Bindings {
			for _, queue := range queues {
				// Store messages per queue (instead of the exchange)
				err := s.store.SaveMessage(queue.Name, message.Body, routingKey)
				if err != nil {
					log.LogError("Failed to store message %s for queue %s: %v", message.Body, queue.Name, err)
				}
				s.sendMessageToQueue(queue, message)
			}
		}
	case types.Direct:
		if queues, ok := exchange.Bindings[routingKey]; ok {
			for _, queue := range queues {
				// Store messages per queue (instead of the exchange)
				err := s.store.SaveMessage(queue.Name, message.Body, routingKey)
				if err != nil {
					log.LogError("Failed to store message %s for queue %s: %v", message.Body, queue.Name, err)
				}
				s.sendMessageToQueue(queue, message)
			}
		}
	case types.Topic:
		for key, queues := range exchange.Bindings {
			if matchesTopic2(routingKey, key) {
				for _, queue := range queues {
					// Store messages per queue (instead of the exchange)
					err := s.store.SaveMessage(queue.Name, message.Body, routingKey)
					if err != nil {
						log.LogError("Failed to store message %s for queue %s: %v", message.Body, queue.Name, err)
					}
					s.sendMessageToQueue(queue, message)
				}
			}
		}
	default:
		return fmt.Errorf("Unsupported exchange type: %v", exchange.Type)
	}
	return nil
}

func (s *RavenMQAmqpBroker) sendMessageToQueue(queue *types.Queue, message types.Message) {
	select {
	case queue.Messages <- message:
		log.LogDebug("Message '%s' sent to queue '%s'", message.Body, queue.Name)
	default:
		log.LogDebug("Queue %s is full. Message dropped.", queue.Name)
	}
}

func (s *RavenMQAmqpBroker) consumeQueue(conn net.Conn, queueName string) {
	s.mux.RLock()
	queue, exists := s.queues[queueName]
	s.mux.RUnlock()

	if !exists {
		fmt.Fprintf(conn, "Queue %s does not exist\n", queueName)
		return
	}

	fmt.Fprintf(conn, "Consumer started\n")
	queue.ConsumeMux.Lock()
	defer queue.ConsumeMux.Unlock()

	for msg := range queue.Messages {
		log.LogDebug("Dispatching message to consumer: %s", msg.Body)
		err := s.store.DeleteMessage(queue.Name, msg.Body)
		if err != nil {
			log.LogError("Failed to delete message %s for queue %s: %v", msg.Body, queue.Name, err)
		}
		_, err = fmt.Fprintf(conn, "Consumed message: %s\n", msg.Body)
		if err != nil {
			log.LogDebug("Error sending message to consumer: %v", err)
			return
		}
	}
}

func matchesTopic2(routingKey, pattern string) bool {
	return routingKey == pattern
}
