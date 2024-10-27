package main

//
//import (
//	"fmt"
//	"github.com/ozgen/raven-mq/client"
//	"log"
//	"net/http"
//	"sync"
//)
//
//// ConsumerService manages the consumer's state and operations.
//type ConsumerService struct {
//	client          *client.ConsumerClient
//	consumerStarted bool
//	mu              sync.Mutex
//}
//
//// NewConsumerService initializes and returns a new ConsumerService.
//func NewConsumerService(brokerURL string) *ConsumerService {
//	return &ConsumerService{
//		client: client.NewConsumerClient(brokerURL),
//	}
//}
//
//// Start initializes the queue and exchange, then begins consuming messages if not already running.
//func (s *ConsumerService) Start() error {
//	s.mu.Lock()
//	defer s.mu.Unlock()
//
//	if s.consumerStarted {
//		return fmt.Errorf("consumer is already running")
//	}
//
//	// Define the necessary exchange and queue on RavenMQ
//	err := s.client.DefineQueueAndExchange("example_exchange", "direct", "example_queue", "example_key")
//	if err != nil {
//		log.Printf("Failed to define exchange and queue: %v", err)
//		return err
//	}
//	log.Println("Exchange and queue defined successfully")
//
//	// Start consuming messages in a goroutine
//	go func() {
//		err := s.client.Consume("example_queue", func(message string) {
//			fmt.Println("Consumed message:", message)
//		})
//		if err != nil {
//			log.Printf("Failed to consume messages: %v", err)
//		}
//	}()
//
//	s.consumerStarted = true
//	return nil
//}
//
//func main() {
//	// Initialize the consumer service with the RavenMQ server URL
//	consumerService := NewConsumerService("http://localhost:2122")
//
//	// Define the /start handler
//	http.HandleFunc("/start", func(w http.ResponseWriter, r *http.Request) {
//		err := consumerService.Start()
//		if err != nil {
//			http.Error(w, err.Error(), http.StatusConflict)
//			return
//		}
//		w.Write([]byte("Consumer started"))
//		log.Println("Consumer started and listening for messages")
//	})
//
//	// Start the consumer server on port 8081
//	log.Println("Consumer server running on port 8081...")
//	log.Fatal(http.ListenAndServe(":8081", nil))
//}
