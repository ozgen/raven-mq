package main

import (
	"fmt"
	"github.com/ozgen/raven-mq/broker"
	"github.com/ozgen/raven-mq/service"
	"log"
	"net/http"
)

func main() {
	// Initialize the broker
	b := broker.NewBroker()

	// Initialize the broker service with HTTP endpoints
	brokerService := service.NewBrokerService(b)
	brokerService.RegisterRoutes()

	// Start the HTTP server on port 2122
	fmt.Println("Message broker server running on port 2122...")
	if err := http.ListenAndServe(":2122", nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
