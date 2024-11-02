package main

import (
	"fmt"
	"github.com/ozgen/raven-mq/broker"
	"log"
	"net"
)

func main() {
	// Initialize the AMQP service, which acts as the broker.
	amqpBroker := broker.NewAMQPBroker()

	// Start a TCP listener on port 2122 for AMQP client connections.
	listener, err := net.Listen("tcp", ":2122")
	if err != nil {
		log.Fatalf("Failed to start server on port 2122: %v", err)
	}
	defer listener.Close()

	fmt.Println("Raven-MQ server running on port 2122...")

	// Accept and handle incoming client connections.
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Failed to accept connection:", err)
			continue
		}

		// Handle each connection in a separate goroutine
		go amqpBroker.HandleConnection(conn)
	}
}
