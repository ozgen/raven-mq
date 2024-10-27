package main

import (
	"github.com/ozgen/raven-mq/client"
	"log"
	"net/http"
)

func main() {
	// Initialize the ProducerClient with the RavenMQ server URL
	producer := client.NewProducerClient("http://localhost:2122")

	// Define a handler to set up the exchange and queue, then publish a message
	http.HandleFunc("/send", func(w http.ResponseWriter, r *http.Request) {

		// Publish the message to the defined exchange and queue
		err := producer.Publish("example_exchange", "example_key", "Hello from Producer!")
		if err != nil {
			http.Error(w, "Failed to publish message", http.StatusInternalServerError)
			log.Printf("Failed to publish message: %v", err)
			return
		}
		w.Write([]byte("Message published successfully"))
		log.Println("Message published to RavenMQ")
	})

	// Start the producer server on port 8080
	log.Println("Producer server running on port 8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
