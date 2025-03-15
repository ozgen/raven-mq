package broker

import (
	"fmt"
	"github.com/ozgen/raven-mq/internal/tcommon"
	"github.com/ozgen/raven-mq/internal/types"
	"strings"
	"sync"
	"testing"
	"time"
)

// Test DeclareQueue
func TestDeclareQueue(t *testing.T) {
	broker := NewAMQPBroker()

	// Declare a new queue
	err := broker.declareQueue("test_queue")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	// Declare the same queue again, should not return an error (idempotent operation)
	err = broker.declareQueue("test_queue")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	// Ensure queue exists
	if _, exists := broker.queues["test_queue"]; !exists {
		t.Errorf("Queue was not created")
	}
}

// Test DeclareExchange
func TestDeclareExchange(t *testing.T) {
	broker := NewAMQPBroker()

	// Declare valid exchanges
	err := broker.declareExchange("test_exchange", "direct")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	err = broker.declareExchange("test_exchange", "direct") // Should be idempotent
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	// Ensure exchange exists
	if _, exists := broker.exchanges["test_exchange"]; !exists {
		t.Errorf("Exchange was not created")
	}

	// Declare invalid exchange type
	err = broker.declareExchange("invalid_exchange", "invalid")
	if err == nil {
		t.Errorf("Expected error for invalid exchange type, got nil")
	}
}

// Test BindQueue
func TestBindQueue(t *testing.T) {
	broker := NewAMQPBroker()

	// Declare a queue and exchange first
	broker.declareQueue("test_queue")
	broker.declareExchange("test_exchange", "direct")

	// Bind queue to exchange
	err := broker.bindQueue("test_queue", "test_exchange", "test_key")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	// Check if binding exists
	exchange := broker.exchanges["test_exchange"]
	if len(exchange.Bindings["test_key"]) == 0 {
		t.Errorf("Queue was not bound to exchange")
	}

	// Try binding non-existing queue
	err = broker.bindQueue("non_existing_queue", "test_exchange", "test_key")
	if err == nil {
		t.Errorf("Expected error for non-existing queue, got nil")
	}

	// Try binding to a non-existing exchange
	err = broker.bindQueue("test_queue", "non_existing_exchange", "test_key")
	if err == nil {
		t.Errorf("Expected error for non-existing exchange, got nil")
	}
}

// Test PublishMessage
func TestPublishMessage(t *testing.T) {
	broker := NewAMQPBroker()

	// Declare an exchange and queue, then bind them
	broker.declareQueue("test_queue1")
	broker.declareExchange("test_exchange1", "direct")
	broker.bindQueue("test_queue1", "test_exchange1", "test_key1")

	// Publish a message
	message := types.Message{Body: "Hello, World!", RoutingKey: "test_key1"}
	err := broker.publishMessage("test_exchange1", "test_key1", message)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	// Check if message is in queue
	queue := broker.queues["test_queue1"]
	select {
	case msg := <-queue.Messages:
		if msg.Body != "Hello, World!" {
			t.Errorf("Expected message body 'Hello, World!', got '%s'", msg.Body)
		}
	default:
		t.Errorf("Message was not published to queue")
	}
}

// Test ConsumeQueue
func TestConsumeQueue(t *testing.T) {
	broker := NewAMQPBroker()

	// Create a mock connection to capture consumer output
	mockConn := tcommon.NewMockConn("", 0, 50*time.Millisecond)

	// Declare a queue
	queueName := "test_queue2"
	err := broker.declareQueue(queueName)
	if err != nil {
		t.Fatalf("Failed to declare queue: %v", err)
	}

	// Retrieve the queue and add messages
	queue := broker.queues[queueName]
	testMessages := []string{"Hello, World!", "Raven-MQ Test", "Final Message"}

	// Use WaitGroup to track consumer completion
	var wg sync.WaitGroup

	// Start the consumer
	wg.Add(1)
	go func() {
		defer wg.Done() // Ensure this runs when the function exits
		broker.consumeQueue(mockConn, queueName)
	}()

	// Send messages asynchronously
	go func() {
		for _, msg := range testMessages {
			queue.Messages <- types.Message{Body: msg}
		}
		close(queue.Messages) // Close queue to stop consumption
	}()

	// Wait for consumer to finish
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// Add a timeout to prevent infinite waiting
	select {
	case <-done:
		// Consumer finished, check results
		output := mockConn.WriteBuffer.String()

		// Check for "Consumer started" message
		if !strings.Contains(output, "Consumer started") {
			t.Errorf("Expected 'Consumer started' message, but got: %s", output)
		}

		// Check if all messages are consumed correctly
		for _, expectedMsg := range testMessages {
			expectedOutput := fmt.Sprintf("Consumed message: %s\n", expectedMsg)
			if !strings.Contains(output, expectedOutput) {
				t.Errorf("Expected message '%s' not found in output: %s", expectedOutput, output)
			}
		}

		t.Logf("Test completed successfully with output:\n%s", output)

	case <-time.After(2 * time.Second):
		t.Fatal("Test timed out! Consumer likely stuck in an infinite loop.")
	}
}

func TestConsumeQueue_NonExistentQueue(t *testing.T) {
	broker := NewAMQPBroker()
	mockConn := tcommon.NewMockConn("", 0, 50*time.Millisecond)

	// Try to consume from a queue that doesn't exist
	queueName := "non_existent_queue"
	go broker.consumeQueue(mockConn, queueName)

	// Allow processing time
	time.Sleep(50 * time.Millisecond)

	// Read output from the mock connection
	output := mockConn.WriteBuffer.String()

	// Verify error message is returned
	expectedOutput := fmt.Sprintf("Queue %s does not exist\n", queueName)
	if !strings.Contains(output, expectedOutput) {
		t.Errorf("Expected error message '%s', but got: %s", expectedOutput, output)
	}
}

// TestHandleConnection
func TestHandleConnection(t *testing.T) {
	broker := NewAMQPBroker()

	// Simulated client input with multiple commands
	clientInput := `
DECLARE_QUEUE test_queue4
DECLARE_EXCHANGE test_exchange4 direct
BIND_QUEUE test_queue4 test_exchange4 test_key4
PUBLISH test_exchange4 test_key4 Hello, Raven-MQ!
CONSUME test_queue4
UNKNOWN_COMMAND
`

	mockConn := tcommon.NewMockConn(clientInput, 0, 50*time.Millisecond)

	// Run HandleConnection in a separate goroutine
	go broker.HandleConnection(mockConn)

	// Allow processing time
	time.Sleep(100 * time.Millisecond)

	// Get captured output
	output := mockConn.WriteBuffer.String()

	// Check expected responses
	expectedResponses := []string{
		"Queue declared successfully",
		"Exchange declared successfully",
		"Queue bound to exchange successfully",
		"Message published successfully",
		"Consumer started",
		"Unknown command",
	}

	for _, expected := range expectedResponses {
		if !strings.Contains(output, expected) {
			t.Errorf("Expected response '%s' not found in output:\n%s", expected, output)
		}
	}
}
