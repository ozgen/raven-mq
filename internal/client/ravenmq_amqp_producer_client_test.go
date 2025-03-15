package client

import (
	"github.com/ozgen/raven-mq/internal/tcommon"
	"strings"
	"testing"
)

// Test NewAmqpProducerClient
func TestNewAmqpProducerClient(t *testing.T) {
	mockConn := tcommon.NewMockConn("Connected\n", 0, 0)
	reconnectPolicy := NewDefaultReconnectionPolicy()

	client, err := NewAmqpProducerClientWithPolicy("127.0.0.1:2122", reconnectPolicy)
	if err != nil {
		t.Fatalf("Failed to create producer client: %v", err)
	}

	client.conn = mockConn
	if client.conn == nil {
		t.Errorf("Expected a valid connection, got nil")
	}
}

// Test DefineQueueAndExchange
func TestPeoducerDefineQueueAndExchange(t *testing.T) {
	mockConn := tcommon.NewMockConn("", 0, 0)
	client := &RavenMQAmqpProducerClient{
		brokerAddr:      "127.0.0.1:2122",
		conn:            mockConn,
		reconnectPolicy: NewDefaultReconnectionPolicy(),
	}

	err := client.DefineQueueAndExchange("test_exchange", "direct", "test_queue", "test_key")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	expectedCommands := []string{
		"DECLARE_EXCHANGE test_exchange direct",
		"DECLARE_QUEUE test_queue",
		"BIND_QUEUE test_queue test_exchange test_key",
	}

	for _, cmd := range expectedCommands {
		if !strings.Contains(mockConn.WriteBuffer.String(), cmd) {
			t.Errorf("Expected command '%s' to be sent to broker", cmd)
		}
	}
}

// Test Publish Function
func TestPublish(t *testing.T) {
	mockConn := tcommon.NewMockConn("Message published successfully\n", 0, 0)
	client := &RavenMQAmqpProducerClient{
		brokerAddr:      "127.0.0.1:2122",
		conn:            mockConn,
		reconnectPolicy: NewDefaultReconnectionPolicy(),
	}

	err := client.Publish("test_exchange", "test_key", "Hello, World!")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	expectedCommand := "PUBLISH test_exchange test_key Hello, World!"
	if !strings.Contains(mockConn.WriteBuffer.String(), expectedCommand) {
		t.Errorf("Expected command '%s' to be sent to broker", expectedCommand)
	}
}

// Test Reconnect Handling
func TestProducerReconnect(t *testing.T) {
	mockConn := tcommon.NewMockConn("", 0, 0)
	client := &RavenMQAmqpProducerClient{
		brokerAddr:      "127.0.0.1:2122",
		conn:            mockConn,
		reconnectPolicy: NewDefaultReconnectionPolicy(),
	}

	client.conn.Close() // Simulate connection failure
	err := client.reconnect()
	if err != nil {
		t.Errorf("Expected reconnect to succeed, got error: %v", err)
	}
}
