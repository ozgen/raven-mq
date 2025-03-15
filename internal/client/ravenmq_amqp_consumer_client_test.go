package client

import (
	"github.com/ozgen/raven-mq/internal/tcommon"
	"strings"
	"testing"
)

// TestNewAmqpConsumerClient
func TestNewAmqpConsumerClient(t *testing.T) {
	mockConn := tcommon.NewMockConn("Connected\n", 0, 0)
	reconnectPolicy := NewDefaultReconnectionPolicy()

	client, err := NewAmqpConsumerClientWithPolicy("127.0.0.1:2122", reconnectPolicy)
	if err != nil {
		t.Fatalf("Failed to create consumer client: %v", err)
	}

	client.Conn = mockConn
	if client.Conn == nil {
		t.Errorf("Expected a valid connection, got nil")
	}
}

// Test DefineQueueAndExchange
func TestDefineQueueAndExchange(t *testing.T) {
	mockConn := tcommon.NewMockConn("", 0, 0)
	client := &RavenMQAmqpConsumerClient{
		brokerAddr:      "127.0.0.1:2122",
		Conn:            mockConn,
		recentMessages:  make(map[string]struct{}),
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

// Test Reconnect on Failure
func TestReconnect(t *testing.T) {
	mockConn := tcommon.NewMockConn("", 0, 0)
	client := &RavenMQAmqpConsumerClient{
		brokerAddr:      "127.0.0.1:2122",
		Conn:            mockConn,
		recentMessages:  make(map[string]struct{}),
		reconnectPolicy: NewDefaultReconnectionPolicy(),
	}

	client.Conn.Close() // Simulate connection failure
	err := client.reconnect()
	if err != nil {
		t.Errorf("Expected reconnect to succeed, got error: %v", err)
	}
}
