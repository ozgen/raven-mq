package tcommon

import (
	"bytes"
	"errors"
	"net"
	"sync"
	"time"
)

// MockConn simulates a broker connection with controlled behavior.
type MockConn struct {
	ReadBuffer    *bytes.Buffer
	WriteBuffer   *bytes.Buffer
	Closed        bool
	ExchangeExist bool
	mu            sync.Mutex
	messageCount  int
	MaxMessages   int
	SimulateDelay time.Duration
	stopped       bool
}

func NewMockConn(response string, maxMessages int, simulateDelay time.Duration) *MockConn {
	return &MockConn{
		ReadBuffer:    bytes.NewBufferString(response),
		WriteBuffer:   &bytes.Buffer{},
		ExchangeExist: true,
		MaxMessages:   maxMessages,
		SimulateDelay: simulateDelay,
		stopped:       false,
	}
}

// Implement net.Conn
func (m *MockConn) Read(b []byte) (n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Simulate network delay
	if m.SimulateDelay > 0 {
		time.Sleep(m.SimulateDelay)
	}

	// Send messages before stopping
	if !m.stopped && bytes.Contains(m.WriteBuffer.Bytes(), []byte("CONSUME")) {
		if m.messageCount < m.MaxMessages {
			m.ReadBuffer.WriteString("Consumed message: Hello, World!\n")
			m.messageCount++
		} else {
			m.ReadBuffer.WriteString("Consumer stopped\n") // Stop message
			m.stopped = true
		}
	}

	// If the consumer stopped and the buffer is empty, return EOF
	if m.stopped && m.ReadBuffer.Len() == 0 {
		return 0, errors.New("EOF")
	}

	return m.ReadBuffer.Read(b)
}

func (m *MockConn) Write(b []byte) (n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.WriteBuffer.Write(b)

	// Simulate broker responses
	if bytes.Contains(b, []byte("DECLARE_EXCHANGE")) {
		m.ExchangeExist = true
		m.ReadBuffer.WriteString("Exchange declared successfully\n")
	} else if bytes.Contains(b, []byte("DECLARE_QUEUE")) {
		m.ReadBuffer.WriteString("Queue declared successfully\n")
	} else if bytes.Contains(b, []byte("BIND_QUEUE")) {
		if !m.ExchangeExist {
			m.ReadBuffer.WriteString("Error: Exchange does not exist\n")
		} else {
			m.ReadBuffer.WriteString("Queue bound to exchange successfully\n")
		}
	}

	return len(b), nil
}

func (m *MockConn) Close() error {
	m.Closed = true
	return nil
}
func (m *MockConn) LocalAddr() net.Addr                { return nil }
func (m *MockConn) RemoteAddr() net.Addr               { return nil }
func (m *MockConn) SetDeadline(t time.Time) error      { return nil }
func (m *MockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *MockConn) SetWriteDeadline(t time.Time) error { return nil }
