package client

import (
	"fmt"
	"math"
	"net"
	"time"
)

// ReconnectionPolicy defines the settings for exponential reconnection attempts.
type ReconnectionPolicy struct {
	maxRetries   int
	baseInterval time.Duration
	maxInterval  time.Duration
}

// Default values for the ReconnectionPolicy
const (
	defaultMaxRetries   = 10
	defaultBaseInterval = 1 * time.Second
	defaultMaxInterval  = 10 * time.Second
)

// NewReconnectionPolicy initializes a new ReconnectionPolicy with customizable settings.
// If a value is set to zero, it will default to a predefined value.
func NewReconnectionPolicy(maxRetries int, baseInterval, maxInterval time.Duration) *ReconnectionPolicy {
	if maxRetries == 0 {
		maxRetries = defaultMaxRetries
	}
	if baseInterval == 0 {
		baseInterval = defaultBaseInterval
	}
	if maxInterval == 0 {
		maxInterval = defaultMaxInterval
	}
	return &ReconnectionPolicy{
		maxRetries:   maxRetries,
		baseInterval: baseInterval,
		maxInterval:  maxInterval,
	}
}

// NewDefaultReconnectionPolicy initializes a ReconnectionPolicy with default values.
func NewDefaultReconnectionPolicy() *ReconnectionPolicy {
	return NewReconnectionPolicy(defaultMaxRetries, defaultBaseInterval, defaultMaxInterval)
}

// ExecuteWithReconnect attempts to reconnect using exponential backoff.
func (r *ReconnectionPolicy) ExecuteWithReconnect(connectFunc func() (net.Conn, error)) (net.Conn, error) {
	var conn net.Conn
	var err error

	for i := 0; i < r.maxRetries; i++ {
		conn, err = connectFunc()
		if err == nil {
			return conn, nil
		}

		// Calculate exponential backoff with a cap at maxInterval
		backoff := time.Duration(math.Min(float64(r.baseInterval)*math.Pow(2, float64(i)), float64(r.maxInterval)))
		time.Sleep(backoff)
	}

	return nil, fmt.Errorf("failed to reconnect after %d retries: %w", r.maxRetries, err)
}
