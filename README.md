# Raven-MQ

Raven-MQ is a lightweight, in-memory message broker that supports basic AMQP-like features such as exchanges, queues, and message routing. It enables message publishing and consumption with support for various exchange types, including `direct`, `fanout`, and `topic`, along with a reconnection mechanism for reliable message delivery.

## Features
- **Exchanges**: Supports `direct`, `fanout`, and `topic` exchanges for flexible message routing.
- **Queues**: Allows binding queues to exchanges with routing keys.
- **Message Publishing and Consumption**: Enables producers to publish messages to exchanges and consumers to consume messages from queues.
- **Reconnection Support**: Automatically attempts to reconnect and redefine exchanges and queues in case of a connection loss.
- **Duplicate Detection**: Prevents message duplication during consumption (optional).

## Installation

Clone the repository:

```bash
git clone https://github.com/ozgen/raven-mq.git
cd raven-mq
```

Install the necessary Go packages:

```bash
go mod tidy
```

## Components

### 1. `broker` package
This package defines the core message broker, managing exchanges, queues, bindings, and routing.

### 2. `client` package
Includes producer and consumer clients that connect to the broker over TCP to publish and consume messages. Supports reconnection and error handling.

### 3. `types` package
Defines the data structures used for queues, exchanges, and messages.

## Usage

### Running the Broker
To start the Raven-MQ broker, run:

```bash
go run cmd/server/main.go
```

By default, the broker runs on port `2122`.

### Setting Up a Producer
To publish messages to an exchange:

```go
package main

import (
    "log"
    "github.com/ozgen/raven-mq/client"
)

func main() {
    producer, err := client.NewAmqpProducerClient("localhost:2122")
    if err != nil {
        log.Fatalf("Failed to connect to broker: %v", err)
    }
    defer producer.Close()

    err = producer.DefineQueueAndExchange("example_exchange", "direct", "example_queue", "example_key")
    if err != nil {
        log.Fatalf("Failed to define exchange and queue: %v", err)
    }

    err = producer.Publish("example_exchange", "example_key", "Hello from Producer!")
    if err != nil {
        log.Fatalf("Failed to publish message: %v", err)
    }
}
```

### Setting Up a Consumer
To consume messages from a queue:

```go
package main

import (
    "log"
    "github.com/ozgen/raven-mq/client"
)

func main() {
    consumer, err := client.NewAmqpConsumerClient("localhost:2122")
    if err != nil {
        log.Fatalf("Failed to connect to broker: %v", err)
    }
    defer consumer.Close()

    consumer.Consume("example_exchange", "direct", "example_queue", "example_key", func(message string) {
        log.Printf("Consumed message: %s\n", message)
    })
}
```

### Broker Commands
- **DECLARE_EXCHANGE**: Defines an exchange with a specified type.
  ```plaintext
  DECLARE_EXCHANGE <exchange_name> <type>
  ```
- **DECLARE_QUEUE**: Declares a queue.
  ```plaintext
  DECLARE_QUEUE <queue_name>
  ```
- **BIND_QUEUE**: Binds a queue to an exchange with a routing key.
  ```plaintext
  BIND_QUEUE <queue_name> <exchange_name> <routing_key>
  ```
- **PUBLISH**: Publishes a message to an exchange with a routing key.
  ```plaintext
  PUBLISH <exchange_name> <routing_key> <message>
  ```
- **CONSUME**: Consumes messages from a queue.
  ```plaintext
  CONSUME <queue_name>
  ```

## Exchange Types
- **Direct**: Routes messages to queues bound with a matching routing key.
- **Fanout**: Broadcasts messages to all bound queues.
- **Topic**: Routes messages to queues based on pattern matching in routing keys.

## Improvements

The following list includes areas to enhance Raven-MQ's functionality, performance, and reliability. Feel free to check off completed items as they are implemented.

- [ ] **Unit Tests**
    - Implement unit tests for broker functions (e.g., exchange, queue, and message handling).
    - Add tests for producer and consumer client interactions.
    - Ensure coverage for edge cases, including reconnection and error handling.

- [ ] **Persistence and Database Storage**
    - Integrate optional database storage (e.g., PostgreSQL, Redis) for durable queue and message persistence.
    - Implement failover recovery to restore state from persistent storage on restart.

- [ ] **Message Acknowledgment and Redelivery**
    - Add message acknowledgment from the consumer.
    - Implement redelivery mechanism for messages that are not acknowledged.

- [ ] **Logging and Monitoring**
    - Enhance logging with customizable verbosity levels (e.g., debug, info, warn, error).
    - Integrate monitoring for metrics such as message count, queue depth, and consumer lag.

- [ ] **Load Testing and Benchmarking**
    - Set up load tests to benchmark broker performance under high message volume.
    - Identify and address bottlenecks to improve scalability.

- [ ] **Error Handling and Retry Policies**
    - Improve error handling within broker and clients, especially for network issues.
    - Implement retry policies for publishing and consuming messages to handle transient errors gracefully.

- [ ] **Documentation**
    - Expand documentation to include configuration options, examples for advanced usage, and FAQ.
    - Document internal architecture and flow for contributors.

- [ ] **Enhanced Consumer Features**
    - Support multiple consumers per queue with configurable message distribution (e.g., round-robin).
    - Implement consumer groups for more sophisticated load balancing.

- [ ] **Configurable Storage Backend**
    - Enable the broker to support multiple storage backends with a pluggable interface (e.g., memory, file-based, or external databases).

- [ ] **Admin Interface**
    - Add an admin interface (e.g., HTTP API or CLI) to view and manage exchanges, queues, and consumers in real-time.

- [ ] **Security Enhancements**
    - Implement secure connection options (e.g., TLS/SSL).
    - Add authentication and authorization for clients to connect to the broker.

- [ ] **Dockerization**
    - Create a Dockerfile to containerize the broker.
    - Add `docker-compose.yml` for easy setup with optional dependencies (e.g., PostgreSQL for persistence).
    - Ensure environment variables can configure broker settings within the container.
    - Publish the Docker image to a registry for easy access and deployment.

## Contributing

Check out the [Contributing](#contributing) section above if you'd like to work on any of these improvements. Contributions are welcome!

