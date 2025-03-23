package common

const (
	// Keywords for broker responses
	SuccessKeyword   = "successfully"
	ConsumerStarted  = "Consumer started"
	MessagePublished = "Message published"

	// Broker commands
	CmdDeclareExchange = "DECLARE_EXCHANGE"
	CmdDeclareQueue    = "DECLARE_QUEUE"
	CmdBindQueue       = "BIND_QUEUE"
	CmdPublish         = "PUBLISH"
	CmdConsume         = "CONSUME"
	CmdAck             = "ACK"

	// Broker messages
	MsgQueueDeclared    = "Queue declared successfully"
	MsgExchangeDeclared = "Exchange declared successfully"
	MsgQueueBound       = "Queue bound to exchange successfully"
	MsgPublished        = "Message published successfully"
	MsgAck              = "ACK successfully"
	MsgConsumerStarted  = "Consumer started"
	MsgConsumerStopped  = "Consumer stopped"

	// Misc
	ConsumedMessagePrefix = "Consumed message: "
	QueueNotFoundPrefix   = "Queue does not exist"
)
