package factory

import (
	"context"
	"fmt"
	"sync"

	middleware "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	_QUEUE_DURABLE     = true
	_QUEUE_AUTO_DELETE = false
	_QUEUE_EXCLUSIVE   = false
	_QUEUE_NO_WAIT     = false

	_CONSUMER_AUTO_ACK  = false
	_CONSUMER_EXCLUSIVE = false
	_CONSUMER_NO_LOCAL  = false
	_CONSUMER_NO_WAIT   = false

	_CANCEL_NO_WAIT = false

	_DEFAULT_EXCHANGE  = ""
	_PUBLISH_MANDATORY = false
	_PUBLISH_IMMEDIATE = false

	_SINGLE_MESSAGE  = false
	_REQUEUE_ON_NACK = true
)

type QueueMiddleware struct {
	queue   amqp.Queue
	conn    *amqp.Connection
	channel *amqp.Channel

	mutex              sync.Mutex
	consumerSeq        uint64
	consumerTag        string
	currentlyConsuming bool
}

func NewQueueMiddleware(queueName string, connectionSettings middleware.ConnSettings) (middleware.Middleware, error) {
	uri := GetURI(connectionSettings.Hostname, connectionSettings.Port)

	conn, err := amqp.Dial(uri)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	args := getAmqpTable()
	q, err := ch.QueueDeclare(
		queueName,
		_QUEUE_DURABLE,
		_QUEUE_AUTO_DELETE,
		_QUEUE_EXCLUSIVE,
		_QUEUE_NO_WAIT,
		args,
	)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &QueueMiddleware{
		conn:               conn,
		queue:              q,
		channel:            ch,
		currentlyConsuming: false,
	}, nil
}

func (qm *QueueMiddleware) StartConsuming(callbackFunc func(msg middleware.Message, ack func(), nack func())) error {
	if qm.conn.IsClosed() {
		return middleware.ErrMessageMiddlewareDisconnected
	}

	consumerTag := qm.nextConsumerTag()

	deliveriesChan, err := qm.consume(consumerTag)
	if err != nil {
		return middleware.ErrMessageMiddlewareMessage
	}

	qm.setConsuming(consumerTag)

	for delivery := range deliveriesChan {
		ackHandler, nackHandler := newDeliveryHandlers(delivery)
		callbackFunc(createMsg(delivery.Body), ackHandler, nackHandler)
	}

	qm.clearConsuming()

	return nil
}

func (qm *QueueMiddleware) StopConsuming() error {
	if qm.conn.IsClosed() {
		return middleware.ErrMessageMiddlewareDisconnected
	}

	consumerTag, wasConsuming := qm.takeConsumerTag()
	if !wasConsuming {
		return nil
	}

	if err := qm.channel.Cancel(consumerTag, _CANCEL_NO_WAIT); err != nil {
		return middleware.ErrMessageMiddlewareMessage
	}

	return nil
}

func (qm *QueueMiddleware) Send(msg middleware.Message) error {
	if qm.conn.IsClosed() {
		return middleware.ErrMessageMiddlewareDisconnected
	}

	routingKey := qm.queue.Name
	publishMsg := getPublishMessage(msg.Body)

	err := qm.channel.PublishWithContext(
		context.Background(),
		_DEFAULT_EXCHANGE,
		routingKey,
		_PUBLISH_MANDATORY,
		_PUBLISH_IMMEDIATE,
		publishMsg,
	)
	if err != nil {
		return middleware.ErrMessageMiddlewareMessage
	}

	return nil
}

func (qm *QueueMiddleware) Close() error {
	err := qm.conn.Close()
	if err != nil {
		return middleware.ErrMessageMiddlewareClose
	}

	return nil
}

func (qm *QueueMiddleware) consume(consumerTag string) (<-chan amqp.Delivery, error) {
	noArgs := amqp.Table{}

	return qm.channel.Consume(
		qm.queue.Name,
		consumerTag,
		_CONSUMER_AUTO_ACK,
		_CONSUMER_EXCLUSIVE,
		_CONSUMER_NO_LOCAL,
		_CONSUMER_NO_WAIT,
		noArgs,
	)
}

func (qm *QueueMiddleware) nextConsumerTag() string {
	qm.mutex.Lock()
	defer qm.mutex.Unlock()

	qm.consumerSeq++
	consumerTag := fmt.Sprintf("%s-consumer-%d", qm.queue.Name, qm.consumerSeq)

	return consumerTag
}

func (qm *QueueMiddleware) setConsuming(consumerTag string) {
	qm.mutex.Lock()
	defer qm.mutex.Unlock()

	qm.consumerTag = consumerTag
	qm.currentlyConsuming = true
}

func (qm *QueueMiddleware) clearConsuming() {
	qm.mutex.Lock()
	defer qm.mutex.Unlock()

	qm.currentlyConsuming = false
}

func (qm *QueueMiddleware) takeConsumerTag() (consumerTag string, wasConsuming bool) {
	qm.mutex.Lock()
	defer qm.mutex.Unlock()

	if !qm.currentlyConsuming {
		return "", false
	}

	qm.currentlyConsuming = false

	return qm.consumerTag, true
}

func getAmqpTable() amqp.Table {
	return amqp.Table{
		amqp.QueueTypeArg: amqp.QueueTypeClassic,
	}
}

func newDeliveryHandlers(delivery amqp.Delivery) (ackHandler func(), nackHandler func()) {
	ackHandler = func() { delivery.Ack(_SINGLE_MESSAGE) }
	nackHandler = func() { delivery.Nack(_SINGLE_MESSAGE, _REQUEUE_ON_NACK) }

	return ackHandler, nackHandler
}

func createMsg(body []byte) middleware.Message {
	bodyAsString := string(body)

	return middleware.Message{Body: bodyAsString}
}

func getPublishMessage(body string) amqp.Publishing {
	return amqp.Publishing{
		ContentType:  "text/plain",
		DeliveryMode: amqp.Persistent,
		Body:         []byte(body),
	}
}
