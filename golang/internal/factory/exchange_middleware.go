package factory

import (
	"context"
	"fmt"
	"sync"

	middleware "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	_EXCHANGE_KIND        = "direct"
	_EXCHANGE_DURABLE     = true
	_EXCHANGE_AUTO_DELETE = false
	_EXCHANGE_INTERNAL    = false
	_EXCHANGE_NO_WAIT     = false

	_ANON_QUEUE_NAME        = ""
	_ANON_QUEUE_DURABLE     = false
	_ANON_QUEUE_AUTO_DELETE = true
	_ANON_QUEUE_EXCLUSIVE   = true
	_ANON_QUEUE_NO_WAIT     = false

	_BIND_NO_WAIT = false
)

type ExchangeMiddleware struct {
	exchange string
	keys     []string
	conn     *amqp.Connection
	channel  *amqp.Channel

	mutex              sync.Mutex
	consumerSeq        uint64
	consumerTag        string
	currentlyConsuming bool
}

func NewExchangeMiddleware(exchange string, keys []string, connectionSettings middleware.ConnSettings) (middleware.Middleware, error) {
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

	noArgs := amqp.Table{}
	err = ch.ExchangeDeclare(
		exchange,
		_EXCHANGE_KIND,
		_EXCHANGE_DURABLE,
		_EXCHANGE_AUTO_DELETE,
		_EXCHANGE_INTERNAL,
		_EXCHANGE_NO_WAIT,
		noArgs,
	)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &ExchangeMiddleware{
		exchange:           exchange,
		keys:               keys,
		conn:               conn,
		channel:            ch,
		currentlyConsuming: false,
	}, nil
}

func (em *ExchangeMiddleware) StartConsuming(callbackFunc func(msg middleware.Message, ack func(), nack func())) error {
	if em.conn.IsClosed() {
		return middleware.ErrMessageMiddlewareDisconnected
	}

	queue, err := em.declareAnonymousQueue()
	if err != nil {
		return middleware.ErrMessageMiddlewareMessage
	}

	if err := em.bindKeys(queue.Name); err != nil {
		return middleware.ErrMessageMiddlewareMessage
	}

	consumerTag := em.nextConsumerTag(queue.Name)

	deliveriesChan, err := em.consume(queue.Name, consumerTag)
	if err != nil {
		return middleware.ErrMessageMiddlewareMessage
	}

	em.setConsuming(consumerTag)

	for delivery := range deliveriesChan {
		ackHandler, nackHandler := newDeliveryHandlers(delivery)
		callbackFunc(createMsg(delivery.Body), ackHandler, nackHandler)
	}

	em.clearConsuming()

	return nil
}

func (em *ExchangeMiddleware) StopConsuming() error {
	if em.conn.IsClosed() {
		return middleware.ErrMessageMiddlewareDisconnected
	}

	consumerTag, wasConsuming := em.takeConsumerTag()
	if !wasConsuming {
		return nil
	}

	if err := em.channel.Cancel(consumerTag, _CANCEL_NO_WAIT); err != nil {
		return middleware.ErrMessageMiddlewareMessage
	}

	return nil
}

func (em *ExchangeMiddleware) Send(msg middleware.Message) error {
	if em.conn.IsClosed() {
		return middleware.ErrMessageMiddlewareDisconnected
	}

	publishMsg := getPublishMessage(msg.Body)

	for _, routingKey := range em.keys {
		err := em.channel.PublishWithContext(
			context.Background(),
			em.exchange,
			routingKey,
			_PUBLISH_MANDATORY,
			_PUBLISH_IMMEDIATE,
			publishMsg,
		)
		if err != nil {
			return middleware.ErrMessageMiddlewareMessage
		}
	}

	return nil
}

func (em *ExchangeMiddleware) Close() error {
	err := em.conn.Close()
	if err != nil {
		return middleware.ErrMessageMiddlewareClose
	}

	return nil
}

func (em *ExchangeMiddleware) declareAnonymousQueue() (amqp.Queue, error) {
	noArgs := amqp.Table{}

	return em.channel.QueueDeclare(
		_ANON_QUEUE_NAME,
		_ANON_QUEUE_DURABLE,
		_ANON_QUEUE_AUTO_DELETE,
		_ANON_QUEUE_EXCLUSIVE,
		_ANON_QUEUE_NO_WAIT,
		noArgs,
	)
}

func (em *ExchangeMiddleware) bindKeys(queueName string) error {
	noArgs := amqp.Table{}

	for _, key := range em.keys {
		err := em.channel.QueueBind(queueName, key, em.exchange, _BIND_NO_WAIT, noArgs)
		if err != nil {
			return err
		}
	}

	return nil
}

func (em *ExchangeMiddleware) consume(queueName string, consumerTag string) (<-chan amqp.Delivery, error) {
	noArgs := amqp.Table{}

	return em.channel.Consume(
		queueName,
		consumerTag,
		_CONSUMER_AUTO_ACK,
		_CONSUMER_EXCLUSIVE,
		_CONSUMER_NO_LOCAL,
		_CONSUMER_NO_WAIT,
		noArgs,
	)
}

func (em *ExchangeMiddleware) nextConsumerTag(queueName string) string {
	em.mutex.Lock()
	defer em.mutex.Unlock()

	em.consumerSeq++
	consumerTag := fmt.Sprintf("%s-consumer-%d", queueName, em.consumerSeq)

	return consumerTag
}

func (em *ExchangeMiddleware) setConsuming(consumerTag string) {
	em.mutex.Lock()
	defer em.mutex.Unlock()

	em.consumerTag = consumerTag
	em.currentlyConsuming = true
}

func (em *ExchangeMiddleware) clearConsuming() {
	em.mutex.Lock()
	defer em.mutex.Unlock()

	em.currentlyConsuming = false
}

func (em *ExchangeMiddleware) takeConsumerTag() (consumerTag string, wasConsuming bool) {
	em.mutex.Lock()
	defer em.mutex.Unlock()

	if !em.currentlyConsuming {
		return "", false
	}

	em.currentlyConsuming = false

	return em.consumerTag, true
}
