package factory

import (
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
)

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	queueMiddleware, err := NewQueueMiddleware(queueName, connectionSettings)
	if err != nil {
		return nil, err
	}

	return queueMiddleware, nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	exchageMiddleware, err := NewExchangeMiddleware(exchange, keys, connectionSettings)
	if err != nil {
		return nil, err
	}

	return exchageMiddleware, nil
}
