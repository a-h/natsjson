package natsjson

import (
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go"
)

type Publisher[T any] struct {
	NC *nats.Conn
}

// NewPublisher creates a new publisher.
func NewPublisher[T any](nc *nats.Conn) (p *Publisher[T]) {
	return &Publisher[T]{
		NC: nc,
	}
}

// Publish a message to the given topic in JSON format.
func (p *Publisher[T]) Publish(topic string, v ...T) error {
	for _, vv := range v {
		b, err := json.Marshal(vv)
		if err != nil {
			return fmt.Errorf("failed to marshal message: %w", err)
		}
		err = p.NC.Publish(topic, b)
		if err != nil {
			return fmt.Errorf("failed to publish message: %w", err)
		}
	}
	return nil
}
