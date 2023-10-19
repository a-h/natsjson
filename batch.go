package natsjson

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

func NewBatchProcessor[T any](consumer jetstream.Consumer, batchSize int, processor func(ctx context.Context, messages []T) []error, fetchOpts ...jetstream.FetchOpt) *BatchProcessor[T] {
	return &BatchProcessor[T]{
		consumer:  consumer,
		batchSize: batchSize,
		processor: processor,
		fetchOpts: fetchOpts,
	}
}

type BatchProcessor[T any] struct {
	consumer     jetstream.Consumer
	batchSize    int
	processor    func(ctx context.Context, messages []T) []error
	fetchOpts    []jetstream.FetchOpt
	ErrorHandler func(msg T, err error)
}

func (b *BatchProcessor[T]) Process(ctx context.Context) (err error) {
	// Fetch a batch.
	mb, err := b.consumer.Fetch(b.batchSize, b.fetchOpts...)
	if err != nil {
		return fmt.Errorf("failed to fetch: %w", err)
	}

	// Convert JSON messages to type.
	var msgBodies []T
	var msgs []jetstream.Msg
	for msg := range mb.Messages() {
		var fr T
		if err := json.Unmarshal(msg.Data(), &fr); err != nil {
			unmarshalErr := fmt.Errorf("failed to unmarshal, skipping invalid message: %v", err)
			if ackErr := msg.Ack(); ackErr != nil {
				return errors.Join(unmarshalErr, ackErr)
			}
			continue
		}
		msgBodies = append(msgBodies, fr)
		msgs = append(msgs, msg)
	}
	if len(msgs) == 0 {
		return nil
	}

	// Process messages.
	errs := b.processor(ctx, msgBodies)
	if len(errs) != len(msgs) {
		return fmt.Errorf("expected a slice of %d errors - one for each msg, but got %d", len(msgs), len(errs))
	}

	// Ack or nack messages based on their error state.
	nackAckErrs := make([]error, len(errs))
	for i, err := range errs {
		op := msgs[i].Ack
		if err != nil {
			// Call the error handler hook.
			if b.ErrorHandler != nil {
				b.ErrorHandler(msgBodies[i], err)
			}
			op = msgs[i].Nak
		}
		nackAckErrs[i] = op()
	}
	return errors.Join(nackAckErrs...)
}
