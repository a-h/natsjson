package natsjson

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/nats-io/nats.go/jetstream"
)

type BatchProcessorOpt[T any] func(*BatchProcessor[T])

func WithLogger[T any](log *slog.Logger) BatchProcessorOpt[T] {
	return func(bp *BatchProcessor[T]) {
		bp.Log = log
	}
}

func WithFetchOpts[T any](opts ...jetstream.FetchOpt) BatchProcessorOpt[T] {
	return func(bp *BatchProcessor[T]) {
		bp.fetchOpts = append(bp.fetchOpts, opts...)
	}
}

func NewBatchProcessor[T any](consumer jetstream.Consumer, batchSize int, processor func(ctx context.Context, messages []T) []error, opts ...BatchProcessorOpt[T]) *BatchProcessor[T] {
	bp := &BatchProcessor[T]{
		consumer:  consumer,
		batchSize: batchSize,
		processor: processor,
	}
	for _, opt := range opts {
		opt(bp)
	}
	if bp.Log == nil {
		bp.Log = slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{
			AddSource: true,
			Level:     slog.LevelError,
		}))
	}
	return bp
}

type BatchProcessor[T any] struct {
	Log          *slog.Logger
	consumer     jetstream.Consumer
	batchSize    int
	processor    func(ctx context.Context, messages []T) []error
	fetchOpts    []jetstream.FetchOpt
	ErrorHandler func(msg T, err error)
}

func (b *BatchProcessor[T]) Process(ctx context.Context) (err error) {
	// Fetch a batch.
	b.Log.Debug("Fetching batch")
	mb, err := b.consumer.Fetch(b.batchSize, b.fetchOpts...)
	if err != nil {
		return fmt.Errorf("failed to fetch: %w", err)
	}

	// Convert JSON messages to type.
	b.Log.Debug("Reading messages")
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
		b.Log.Debug("No messages, returning")
		return nil
	}

	// Process messages.
	b.Log.Debug("Processing messages", slog.Int("count", len(msgs)))
	errs := b.processor(ctx, msgBodies)
	if len(errs) != len(msgs) {
		return fmt.Errorf("expected a slice of %d errors - one for each msg, but got %d", len(msgs), len(errs))
	}

	// Ack or nack messages based on their error state.
	var errCount int
	b.Log.Debug("Acknowledging messages", slog.Int("count", len(msgs)))
	nackAckErrs := make([]error, len(errs))
	for i, err := range errs {
		op := msgs[i].Ack
		if err != nil {
			b.Log.Warn("Error processing message", slog.Any("error", err))
			errCount++
			// Call the error handler hook.
			if b.ErrorHandler != nil {
				b.ErrorHandler(msgBodies[i], err)
			}
			op = msgs[i].Nak
		}
		nackAckErrs[i] = op()
	}
	b.Log.Debug("Acknowledged messages", slog.Int("acks", len(msgs)-errCount), slog.Int("nacks", errCount))
	return errors.Join(nackAckErrs...)
}
