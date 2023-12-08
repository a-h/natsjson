package natsjson

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go/jetstream"
)

type BatchMessage struct {
	Index int
}

func TestBatchProcessor(t *testing.T) {
	// Arrange.
	conn, js, shutdown, err := NewInProcessNATSServer()
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown()
	ctx := context.Background()

	// Create a stream.
	streamName := "test_stream"
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{"*"},
		Storage:  jetstream.MemoryStorage, // For speed in tests.
	})
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	// Create a durable consumer.
	consumer, err := js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		Durable:       "testBatchProcessor",
		MemoryStorage: true, // For speed in tests.
	})
	if err != nil {
		t.Fatalf("unexpected failure creating or updating consumer: %v", err)
	}

	log := slog.New(slog.NewJSONHandler(io.Discard, nil))

	t.Run("if no messages are present in the stream, the processor function is not called", func(t *testing.T) {
		p := func(ctx context.Context, msgs []BatchMessage) []error {
			t.Errorf("unexpected call to process function")
			return make([]error, len(msgs))
		}
		bp := NewBatchProcessor[BatchMessage](consumer, 10, p, WithFetchOpts[BatchMessage](jetstream.FetchMaxWait(time.Millisecond)), WithLogger[BatchMessage](log))
		if err := bp.Process(ctx); err != nil {
			t.Fatalf("unexpected error processing batch: %v", err)
		}
	})
	t.Run("the batch processor receives batches of messages", func(t *testing.T) {
		// Arrange.
		// Publish 25 messages into the stream. We should get 2 x batches of 10, then 1 x batch of 5.
		expectedBatches := []int{10, 10, 5}
		var expected []BatchMessage
		for i := 0; i < 25; i++ {
			expected = append(expected, BatchMessage{
				Index: i,
			})
			if err != nil {
				t.Fatalf("unexpected failure sending test messages: %v", err)
			}
		}
		pub := NewPublisher[BatchMessage](conn)
		err = pub.Publish("batch-message", expected...)
		if err != nil {
			t.Fatalf("unexpected failure sending test messages: %v", err)
		}

		// Act.
		var actualBatches []int
		var actual []BatchMessage
		p := func(ctx context.Context, msgs []BatchMessage) []error {
			actualBatches = append(actualBatches, len(msgs))
			actual = append(actual, msgs...)
			return make([]error, len(msgs))
		}
		bp := NewBatchProcessor[BatchMessage](consumer, 10, p, WithFetchOpts[BatchMessage](jetstream.FetchMaxWait(time.Millisecond)))
		for i := 0; i < 3; i++ {
			if err := bp.Process(ctx); err != nil {
				t.Fatalf("unexpected error processing batch: %v", err)
			}
		}

		// Assert.
		// Check that the batches were properly collated.
		if diff := cmp.Diff(expectedBatches, actualBatches); diff != "" {
			t.Error(diff)
		}
		// Check that the message content was properly delivered.
		if diff := cmp.Diff(expected, actual); diff != "" {
			t.Error(diff)
		}
	})
	t.Run("invalid JSON messages are not sent to the process function", func(t *testing.T) {
		// Arrange.
		expected := []BatchMessage{
			{
				Index: 0,
			},
			{
				Index: 2,
			},
		}
		pub := NewPublisher[BatchMessage](conn)
		err = pub.Publish("batch-message", expected[0])
		if err != nil {
			t.Fatalf("unexpected failure sending test message: %v", err)
		}
		err = conn.Publish("batch-message", []byte("{ _this_is_not_json_ }"))
		if err != nil {
			t.Fatalf("unexpected failure sending test message: %v", err)
		}
		err = pub.Publish("batch-message", expected[1])
		if err != nil {
			t.Fatalf("unexpected failure sending test message: %v", err)
		}

		// Act.
		var actual []BatchMessage
		p := func(ctx context.Context, msgs []BatchMessage) []error {
			actual = append(actual, msgs...)
			return make([]error, len(msgs))
		}
		bp := NewBatchProcessor[BatchMessage](consumer, 10, p, WithFetchOpts[BatchMessage](jetstream.FetchMaxWait(time.Millisecond)))
		for i := 0; i < 3; i++ {
			if err := bp.Process(ctx); err != nil {
				t.Fatalf("unexpected error processing batch: %v", err)
			}
		}

		// Assert.
		// Check that the batches were properly collated.
		// Check that the message content was properly delivered.
		if diff := cmp.Diff(expected, actual); diff != "" {
			t.Error(diff)
		}
	})
	t.Run("if the process function errors, the message is nacked and will be sent again", func(t *testing.T) {
		// Arrange.
		expected := []BatchMessage{
			{
				Index: 0,
			},
		}
		pub := NewPublisher[BatchMessage](conn)
		err := pub.Publish("batch-message", expected...)
		if err != nil {
			t.Fatalf("unexpected failure sending test messages: %v", err)
		}

		// Act / Assert.
		errFailedToProcessMessage := errors.New("failed to process message for some reason")
		fail := func(ctx context.Context, msgs []BatchMessage) (errs []error) {
			if len(msgs) != 1 {
				t.Fatalf("expected 1 message in batch, got %d", len(msgs))
			}
			for i := 0; i < len(msgs); i++ {
				errs = append(errs, errFailedToProcessMessage)
			}
			return
		}
		bpFail := NewBatchProcessor[BatchMessage](consumer, 10, fail, WithFetchOpts[BatchMessage](jetstream.FetchMaxWait(time.Millisecond)))
		var bpFailErrorHandlerCalls int
		bpFail.ErrorHandler = func(msg BatchMessage, err error) {
			if err != errFailedToProcessMessage {
				t.Errorf("error handler expected %q, got %q", errFailedToProcessMessage, err)
			}
			bpFailErrorHandlerCalls++
		}
		if err := bpFail.Process(ctx); err != nil {
			t.Fatalf("unexpected error processing batch: %v", err)
		}
		if bpFailErrorHandlerCalls != 1 {
			t.Errorf("expected 1 error handler call, got %d", bpFailErrorHandlerCalls)
		}

		var actual []BatchMessage
		succeed := func(ctx context.Context, msgs []BatchMessage) (errs []error) {
			actual = append(actual, msgs...)
			return make([]error, len(msgs))
		}
		bpSucceed := NewBatchProcessor[BatchMessage](consumer, 10, succeed, WithFetchOpts[BatchMessage](jetstream.FetchMaxWait(time.Millisecond)))
		var bpSucceedErrorHandlerCalls int
		bpSucceed.ErrorHandler = func(msg BatchMessage, err error) {
			bpSucceedErrorHandlerCalls++
		}
		if err := bpSucceed.Process(ctx); err != nil {
			t.Fatalf("unexpected error processing batch: %v", err)
		}
		if bpSucceedErrorHandlerCalls != 0 {
			t.Errorf("expected 0 error handler calls, got %d", bpSucceedErrorHandlerCalls)
		}

		// Check that everything was processed.
		if diff := cmp.Diff(expected, actual); diff != "" {
			t.Error(diff)
		}
	})
}
