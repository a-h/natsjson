package natsjson

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/nats-io/nats.go/jetstream"
)

func NewKV[T any](kv jetstream.KeyValue, subject string) (db *KV[T]) {
	return &KV[T]{
		kv:      kv,
		subject: subject,
	}
}

type KV[T any] struct {
	kv      jetstream.KeyValue
	subject string
}

func (db *KV[T]) keyToSubject(key string) (hash string) {
	h := sha256.New()
	_, _ = h.Write([]byte(key))
	return db.subject + "." + hex.EncodeToString(h.Sum(nil))
}

func (db *KV[T]) Get(ctx context.Context, key string) (value T, rev uint64, ok bool, err error) {
	entry, err := db.kv.Get(ctx, db.keyToSubject(key))
	if err != nil {
		if err == jetstream.ErrKeyNotFound {
			return value, 0, false, nil
		}
		return value, 0, false, err
	}
	err = json.Unmarshal(entry.Value(), &value)
	return value, entry.Revision(), err == nil, err
}

func (db *KV[T]) GetRevision(ctx context.Context, key string, revision uint64) (value T, ok bool, err error) {
	entry, err := db.kv.GetRevision(ctx, db.keyToSubject(key), revision)
	if err != nil {
		if err == jetstream.ErrKeyNotFound {
			return value, false, nil
		}
		return value, false, err
	}
	err = json.Unmarshal(entry.Value(), &value)
	return value, err == nil, err
}

func (db *KV[T]) History(ctx context.Context, key string) (values []T, ok bool, err error) {
	entries, err := db.kv.History(ctx, db.keyToSubject(key))
	if err != nil {
		if err == jetstream.ErrKeyNotFound {
			return values, false, nil
		}
		return values, false, err
	}
	values = make([]T, len(entries))
	for i := 0; i < len(entries); i++ {
		entry := entries[i]
		err = json.Unmarshal(entry.Value(), &values[i])
		if err != nil {
			return values, false, err
		}
	}
	return values, true, nil
}

func (db *KV[T]) Put(ctx context.Context, key string, value T) (rev uint64, err error) {
	entry, err := json.Marshal(value)
	if err != nil {
		return rev, err
	}
	rev, err = db.kv.Put(ctx, db.keyToSubject(key), entry)
	return
}

func (db *KV[T]) Delete(ctx context.Context, key string) (err error) {
	return db.kv.Delete(ctx, db.keyToSubject(key))
}

var ErrOptimisticConcurrencyCheckFailed = errors.New("optimistic concurrency check failed")

func (db *KV[T]) Update(ctx context.Context, key string, value T, last uint64) (rev uint64, err error) {
	entry, err := json.Marshal(value)
	if err != nil {
		return rev, err
	}
	rev, err = db.kv.Update(ctx, db.keyToSubject(key), entry, last)
	var apiErr jetstream.JetStreamError
	if errors.As(err, &apiErr) && apiErr.APIError() != nil {
		if apiErr.APIError().ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence {
			return 0, ErrOptimisticConcurrencyCheckFailed
		}
	}
	return
}

type Revision[T any] struct {
	Value T
	Rev   uint64
}

type Iterator[T any] struct {
	next  func() (value T, ok bool, err error)
	Value T
	Error error
	stop  func() error
}

func (it *Iterator[T]) Next() (ok bool) {
	it.Value, ok, it.Error = it.next()
	return ok
}

func (it *Iterator[T]) Stop() error {
	return it.stop()
}

func NewIterator[T any](next func() (value T, ok bool, err error), stop func() error) *Iterator[T] {
	return &Iterator[T]{
		next: next,
		stop: stop,
	}
}

func (db *KV[T]) List(ctx context.Context) (it *Iterator[T]) {
	w, err := db.kv.WatchAll(ctx, jetstream.IgnoreDeletes())
	if err != nil {
		next := func() (T, bool, error) {
			var t T
			return t, false, err
		}
		stop := func() error {
			return nil
		}
		return NewIterator[T](next, stop)
	}
	updates := w.Updates()

	next := func() (v T, ok bool, err error) {
		update := <-updates
		if update == nil {
			// We're finished.
			return
		}
		err = json.Unmarshal(update.Value(), &v)
		if err != nil {
			return
		}
		return v, true, nil
	}
	return NewIterator[T](next, w.Stop)
}
