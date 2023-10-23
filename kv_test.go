package natsjson

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go/jetstream"
)

type User struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func TestKV(t *testing.T) {
	// Arrange.
	_, js, shutdown, err := NewInProcessNATSServer()
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown()
	ctx := context.Background()

	// Create key/value bucket in NATS.
	bucketName := "test_kv"
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  bucketName,
		History: 10, // Store previous 10 versions.
		// Storage: jetstream.MemoryStorage, -- See bug at https://github.com/nats-io/nats.go/issues/1447
	})
	if err != nil {
		t.Fatalf("unexpected failure creating bucket: %v", err)
	}

	db := NewKV[User](kv, "users")

	t.Run("getting a non-existent value returns ok=false", func(t *testing.T) {
		_, _, ok, err := db.Get(ctx, "non-existent-key")
		if err != nil {
			t.Errorf("unexpected error getting value: %v", err)
		}
		if ok {
			t.Error("expected ok=false, got ok=true")
		}
	})

	user1Rev1 := User{
		Name: "john",
		Age:  42,
	}
	user1Rev2 := User{
		Name: "john",
		Age:  43,
	}
	user1Rev3 := User{
		Name: "john",
		Age:  44,
	}
	user2Rev1 := User{
		Name: "paul",
		Age:  45,
	}
	user3Rev1 := User{
		Name: "ringo",
		Age:  46,
	}
	user4Rev1 := User{
		Name: "george",
		Age:  47,
	}
	user5Rev1 := User{
		Name: "thomas",
		Age:  48,
	}

	t.Run("Put object", func(t *testing.T) {
		expectedRev, err := db.Put(ctx, "user1", user1Rev1)
		if err != nil {
			t.Errorf("unexpected error putting value: %v", err)
		}
		if expectedRev != 1 {
			t.Errorf("expected to have put revision 1, got %d", expectedRev)
		}
	})
	t.Run("Get object", func(t *testing.T) {
		actual, actualRev, ok, err := db.Get(ctx, "user1")
		if err != nil {
			t.Errorf("unexpected error getting value: %v", err)
		}
		if !ok {
			t.Error("expected ok=true, got ok=false")
		}
		if diff := cmp.Diff(user1Rev1, actual); diff != "" {
			t.Error(diff)
		}
		if actualRev != 1 {
			t.Errorf("expected rev 1, got %d", actualRev)
		}
	})
	t.Run("Put new version", func(t *testing.T) {
		expectedRev, err := db.Put(ctx, "user1", user1Rev2)
		if err != nil {
			t.Errorf("unexpected error putting value: %v", err)
		}
		if expectedRev != 2 {
			t.Errorf("expected to have put revision 1, got %d", expectedRev)
		}
	})
	t.Run("Get latest version", func(t *testing.T) {
		actual, actualRev, ok, err := db.Get(ctx, "user1")
		if err != nil {
			t.Errorf("unexpected error getting value: %v", err)
		}
		if !ok {
			t.Error("expected ok=true, got ok=false")
		}
		if diff := cmp.Diff(user1Rev2, actual); diff != "" {
			t.Error(diff)
		}
		if actualRev != 2 {
			t.Errorf("expected rev 1, got %d", actualRev)
		}
	})
	t.Run("GetRevision", func(t *testing.T) {
		actual, ok, err := db.GetRevision(ctx, "user1", 1)
		if err != nil {
			t.Errorf("unexpected error getting value: %v", err)
		}
		if !ok {
			t.Error("expected ok=true, got ok=false")
		}
		if diff := cmp.Diff(user1Rev1, actual); diff != "" {
			t.Error(diff)
		}
	})
	t.Run("GetRevision returns ok=false for non-existent keys", func(t *testing.T) {
		_, ok, err := db.GetRevision(ctx, "non-existent-key", 1)
		if err != nil {
			t.Errorf("unexpected error getting value: %v", err)
		}
		if ok {
			t.Error("expected ok=false, got ok=true")
		}
	})
	t.Run("History", func(t *testing.T) {
		actual, ok, err := db.History(ctx, "user1")
		if err != nil {
			t.Errorf("unexpected error getting value: %v", err)
		}
		if !ok {
			t.Error("expected ok=true, got ok=false")
		}
		expected := []User{user1Rev1, user1Rev2}
		if diff := cmp.Diff(expected, actual); diff != "" {
			t.Error(diff)
		}
	})
	t.Run("History returns ok=false for non-existent keys", func(t *testing.T) {
		actual, ok, err := db.History(ctx, "non-existent-key")
		if err != nil {
			t.Errorf("unexpected error getting value: %v", err)
		}
		if ok {
			t.Error("expected ok=false, got ok=true")
		}
		var expected []User
		if diff := cmp.Diff(expected, actual); diff != "" {
			t.Error(diff)
		}
	})
	t.Run("Update cannot overwrite a non-current version", func(t *testing.T) {
		_, err := db.Update(ctx, "user1", user1Rev3, 1)
		if err != ErrOptimisticConcurrencyCheckFailed {
			t.Errorf("expected optimistic concurrency error, got %v", err)
		}
	})
	t.Run("Update can overwrite the current version", func(t *testing.T) {
		expectedRev, err := db.Update(ctx, "user1", user1Rev3, 2)
		if err != nil {
			t.Errorf("unexpected error overwriting latest version: %v", err)
		}
		if expectedRev != 3 {
			t.Errorf("expected rev=3, got rev=%d", expectedRev)
		}
	})
	t.Run("Delete", func(t *testing.T) {
		if _, err := db.Put(ctx, "user5", user5Rev1); err != nil {
			t.Fatalf("unexpected error putting user 5: %v", err)
		}
		if err = db.Delete(ctx, "user5"); err != nil {
			t.Fatalf("unexpected error deleting user 5: %v", err)
		}
		_, _, ok, err := db.Get(ctx, "user5")
		if err != nil {
			t.Errorf("unexpected error getting value: %v", err)
		}
		if ok {
			t.Error("expected ok=false, got ok=true")
		}
	})
	t.Run("List can iterate the bucket", func(t *testing.T) {
		if _, err = db.Put(ctx, "user2", user2Rev1); err != nil {
			t.Fatalf("unexpected error putting user 2: %v", err)
		}
		if _, err = db.Put(ctx, "user3", user3Rev1); err != nil {
			t.Fatalf("unexpected error putting user 3: %v", err)
		}
		if _, err := db.Put(ctx, "user4", user4Rev1); err != nil {
			t.Fatalf("unexpected error putting user 4: %v", err)
		}

		expected := []User{user1Rev3, user2Rev1, user3Rev1, user4Rev1}
		var actual []User

		iterator := db.List(ctx)
		for iterator.Next() {
			actual = append(actual, iterator.Value)
		}
		if iterator.Error != nil {
			t.Errorf("unexpected error: %v", iterator.Error)
		}
		iterator.Stop()

		if diff := cmp.Diff(expected, actual); diff != "" {
			t.Error(diff)
		}
	})
}
