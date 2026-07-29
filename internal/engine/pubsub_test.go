package engine

import (
	"testing"
	"time"
)

func TestPubSubSubscriptionExcludesAlreadyPublishedEvents(t *testing.T) {
	ps := NewPubSub()
	defer ps.Close()

	ps.Publish(ChangeEvent{Table: "items", Op: "insert", RowID: "before"})
	received := make(chan ChangeEvent, 2)
	unsubscribe := ps.Subscribe([]string{"items"}, func(event ChangeEvent) {
		received <- event
	})
	defer unsubscribe()
	ps.Publish(ChangeEvent{Table: "items", Op: "insert", RowID: "after"})

	select {
	case event := <-received:
		if event.RowID != "after" {
			t.Fatalf("subscriber received pre-subscription event %q", event.RowID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for post-subscription event")
	}
	select {
	case event := <-received:
		t.Fatalf("unexpected extra event: %+v", event)
	case <-time.After(10 * time.Millisecond):
	}
}
