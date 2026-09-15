package bitfab

import (
	"context"
	"testing"
	"time"
)

func TestSpanFinalize_CloseTimeoutRemainsFailedAfterFinalizerEnds(t *testing.T) {
	client, requests := subtreeClient(t)
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	_, err := client.Span(context.Background(), "root", func(context.Context) (any, error) {
		return "live", nil
	}, WithFinalize(func(any) (any, error) {
		<-release
		return "recorded", nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if client.Close(0) {
		t.Fatal("close succeeded with a pending finalizer")
	}
	close(release)
	if !client.waitAutoFinalizers(time.Second) {
		t.Fatal("finalizer did not settle")
	}
	if client.Close(time.Second) {
		t.Fatal("repeated close forgot the failed delivery")
	}
	if len(requests()) != 0 {
		t.Fatal("timed-out finalizer sent records after close")
	}
}
