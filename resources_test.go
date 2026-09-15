package bitfab

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestResources_InitializedOnClient(t *testing.T) {
	client := NewClient("test-key")
	if client.Traces == nil || client.Labels == nil || client.Graders == nil {
		t.Fatal("resource namespaces are missing")
	}
}

func TestResources_PropagateHTTPFailures(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "rejected by server", http.StatusBadRequest)
	}))
	defer server.Close()
	client := NewClient("test-key", WithServiceURL(server.URL))
	ctx := context.Background()
	operations := map[string]func() error{
		"assertions read": func() error { _, err := client.Traces.GetAssertions(ctx, "trace"); return err },
		"assertions save": func() error {
			_, err := client.Traces.SaveAssertions(ctx, SaveAssertionsParams{TraceID: "trace", Assertions: []SaveAssertion{{Assertion: "works"}}})
			return err
		},
		"assertions archive": func() error {
			_, err := client.Traces.ArchiveAssertions(ctx, ArchiveAssertionsParams{TraceID: "trace", AssertionIDs: []string{"assertion"}})
			return err
		},
		"labels read": func() error { _, err := client.Labels.Get(ctx, "trace"); return err },
		"labels save": func() error {
			_, err := client.Labels.Save(ctx, LabelUpdate{LabelTarget: LabelTarget{TraceID: "trace"}, Label: false, Annotation: "failed"})
			return err
		},
		"human labels": func() error {
			_, err := client.Labels.SaveHuman(ctx, HumanLabelUpdate{TraceID: "trace", Label: false, Annotation: "failed"})
			return err
		},
		"grader labels": func() error {
			_, err := client.Graders.GetLabels(ctx, GetGraderLabelsParams{TraceIDs: []string{"trace"}})
			return err
		},
	}
	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			if err := operation(); err == nil || !strings.Contains(err.Error(), "rejected by server") {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestResources_MalformedResponseReturnsError(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any {
		return map[string]any{"labels": "not an array"}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	_, err := client.Labels.Save(context.Background(), LabelUpdate{LabelTarget: LabelTarget{TraceID: "trace"}, Label: true, Annotation: "good"})
	if err == nil || !strings.Contains(err.Error(), "decode SDK response") {
		t.Fatalf("error = %v", err)
	}
}
