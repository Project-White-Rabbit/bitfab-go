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
	if client.Traces == nil || client.Verdicts == nil || client.Graders == nil {
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
		"verdicts read": func() error { _, err := client.Verdicts.Get(ctx, "trace"); return err },
		"verdicts save": func() error {
			_, err := client.Verdicts.Save(ctx, VerdictUpdate{VerdictTarget: VerdictTarget{TraceID: "trace", AssertionID: "assertion"}, Verdict: VerdictFail, Annotation: "failed"})
			return err
		},
		"human verdicts": func() error {
			_, err := client.Verdicts.SaveHuman(ctx, HumanVerdictUpdate{TraceID: "trace", AssertionID: "assertion", Verdict: VerdictFail, Annotation: "failed"})
			return err
		},
		"grader verdicts": func() error {
			_, err := client.Graders.GetVerdicts(ctx, GetGraderVerdictsParams{TraceIDs: []string{"trace"}})
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
		return map[string]any{"verdicts": "not an array"}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	_, err := client.Verdicts.Save(context.Background(), VerdictUpdate{VerdictTarget: VerdictTarget{TraceID: "trace", AssertionID: "assertion"}, Verdict: VerdictPass, Annotation: "good"})
	if err == nil || !strings.Contains(err.Error(), "decode SDK response") {
		t.Fatalf("error = %v", err)
	}
}
