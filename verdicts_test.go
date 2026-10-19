package bitfab

import (
	"context"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"
)

func TestVerdicts_LabelsIsTheSameClient(t *testing.T) {
	client := NewClient("test-key")
	if client.Verdicts == nil || client.Labels != client.Verdicts {
		t.Fatalf("Verdicts = %p, Labels = %p", client.Verdicts, client.Labels)
	}
}

func TestVerdicts_GenerateEvidenceUsesSelectedTraceAndAssertion(t *testing.T) {
	spanName := "lookup-record"
	want := []AssertionVerdictEvidence{{
		SpanID:   "span",
		Text:     "Returned the right record",
		SpanName: &spanName,
		Parameters: []AssertionEvidenceParameter{{
			Name: "recordId",
			Type: "string",
		}},
		SpanType: "function",
		IsMocked: false,
	}}
	server := newDatasetsServer(t, func(datasetRequest) any {
		return map[string]any{"traceId": "one", "assertionId": "assertion", "evidence": want}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	evidence, err := client.Verdicts.GenerateEvidence(context.Background(), "trace", "assertion")
	if err != nil || !reflect.DeepEqual(evidence, want) {
		t.Fatalf("GenerateEvidence = %#v, %v", evidence, err)
	}
	deprecated, err := client.Labels.GenerateLabelEvidence(context.Background(), "trace", "assertion")
	if err != nil || !reflect.DeepEqual(deprecated, want) {
		t.Fatalf("GenerateLabelEvidence = %#v, %v", deprecated, err)
	}
	for _, request := range server.recorded() {
		if request.path != "/api/sdk/traces/trace/assertions/assertion/evidence" || request.query != "" {
			t.Fatalf("request = %+v", request)
		}
	}
}

func TestVerdicts_SaveReplayAssertionSendsFailAndAttemptZero(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any {
		return map[string]any{"verdicts": []any{map[string]any{"key": "original#0", "traceId": "replayed", "action": "set"}}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	attempt := 0
	result, err := client.Verdicts.Save(context.Background(), VerdictUpdate{
		VerdictTarget: VerdictTarget{OriginalTraceID: "original", Attempt: &attempt, AssertionID: "assertion"},
		Verdict:       VerdictFail, Annotation: "arrived late", Confidence: VerdictConfidenceHigh,
	}, WithVerdictExperimentID("run"))
	if err != nil || result.TraceID != "replayed" || result.Action != VerdictActionSet {
		t.Fatalf("Save = %+v, %v", result, err)
	}
	want := map[string]any{"experimentId": "run", "testRunId": "run", "verdicts": []any{map[string]any{
		"originalTraceId": "original", "attempt": float64(0), "assertionId": "assertion",
		"verdict": "fail", "annotation": "arrived late", "confidence": "High",
	}}}
	request := server.recorded()[0]
	if request.path != "/api/sdk/traces/verdicts" || !reflect.DeepEqual(request.body, want) {
		t.Fatalf("request = %+v, want %#v", request, want)
	}
}

func TestVerdicts_SaveAndReadEvidence(t *testing.T) {
	evidence := Justification{{SpanID: "span", Text: "Returned the right record"}}
	server := newDatasetsServer(t, func(r datasetRequest) any {
		if r.method == http.MethodGet {
			return map[string]any{"verdicts": []any{map[string]any{
				"traceId": "one", "verdict": "pass", "verdictStatus": "judged",
				"labelStatus": "labeled", "label": true,
				"annotation": "good", "evidence": evidence, "approved": false,
				"passed": 1, "failed": 0, "assertions": []any{},
			}}}
		}
		return map[string]any{"verdicts": []any{map[string]any{
			"key": "one", "traceId": "one", "action": "set",
		}}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	ctx := context.Background()
	if _, err := client.Verdicts.Save(ctx, VerdictUpdate{
		VerdictTarget: VerdictTarget{TraceID: "one", AssertionID: "assertion"}, Verdict: VerdictPass,
		Annotation: "good", Evidence: &evidence,
	}); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"verdicts": []any{map[string]any{
		"traceId": "one", "assertionId": "assertion", "verdict": "pass", "annotation": "good",
		"evidence": []any{map[string]any{"spanId": "span", "text": "Returned the right record"}},
	}}}
	if !reflect.DeepEqual(server.recorded()[0].body, want) {
		t.Fatalf("request = %#v, want %#v", server.recorded()[0].body, want)
	}
	got, err := client.Verdicts.Get(ctx, "one")
	if err != nil || got == nil || got.Evidence == nil || !reflect.DeepEqual(*got.Evidence, evidence) {
		t.Fatalf("Get = %+v, %v", got, err)
	}
	if got.Verdict == nil || *got.Verdict != VerdictPass || got.VerdictStatus != VerdictStatusJudged ||
		got.Label == nil || !*got.Label || got.LabelStatus != LabelStatusLabeled {
		t.Fatalf("Get = %+v", got)
	}
	if server.recorded()[1].path != "/api/sdk/traces/verdicts" {
		t.Fatalf("read path = %s", server.recorded()[1].path)
	}
}

func TestVerdicts_SkipCarriesItsReason(t *testing.T) {
	server := newDatasetsServer(t, func(r datasetRequest) any {
		return map[string]any{"verdicts": []any{map[string]any{
			"key": "one", "traceId": "one", "action": "skipped",
		}}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	if _, err := client.Verdicts.Save(context.Background(), VerdictUpdate{
		VerdictTarget: VerdictTarget{TraceID: "one", AssertionID: "assertion"},
		Skip:          true,
		Annotation:    "the turn never terminated",
	}); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"verdicts": []any{map[string]any{
		"traceId": "one", "assertionId": "assertion", "skip": true, "annotation": "the turn never terminated",
	}}}
	if !reflect.DeepEqual(server.recorded()[0].body, want) {
		t.Fatalf("request = %#v, want %#v", server.recorded()[0].body, want)
	}
}

func TestVerdicts_AbstainCarriesItsReasonAndCountsAsItsOwnAction(t *testing.T) {
	server := newDatasetsServer(t, func(r datasetRequest) any {
		return map[string]any{"verdicts": []any{map[string]any{
			"key": "one", "traceId": "replayed", "action": "abstained",
		}}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	outcome, err := client.Verdicts.Save(context.Background(), VerdictUpdate{
		VerdictTarget: VerdictTarget{OriginalTraceID: "one", AssertionID: "assertion"},
		Verdict:       VerdictAbstained,
		Annotation:    "No message to the exec appears in the evidence.",
	}, WithVerdictExperimentID("run"))
	if err != nil || outcome.Action != VerdictActionAbstained {
		t.Fatalf("Save = %+v, %v", outcome, err)
	}
	want := map[string]any{"experimentId": "run", "testRunId": "run", "verdicts": []any{map[string]any{
		"originalTraceId": "one", "assertionId": "assertion", "verdict": "abstained", "annotation": "No message to the exec appears in the evidence.",
	}}}
	if !reflect.DeepEqual(server.recorded()[0].body, want) {
		t.Fatalf("request = %#v, want %#v", server.recorded()[0].body, want)
	}
	if _, err := client.Verdicts.Abstain(context.Background(), VerdictTarget{TraceID: "direct", AssertionID: "assertion"}); err != nil {
		t.Fatal(err)
	}
	wantShorthand := map[string]any{"verdicts": []any{map[string]any{"traceId": "direct", "assertionId": "assertion", "verdict": "abstained"}}}
	if !reflect.DeepEqual(server.recorded()[1].body, wantShorthand) {
		t.Fatalf("Abstain sent %#v", server.recorded()[1].body)
	}
	if _, err := client.Verdicts.Abstain(context.Background(), VerdictTarget{TraceID: "direct"}); err == nil || !strings.Contains(err.Error(), "must name an assertion") {
		t.Fatalf("Abstain without an assertion = %v", err)
	}
	if _, err := client.Verdicts.Save(context.Background(), VerdictUpdate{VerdictTarget: VerdictTarget{TraceID: "direct", AssertionID: "assertion"}, Verdict: VerdictAbstained, Skip: true}); err == nil || !strings.Contains(err.Error(), "only do one of") {
		t.Fatalf("Abstain plus Skip = %v", err)
	}
}

func TestVerdicts_MixedBatchAndTargetedSkipArchive(t *testing.T) {
	server := newDatasetsServer(t, func(r datasetRequest) any {
		verdicts := r.body["verdicts"].([]any)
		outcomes := make([]any, len(verdicts))
		for index, raw := range verdicts {
			action := "set"
			update := raw.(map[string]any)
			if update["skip"] == true {
				action = "skipped"
			} else if update["archive"] == true {
				action = "archived"
			}
			outcomes[index] = map[string]any{"key": "key", "traceId": "trace", "action": action}
		}
		return map[string]any{"verdicts": outcomes}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	ctx := context.Background()
	_, err := client.Verdicts.SaveAll(ctx, []VerdictUpdate{
		{VerdictTarget: VerdictTarget{TraceID: "one", AssertionID: "check"}, Verdict: VerdictPass, Annotation: "works"},
		{VerdictTarget: VerdictTarget{TraceID: "two", AssertionID: "check"}, Skip: true},
		{VerdictTarget: VerdictTarget{OriginalTraceID: "three"}, Archive: true},
	}, WithVerdictExperimentID("run"))
	if err != nil {
		t.Fatal(err)
	}
	attempt := 2
	target := VerdictTarget{OriginalTraceID: "original", Attempt: &attempt, AssertionID: "assertion"}
	skipped, err := client.Verdicts.Skip(ctx, target, WithVerdictExperimentID("run"))
	if err != nil || skipped.Action != VerdictActionSkipped {
		t.Fatalf("Skip = %+v, %v", skipped, err)
	}
	archived, err := client.Verdicts.Archive(ctx, target, WithVerdictExperimentID("run"))
	if err != nil || archived.Action != VerdictActionArchived {
		t.Fatalf("Archive = %+v, %v", archived, err)
	}
	if _, err := client.Verdicts.Skip(ctx, VerdictTarget{TraceID: "direct", AssertionID: "check"}); err != nil {
		t.Fatal(err)
	}
	requests := server.recorded()
	for index, action := range []string{"skip", "archive"} {
		want := map[string]any{"experimentId": "run", "testRunId": "run", "verdicts": []any{map[string]any{
			"originalTraceId": "original", "attempt": float64(2), "assertionId": "assertion", action: true,
		}}}
		if !reflect.DeepEqual(requests[index+1].body, want) {
			t.Errorf("%s request = %#v", action, requests[index+1].body)
		}
	}
	wantDirect := map[string]any{"verdicts": []any{map[string]any{"traceId": "direct", "assertionId": "check", "skip": true}}}
	if !reflect.DeepEqual(requests[3].body, wantDirect) {
		t.Fatalf("direct skip = %#v", requests[3].body)
	}
}

func TestVerdicts_HumanWritesUseDedicatedEndpoint(t *testing.T) {
	server := newDatasetsServer(t, func(r datasetRequest) any {
		outcomes := make([]any, 0)
		for _, raw := range r.body["verdicts"].([]any) {
			update := raw.(map[string]any)
			outcomes = append(outcomes, map[string]any{
				"traceId": update["traceId"], "assertionId": update["assertionId"], "verdict": update["verdict"], "action": "set",
			})
		}
		return map[string]any{"verdicts": outcomes}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	result, err := client.Verdicts.SaveHuman(context.Background(), HumanVerdictUpdate{
		TraceID: "one", AssertionID: "assertion", Verdict: VerdictFail, Annotation: "human check", Confidence: VerdictConfidenceMedium,
	})
	if err != nil || result.Verdict != VerdictFail || result.Label || result.AssertionID == nil || *result.AssertionID != "assertion" {
		t.Fatalf("SaveHuman = %+v, %v", result, err)
	}
	results, err := client.Verdicts.SaveHumanAll(context.Background(), []HumanVerdictUpdate{
		{TraceID: "two", AssertionID: "assertion", Verdict: VerdictPass, Annotation: "pass"},
		{TraceID: "three", AssertionID: "assertion", Verdict: VerdictFail, Annotation: "fail"},
	})
	if err != nil || len(results) != 2 || results[0].Verdict != VerdictPass || !results[0].Label || results[1].Label {
		t.Fatalf("SaveHumanAll = %+v, %v", results, err)
	}
	for _, request := range server.recorded() {
		if request.method != http.MethodPost || request.path != "/api/sdk/traces/verdicts/human" {
			t.Fatalf("request = %+v", request)
		}
	}
	want := map[string]any{"verdicts": []any{map[string]any{
		"traceId": "one", "assertionId": "assertion", "verdict": "fail", "annotation": "human check", "confidence": "Medium",
	}}}
	if !reflect.DeepEqual(server.recorded()[0].body, want) {
		t.Fatalf("human body = %#v", server.recorded()[0].body)
	}
}

func TestVerdicts_HumanWritesRefuseAbstainAndMissingAssertion(t *testing.T) {
	server := newDatasetsServer(t, func(r datasetRequest) any {
		return map[string]any{"verdicts": []any{}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	if _, err := client.Verdicts.SaveHuman(context.Background(), HumanVerdictUpdate{
		TraceID: "one", Verdict: VerdictPass, Annotation: "whole trace",
	}); err == nil || !strings.Contains(err.Error(), "human verdicts must name an assertion: one") {
		t.Fatalf("SaveHuman without an assertion = %v", err)
	}
	if _, err := client.Verdicts.SaveHumanAll(context.Background(), []HumanVerdictUpdate{
		{TraceID: "two", AssertionID: "assertion", Verdict: VerdictPass, Annotation: "scoped"},
		{TraceID: "three", Verdict: VerdictFail, Annotation: "whole trace"},
	}); err == nil || !strings.Contains(err.Error(), "human verdicts must name an assertion: three") {
		t.Fatalf("SaveHumanAll without an assertion = %v", err)
	}
	if _, err := client.Verdicts.SaveHuman(context.Background(), HumanVerdictUpdate{
		TraceID: "one", AssertionID: "assertion", Verdict: VerdictAbstained, Annotation: "unsure",
	}); err == nil || !strings.Contains(err.Error(), "must be pass or fail") {
		t.Fatalf("SaveHuman abstained = %v", err)
	}
	if recorded := server.recorded(); len(recorded) != 0 {
		t.Fatalf("requests sent = %+v", recorded)
	}
}

func TestVerdicts_AbstainedRefusesConfidenceAndEvidence(t *testing.T) {
	server := newDatasetsServer(t, func(r datasetRequest) any {
		return map[string]any{"verdicts": []any{}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	for name, update := range map[string]VerdictUpdate{
		"confidence": {VerdictTarget: VerdictTarget{TraceID: "one", AssertionID: "assertion"}, Verdict: VerdictAbstained, Confidence: "High"},
		"evidence":   {VerdictTarget: VerdictTarget{TraceID: "one", AssertionID: "assertion"}, Verdict: VerdictAbstained, Evidence: &Justification{}},
	} {
		if _, err := client.Verdicts.Save(context.Background(), update); err == nil || !strings.Contains(err.Error(), "abstained verdict carries no Confidence or Evidence") {
			t.Fatalf("abstained with %s = %v", name, err)
		}
	}
	if recorded := server.recorded(); len(recorded) != 0 {
		t.Fatalf("requests sent = %+v", recorded)
	}
}

func TestVerdicts_ReadsPreserveNullsAndAssertionVerdicts(t *testing.T) {
	server := newDatasetsServer(t, func(r datasetRequest) any {
		query, _ := url.ParseQuery(r.query)
		if query.Get("traceIds") == "missing" {
			return map[string]any{"verdicts": []any{}}
		}
		return map[string]any{"verdicts": []any{map[string]any{
			"traceId": "one", "verdict": nil, "verdictStatus": "skipped", "labelStatus": "skipped", "label": nil, "annotation": nil,
			"approved": false, "passed": 0, "failed": 1,
			"assertions": []any{map[string]any{
				"assertionId": "assertion", "assertion": "arrives on time", "verdictStatus": "judged", "labelStatus": "labeled", "label": false,
				"verdict": "abstained", "annotation": "arrived late", "confidence": "High", "source": "agent", "labelSource": "agent", "approved": true,
			}},
		}}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	ctx := context.Background()
	verdicts, err := client.Verdicts.GetAll(ctx, []string{"one", "two"})
	if err != nil || len(verdicts) != 1 || verdicts[0].Verdict != nil || verdicts[0].VerdictStatus != VerdictStatusSkipped ||
		verdicts[0].Annotation != nil || verdicts[0].Failed != 1 {
		t.Fatalf("GetAll = %+v, %v", verdicts, err)
	}
	assertion := verdicts[0].Assertions[0]
	if assertion.Verdict == nil || *assertion.Verdict != VerdictAbstained || assertion.VerdictStatus != VerdictStatusJudged ||
		assertion.Source != VerdictSourceAgent || !assertion.Approved || assertion.Label == nil || *assertion.Label {
		t.Fatalf("assertion = %+v", assertion)
	}
	missing, err := client.Verdicts.Get(ctx, "missing")
	if err != nil || missing != nil {
		t.Fatalf("Get missing = %+v, %v", missing, err)
	}
	empty, err := client.Verdicts.GetAll(ctx, nil)
	if err != nil || empty == nil || len(empty) != 0 || len(server.recorded()) != 2 {
		t.Fatalf("empty read = %+v, %v", empty, err)
	}
	if server.recorded()[0].query != "traceIds=one%2Ctwo" {
		t.Fatalf("query = %s", server.recorded()[0].query)
	}
}

func TestVerdicts_InvalidTargetsDoNotSendPartialBatch(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any { return map[string]any{} })
	client := NewClient("test-key", WithServiceURL(server.URL))
	negative := -1
	zero := 0
	for _, target := range []VerdictTarget{
		{}, {TraceID: "one", OriginalTraceID: "two"},
		{OriginalTraceID: "one", Attempt: &negative}, {TraceID: "one", Attempt: &zero},
	} {
		_, err := client.Verdicts.SaveAll(context.Background(), []VerdictUpdate{
			{VerdictTarget: VerdictTarget{TraceID: "valid", AssertionID: "assertion"}, Verdict: VerdictPass, Annotation: "good"},
			{VerdictTarget: target, Archive: true},
		})
		if err == nil {
			t.Fatalf("invalid target accepted: %+v", target)
		}
	}
	if _, err := client.Verdicts.Save(context.Background(), VerdictUpdate{
		VerdictTarget: VerdictTarget{TraceID: "valid", AssertionID: "assertion"}, Verdict: "maybe", Annotation: "unsure",
	}); err == nil || !strings.Contains(err.Error(), "not one of pass, fail, or abstained") {
		t.Fatalf("unknown verdict = %v", err)
	}
	if len(server.recorded()) != 0 {
		t.Fatal("invalid batch sent a request")
	}
}

func TestVerdicts_ExperimentIDAcceptsDeprecatedOptionsAndRejectsConflicts(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any {
		return map[string]any{"verdicts": []any{map[string]any{"key": "original#0", "traceId": "replayed", "action": "skipped"}}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	ctx := context.Background()
	target := VerdictTarget{OriginalTraceID: "original", AssertionID: "assertion"}
	for _, options := range [][]VerdictWriteOption{
		{WithVerdictExperimentID("experiment")},
		{WithLabelExperimentID("experiment")},
		{WithLabelTestRunID("experiment")},
		{WithVerdictExperimentID("experiment"), WithLabelTestRunID("experiment")},
	} {
		if _, err := client.Verdicts.Skip(ctx, target, options...); err != nil {
			t.Fatal(err)
		}
	}
	for index, request := range server.recorded() {
		if request.body["experimentId"] != "experiment" || request.body["testRunId"] != "experiment" {
			t.Fatalf("request %d body = %#v", index, request.body)
		}
	}
	if _, err := client.Verdicts.Skip(ctx, target, WithVerdictExperimentID("one"), WithLabelTestRunID("two")); err == nil || !strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("conflict error = %v", err)
	}
	if len(server.recorded()) != 4 {
		t.Fatal("conflicting options reached the server")
	}
}

func TestVerdicts_AgentVerdictWithoutAssertionIsRefusedBeforeSending(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any { return map[string]any{} })
	client := NewClient("test-key", WithServiceURL(server.URL))
	ctx := context.Background()
	operations := map[string]func() error{
		"Save": func() error {
			_, err := client.Verdicts.Save(ctx, VerdictUpdate{VerdictTarget: VerdictTarget{TraceID: "one"}, Verdict: VerdictPass, Annotation: "good"})
			return err
		},
		"SaveAll": func() error {
			_, err := client.Verdicts.SaveAll(ctx, []VerdictUpdate{
				{VerdictTarget: VerdictTarget{TraceID: "one", AssertionID: "assertion"}, Verdict: VerdictPass, Annotation: "good"},
				{VerdictTarget: VerdictTarget{OriginalTraceID: "two"}, Verdict: VerdictFail, Annotation: "whole trace"},
			}, WithVerdictExperimentID("run"))
			return err
		},
		"Skip": func() error {
			_, err := client.Verdicts.Skip(ctx, VerdictTarget{TraceID: "one"})
			return err
		},
	}
	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			if err := operation(); err == nil || !strings.Contains(err.Error(), "must name an assertion") {
				t.Fatalf("error = %v", err)
			}
		})
	}
	if len(server.recorded()) != 0 {
		t.Fatal("a verdict without an assertion reached the server")
	}
}

func TestVerdicts_ArchiveMayOmitAssertionToClearWholeTraceVerdict(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any {
		return map[string]any{"verdicts": []any{map[string]any{"key": "one", "traceId": "one", "action": "archived"}}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	archived, err := client.Verdicts.Archive(context.Background(), VerdictTarget{TraceID: "one"})
	if err != nil || archived.Action != VerdictActionArchived {
		t.Fatalf("Archive = %+v, %v", archived, err)
	}
	want := map[string]any{"verdicts": []any{map[string]any{"traceId": "one", "archive": true}}}
	if !reflect.DeepEqual(server.recorded()[0].body, want) {
		t.Fatalf("archive body = %#v", server.recorded()[0].body)
	}
}

func TestVerdicts_DeprecatedLabelFieldsSendTheNewSpelling(t *testing.T) {
	server := newDatasetsServer(t, func(r datasetRequest) any {
		count := 1
		if verdicts, ok := r.body["verdicts"].([]any); ok {
			count = len(verdicts)
		}
		outcomes := make([]any, count)
		for index := range outcomes {
			outcomes[index] = map[string]any{"key": "key", "traceId": "trace", "action": "set", "verdict": "pass"}
		}
		return map[string]any{"verdicts": outcomes}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	ctx := context.Background()
	if _, err := client.Labels.SaveAll(ctx, []LabelUpdate{
		{LabelTarget: LabelTarget{TraceID: "one", AssertionID: "check"}, Label: true, Annotation: "works"},
		{LabelTarget: LabelTarget{TraceID: "two", AssertionID: "check"}, Label: false, Annotation: "broke", Confidence: LabelConfidenceHigh},
		{LabelTarget: LabelTarget{TraceID: "three", AssertionID: "check"}, Abstain: true},
	}, WithLabelExperimentID("run")); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"experimentId": "run", "testRunId": "run", "verdicts": []any{
		map[string]any{"traceId": "one", "assertionId": "check", "verdict": "pass", "annotation": "works"},
		map[string]any{"traceId": "two", "assertionId": "check", "verdict": "fail", "annotation": "broke", "confidence": "High"},
		map[string]any{"traceId": "three", "assertionId": "check", "verdict": "abstained"},
	}}
	if !reflect.DeepEqual(server.recorded()[0].body, want) {
		t.Fatalf("deprecated agent body = %#v", server.recorded()[0].body)
	}
	human, err := client.Labels.SaveHuman(ctx, HumanLabelUpdate{TraceID: "one", AssertionID: "check", Label: true, Annotation: "pass"})
	if err != nil || !human.Label || human.Verdict != VerdictPass {
		t.Fatalf("deprecated SaveHuman = %+v, %v", human, err)
	}
	if got := server.recorded()[1].body["verdicts"].([]any)[0].(map[string]any)["verdict"]; got != "pass" {
		t.Fatalf("deprecated human body = %#v", server.recorded()[1].body)
	}
}

func TestVerdicts_RefusesBothVerdictAndDeprecatedLabel(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any { return map[string]any{} })
	client := NewClient("test-key", WithServiceURL(server.URL))
	ctx := context.Background()
	target := VerdictTarget{TraceID: "one", AssertionID: "check"}
	cases := map[string]func() error{
		"agent Verdict and Label": func() error {
			_, err := client.Verdicts.Save(ctx, VerdictUpdate{VerdictTarget: target, Verdict: VerdictFail, Label: true, Annotation: "x"})
			return err
		},
		"agent Verdict and Abstain": func() error {
			_, err := client.Verdicts.Save(ctx, VerdictUpdate{VerdictTarget: target, Verdict: VerdictPass, Abstain: true})
			return err
		},
		"agent VerdictTarget and LabelTarget": func() error {
			_, err := client.Verdicts.Save(ctx, VerdictUpdate{VerdictTarget: target, LabelTarget: LabelTarget{TraceID: "other", AssertionID: "check"}, Verdict: VerdictPass, Annotation: "x"})
			return err
		},
		"human Verdict and Label": func() error {
			_, err := client.Verdicts.SaveHuman(ctx, HumanVerdictUpdate{TraceID: "one", AssertionID: "check", Verdict: VerdictFail, Label: true, Annotation: "x"})
			return err
		},
	}
	for name, operation := range cases {
		t.Run(name, func(t *testing.T) {
			if err := operation(); err == nil || !strings.Contains(err.Error(), "set only") {
				t.Fatalf("error = %v", err)
			}
		})
	}
	if len(server.recorded()) != 0 {
		t.Fatal("a conflicting update reached the server")
	}
}

func TestVerdicts_DeprecatedNoActiveLabelMatchesTheServerAction(t *testing.T) {
	if LabelActionNoActiveLabel != VerdictActionNoActiveVerdict || string(LabelActionNoActiveLabel) != "no-active-verdict" {
		t.Fatalf("LabelActionNoActiveLabel = %q", LabelActionNoActiveLabel)
	}
	server := newDatasetsServer(t, func(datasetRequest) any {
		return map[string]any{"verdicts": []any{map[string]any{"key": "one", "traceId": "one", "action": "no-active-verdict"}}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	outcome, err := client.Labels.Archive(context.Background(), LabelTarget{TraceID: "one"})
	if err != nil || outcome.Action != LabelActionNoActiveLabel {
		t.Fatalf("Archive = %+v, %v", outcome, err)
	}
}

func TestVerdicts_DeprecatedLabelTargetMergesWithPromotedFields(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any {
		return map[string]any{"verdicts": []any{map[string]any{"key": "one", "traceId": "one", "action": "set"}}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	update := LabelUpdate{LabelTarget: LabelTarget{TraceID: "one"}, Label: true, Annotation: "works"}
	update.AssertionID = "check"
	if _, err := client.Labels.Save(context.Background(), update); err != nil {
		t.Fatal(err)
	}
	attempt := 0
	matching := VerdictUpdate{
		VerdictTarget: VerdictTarget{OriginalTraceID: "original", AssertionID: "check"},
		LabelTarget:   LabelTarget{OriginalTraceID: "original", Attempt: &attempt},
		Verdict:       VerdictFail, Annotation: "broke",
	}
	if _, err := client.Verdicts.Save(context.Background(), matching, WithVerdictExperimentID("run")); err != nil {
		t.Fatal(err)
	}
	recorded := server.recorded()
	wantFirst := map[string]any{"verdicts": []any{map[string]any{
		"traceId": "one", "assertionId": "check", "verdict": "pass", "annotation": "works",
	}}}
	if !reflect.DeepEqual(recorded[0].body, wantFirst) {
		t.Fatalf("promoted field body = %#v", recorded[0].body)
	}
	wantSecond := map[string]any{"experimentId": "run", "testRunId": "run", "verdicts": []any{map[string]any{
		"originalTraceId": "original", "attempt": float64(0), "assertionId": "check", "verdict": "fail", "annotation": "broke",
	}}}
	if !reflect.DeepEqual(recorded[1].body, wantSecond) {
		t.Fatalf("matching targets body = %#v", recorded[1].body)
	}
}

func TestVerdicts_DeprecatedLabelTargetConflictsAreRefused(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any { return map[string]any{} })
	client := NewClient("test-key", WithServiceURL(server.URL))
	zero, one := 0, 1
	cases := map[string]VerdictUpdate{
		"TraceID": {
			VerdictTarget: VerdictTarget{TraceID: "one", AssertionID: "check"},
			LabelTarget:   LabelTarget{TraceID: "two"},
		},
		"AssertionID": {
			VerdictTarget: VerdictTarget{TraceID: "one", AssertionID: "check"},
			LabelTarget:   LabelTarget{AssertionID: "other"},
		},
		"Attempt": {
			VerdictTarget: VerdictTarget{OriginalTraceID: "original", Attempt: &zero, AssertionID: "check"},
			LabelTarget:   LabelTarget{Attempt: &one},
		},
	}
	for field, update := range cases {
		t.Run(field, func(t *testing.T) {
			update.Verdict = VerdictPass
			update.Annotation = "x"
			_, err := client.Verdicts.Save(context.Background(), update, WithVerdictExperimentID("run"))
			if err == nil || !strings.Contains(err.Error(), "sets "+field) || !strings.Contains(err.Error(), "set only VerdictTarget") {
				t.Fatalf("error = %v", err)
			}
		})
	}
	if len(server.recorded()) != 0 {
		t.Fatal("a conflicting target reached the server")
	}
}

func TestVerdicts_SaveHumanAllFillsDeprecatedLabelFromVerdict(t *testing.T) {
	server := newDatasetsServer(t, func(datasetRequest) any {
		return map[string]any{"verdicts": []any{
			map[string]any{"traceId": "one", "assertionId": "check", "verdict": "fail", "label": true, "action": "set"},
			map[string]any{"traceId": "two", "assertionId": "check", "verdict": "pass", "label": false, "action": "set"},
		}}
	})
	client := NewClient("test-key", WithServiceURL(server.URL))
	results, err := client.Labels.SaveHumanAll(context.Background(), []HumanLabelUpdate{
		{TraceID: "one", AssertionID: "check", Verdict: VerdictFail, Annotation: "broke"},
		{TraceID: "two", AssertionID: "check", Verdict: VerdictPass, Annotation: "works"},
	})
	if err != nil || len(results) != 2 {
		t.Fatalf("SaveHumanAll = %+v, %v", results, err)
	}
	if results[0].Verdict != VerdictFail || results[0].Label {
		t.Fatalf("fail outcome = %+v", results[0])
	}
	if results[1].Verdict != VerdictPass || !results[1].Label {
		t.Fatalf("pass outcome = %+v", results[1])
	}
}
