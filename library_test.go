package bitfab

import (
	"context"
	"strings"
	"testing"
)

const (
	payments       = "example.com/payments"
	paymentsCharge = payments + ".Charge"
	paymentsFee    = payments + "/internal/fees.(*Calculator).Fee"
	ledger         = "example.com/payments/ledger"
	ledgerPost     = ledger + ".Post"
	otherLibrary   = "example.com/other.Helper"
)

func withBuildModules(t *testing.T, modules ...string) {
	t.Helper()
	set := buildModuleSet{main: "example.com/app", modules: map[string]bool{}, known: true}
	for _, module := range modules {
		set.modules[module] = true
	}
	previous := currentBuildModules
	currentBuildModules = func() buildModuleSet { return set }
	libraryModuleCache.Clear()
	t.Cleanup(func() {
		currentBuildModules = previous
		libraryModuleCache.Clear()
	})
}

func TestLibraryModule_ResolvesLongestDependencyModule(t *testing.T) {
	withBuildModules(t, payments, ledger, "gopkg.in/yaml.v3")
	cases := map[string]string{
		paymentsCharge:                   payments,
		paymentsFee:                      payments,
		ledgerPost:                       ledger,
		"gopkg.in/yaml%2ev3.Marshal":     "gopkg.in/yaml.v3",
		"example.com/app/agent.Run":      "",
		"main.helper":                    "",
		"example.com/other.Helper":       "",
		"example.com/payments2.Charge":   "",
		"example.com/payments.Charge-fm": payments,
	}
	for symbol, want := range cases {
		if got := libraryModule(symbol); got != want {
			t.Errorf("libraryModule(%q) = %q, want %q", symbol, got, want)
		}
	}
}

func TestIncludePackages_ClientOptionRecordsNamedModuleBeneathEveryRoot(t *testing.T) {
	withBuildModules(t, payments, "example.com/other")
	c, requests := subtreeClient(t)
	WithIncludePackages(payments)(c)
	counts, _ := traceAndCount(t, c, requests, "checkout", TraceOptions{}, func(ctx context.Context) {
		autoTestCall(ctx, paymentsCharge, func() int { return autoTestCall(ctx, paymentsFee, func() int { return 1 }) })
		autoTestCall(ctx, otherLibrary, func() int { return 2 })
		autoTestCall(ctx, "example.com/app.helper", func() int { return 3 })
	})
	if counts[paymentsCharge] != 1 || counts[paymentsFee] != 1 || counts[otherLibrary] != 0 || counts["example.com/app.helper"] != 1 {
		t.Fatalf("counts = %#v", counts)
	}
}

func TestIncludePackages_WithoutClientListEveryRootRecordsEveryRewrittenModule(t *testing.T) {
	withBuildModules(t, payments, ledger)
	c, requests := subtreeClient(t)
	first, _ := traceAndCount(t, c, requests, "first", TraceOptions{}, func(ctx context.Context) {
		autoTestCall(ctx, paymentsCharge, func() int { return 1 })
	})
	second, _ := traceAndCount(t, c, requests, "second", TraceOptions{}, func(ctx context.Context) {
		autoTestCall(ctx, ledgerPost, func() int { return 1 })
	})
	if first[paymentsCharge] != 1 || second[ledgerPost] != 1 {
		t.Fatalf("first = %#v second = %#v", first, second)
	}
}

func TestIncludePackages_EmptyClientListLeavesModulesToTheRootsThatNameThem(t *testing.T) {
	withBuildModules(t, payments)
	c, requests := subtreeClient(t)
	WithIncludePackages()(c)
	named, _ := traceAndCount(t, c, requests, "named", TraceOptions{IncludePackages: []string{payments}}, func(ctx context.Context) {
		autoTestCall(ctx, paymentsCharge, func() int { return 1 })
	})
	unnamed, _ := traceAndCount(t, c, requests, "unnamed", TraceOptions{}, func(ctx context.Context) {
		autoTestCall(ctx, paymentsCharge, func() int { return 1 })
	})
	if named[paymentsCharge] != 1 || unnamed[paymentsCharge] != 0 {
		t.Fatalf("named = %#v unnamed = %#v", named, unnamed)
	}
}

func TestIncludePackages_TraceOptionRecordsOnlyBeneathTheRootThatNamesIt(t *testing.T) {
	withBuildModules(t, payments, ledger)
	c, requests := subtreeClient(t)
	WithIncludePackages(ledger)(c)
	named, _ := traceAndCount(t, c, requests, "named", TraceOptions{IncludePackages: []string{payments}}, func(ctx context.Context) {
		autoTestCall(ctx, paymentsCharge, func() int { return 1 })
		autoTestCall(ctx, ledgerPost, func() int { return 1 })
	})
	if named[paymentsCharge] != 1 || named[ledgerPost] != 1 {
		t.Fatalf("root naming the module: %#v", named)
	}
	unnamed, _ := traceAndCount(t, c, requests, "unnamed", TraceOptions{}, func(ctx context.Context) {
		autoTestCall(ctx, paymentsCharge, func() int {
			return autoTestCall(ctx, "example.com/app.callback", func() int { return 1 })
		})
		autoTestCall(ctx, ledgerPost, func() int { return 1 })
	})
	if unnamed[paymentsCharge] != 0 || unnamed["example.com/app.callback"] != 1 || unnamed[ledgerPost] != 1 {
		t.Fatalf("root not naming the module: %#v", unnamed)
	}
}

func TestIncludePackages_NestedRootNamesModuleOnlyForItself(t *testing.T) {
	withBuildModules(t, payments)
	c, requests := subtreeClient(t)
	WithIncludePackages()(c)
	_, err := c.Trace(context.Background(), "outer", func(ctx context.Context) (any, error) {
		return c.Trace(ctx, "inner", func(ctx context.Context) (any, error) {
			autoTestCall(ctx, paymentsCharge, func() int { return 1 })
			return nil, nil
		}, TraceOptions{IncludePackages: []string{payments}})
	}, TraceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	spans := flushedRawSpans(t, c, requests)
	byTrace := map[any]map[string]int{}
	for _, span := range spans {
		name := span["span_data"].(map[string]any)["name"].(string)
		if byTrace[span["trace_id"]] == nil {
			byTrace[span["trace_id"]] = map[string]int{}
		}
		byTrace[span["trace_id"]][name]++
	}
	var innerTrace any
	total := 0
	for traceID, counts := range byTrace {
		total += counts[paymentsCharge]
		if counts["outer"] == 0 {
			innerTrace = traceID
		}
	}
	if innerTrace == nil || byTrace[innerTrace][paymentsCharge] != 1 || total != 1 {
		t.Fatalf("spans by trace = %#v", byTrace)
	}
}

func TestIncludePackages_LimitsExcludeAndCaptureOffStillApply(t *testing.T) {
	withBuildModules(t, payments)
	c, requests := subtreeClient(t)
	no := false
	if err := c.Node(paymentsFee, NodeOptions{Capture: &no}); err != nil {
		t.Fatal(err)
	}
	subtree, depth := 3, 1
	counts, truncation := traceAndCount(t, c, requests, "limited", TraceOptions{IncludePackages: []string{payments}, MaxCapturedSubtreeSpans: &subtree, Exclude: []string{"Refund"}}, func(ctx context.Context) {
		callAuto(ctx, paymentsCharge, 5)
		callAuto(ctx, payments+".Refund", 2)
		callAuto(ctx, paymentsFee, 2)
	})
	if counts[paymentsCharge] != 3 || counts[payments+".Refund"] != 0 || counts[paymentsFee] != 0 {
		t.Fatalf("counts = %#v", counts)
	}
	if truncation["bitfab.truncated_by"] != "max_captured_subtree_spans" {
		t.Fatalf("truncation = %#v", truncation)
	}
	deep, deepTruncation := traceAndCount(t, c, requests, "deep", TraceOptions{IncludePackages: []string{payments}, MaxDepth: &depth}, func(ctx context.Context) {
		autoNestedCall(ctx, paymentsCharge, func(ctx context.Context) { autoTestCall(ctx, payments+".Inner", func() int { return 1 }) })
	})
	if deep[paymentsCharge] != 1 || deep[payments+".Inner"] != 0 || deepTruncation["bitfab.truncated_by"] != "max_depth" {
		t.Fatalf("deep counts = %#v truncation = %#v", deep, deepTruncation)
	}
	limit := 10
	big := strings.Repeat("x", 64)
	payload, payloadTruncation := traceAndCount(t, c, requests, "payload", TraceOptions{IncludePackages: []string{payments}, MaxPayloadBytesPerFunction: &limit}, func(ctx context.Context) {
		for range 3 {
			payloadCall(ctx, paymentsCharge, big)
		}
	})
	if payload[paymentsCharge] != 1 || payloadTruncation["bitfab.truncated_by"] != "max_payload_bytes_per_function" {
		t.Fatalf("payload counts = %#v truncation = %#v", payload, payloadTruncation)
	}
}

func payloadCall(ctx context.Context, symbol, input string) (result string) {
	node := EnterAutoNode(ctx, symbol, symbol, []any{input}, []any{&result})
	if node != nil {
		defer node.End(nil)
	}
	return input
}

func TestIncludePackages_RejectsTheSDKAndTheStandardLibrary(t *testing.T) {
	withBuildModules(t, payments)
	c, _ := subtreeClient(t)
	for _, libraries := range [][]string{{sdkModulePath}, {sdkModulePath + "/baml"}, {"fmt"}, {""}} {
		if _, err := c.Trace(context.Background(), "invalid", func(context.Context) (any, error) { t.Fatal("body ran"); return nil, nil }, TraceOptions{IncludePackages: libraries}); err == nil {
			t.Fatalf("IncludePackages %q was accepted", libraries)
		}
		client := NewClient("test-key", WithIncludePackages(libraries...))
		if _, err := client.Trace(context.Background(), "invalid", func(context.Context) (any, error) { t.Fatal("body ran"); return nil, nil }, TraceOptions{}); err == nil {
			t.Fatalf("WithIncludePackages(%q) was accepted", libraries)
		}
	}
}

func TestIncludePackages_ModuleThisProgramDoesNotUseWarnsOnce(t *testing.T) {
	withBuildModules(t, payments)
	resetWarnOnce()
	t.Cleanup(resetWarnOnce)
	c, _ := subtreeClient(t)
	if _, err := c.Trace(context.Background(), "missing", func(context.Context) (any, error) { return nil, nil }, TraceOptions{IncludePackages: []string{"example.com/absent", payments}}); err != nil {
		t.Fatal(err)
	}
	if _, warned := warnedKeys.Load("include-library-missing:example.com/absent"); !warned {
		t.Fatal("missing module did not warn")
	}
	if _, warned := warnedKeys.Load("include-library-missing:" + payments); warned {
		t.Fatal("module in the build warned")
	}
}
