package bitfab

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

type largeSlicesHolder struct {
	Series  [10][]float64
	Samples []int32
}

func newLargeSlicesHolder() largeSlicesHolder {
	var holder largeSlicesHolder
	for i := range holder.Series {
		series := make([]float64, 2_000_000)
		for j := range series {
			series[j] = float64(j) * 1.25
		}
		holder.Series[i] = series
	}
	holder.Samples = make([]int32, 4_000_000)
	for i := range holder.Samples {
		holder.Samples[i] = int32(i)
	}
	return holder
}

func TestCapValue_StubsLargeSlicesWithoutEncodingThem(t *testing.T) {
	inputs := map[string]any{
		"struct": newLargeSlicesHolder(),
		"map":    map[string]any{"a": make([]float64, 5_000_000), "b": make([]int32, 4_000_000)},
	}
	for name, value := range inputs {
		t.Run(name, func(t *testing.T) {
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			start := time.Now()
			got, dropped := capValueReport(value)
			elapsed := time.Since(start)
			runtime.ReadMemStats(&after)
			allocated := after.TotalAlloc - before.TotalAlloc

			want := fmt.Sprintf("<unserializable: too_large_over_%d_bytes>", MaxSerializedValueBytes)
			if got != want {
				t.Fatalf("got %#v, want %q", got, want)
			}
			if len(dropped) != 1 || dropped[0] != "too_large" {
				t.Fatalf("dropped = %v, want [too_large]", dropped)
			}
			t.Logf("stubbed in %s, allocated %d bytes", elapsed, allocated)
			if elapsed > 50*time.Millisecond {
				t.Fatalf("stubbing took %s, want under 50ms", elapsed)
			}
			if allocated > 1<<20 {
				t.Fatalf("stubbing allocated %d bytes, want under 1 MiB", allocated)
			}
		})
	}
}

func TestCapValue_StubsLargeStringSliceEarly(t *testing.T) {
	words := make([]string, 2_000_000)
	for i := range words {
		words[i] = "word"
	}
	got, dropped := capValueReport(words)
	if s, ok := got.(string); !ok || !strings.Contains(s, "too_large_over_") || len(dropped) != 1 {
		t.Fatalf("got %#v, %v", got, dropped)
	}
}

type hiddenPayload struct {
	Name    string
	Payload []float64 `json:"-"`
}

type summarizedPayload struct {
	values []float64
}

func (s summarizedPayload) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`{"count":%d}`, len(s.values))), nil
}

type pointerSummarizedPayload struct {
	Values []float64
}

func (s *pointerSummarizedPayload) MarshalJSON() ([]byte, error) {
	return []byte(`{"count":1}`), nil
}

type textSummarizedPayload struct {
	Values []float64
}

func (s textSummarizedPayload) MarshalText() ([]byte, error) {
	return []byte("summary"), nil
}

type omittedPayload struct {
	Name  string    `json:"name"`
	Empty []float64 `json:"empty,omitempty"`
	Zero  float64   `json:"zero,omitempty"`
}

func TestCapValue_KeepsValuesThatFitTheCap(t *testing.T) {
	huge := make([]float64, 5_000_000)
	cases := map[string]any{
		"bytes as base64":              make([]byte, MaxSerializedValueBytes/4*3-30),
		"field skipped by dash tag":    hiddenPayload{Name: "kept", Payload: huge},
		"pointer to dash tagged field": &hiddenPayload{Name: "kept", Payload: huge},
		"small MarshalJSON":            summarizedPayload{values: huge},
		"small pointer MarshalJSON":    &pointerSummarizedPayload{Values: huge},
		"small MarshalText":            textSummarizedPayload{Values: huge},
		"raw message":                  json.RawMessage(`{"a":1}`),
		"omitempty":                    omittedPayload{Name: "kept"},
	}
	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			got, dropped := capValueReport(value)
			if len(dropped) != 0 {
				t.Fatalf("value was stubbed: %#v", got)
			}
			if s, ok := got.(string); ok && strings.Contains(s, "too_large") {
				t.Fatalf("value was stubbed: %q", s)
			}
		})
	}
}

type embeddedInner struct {
	Shared string
	Inner  []int
}

type embeddedOther struct {
	Shared string
	Other  string `json:"other_name"`
}

type embeddingOuter struct {
	embeddedInner
	*embeddedOther
	Tagged embeddedInner `json:"tagged"`
	Number json.Number
	Flag   bool  `json:",omitempty"`
	Ptr    *int  `json:"ptr,omitempty"`
	Any    any   `json:"any"`
	Ch     []int `json:"ch,omitzero"`
}

type mapHolder struct {
	Counts map[string]any
}

type promotedMapHolder struct {
	mapHolder
}

type cyclicNode struct {
	Name string
	Next *cyclicNode
}

func TestExceedsSerializedBudget_NeverCountsMoreThanMarshalEmits(t *testing.T) {
	seven := 7
	negativeZero := math.Copysign(0, -1)
	values := []any{
		nil,
		true,
		false,
		0,
		-123456,
		uint8(9),
		3.5,
		negativeZero,
		"",
		"plain",
		"<html> &   \xff",
		json.Number(""),
		json.Number("12"),
		[]byte{},
		[]byte(nil),
		[]byte("abcdefg"),
		[3]byte{1, 2, 3},
		[]bool{true, false},
		[]int{},
		[]any{nil, 1, "x", []int{1}},
		map[string]any{},
		map[string]any{"a": 1, "bb": []string{"x"}},
		map[int]string{1: "a", 22: "b"},
		map[string]any(nil),
		(*int)(nil),
		&seven,
		struct{}{},
		struct{ a, b int }{1, 2},
		omittedPayload{},
		omittedPayload{Zero: negativeZero},
		struct {
			F float64 `json:",omitempty"`
		}{negativeZero},
		hiddenPayload{Name: "x", Payload: []float64{1, 2}},
		summarizedPayload{values: []float64{1}},
		&pointerSummarizedPayload{},
		[]pointerSummarizedPayload{{}, {}},
		textSummarizedPayload{},
		map[string]textSummarizedPayload{"a": {}},
		time.Unix(0, 0).UTC(),
		embeddingOuter{},
		embeddingOuter{embeddedInner: embeddedInner{Shared: "a", Inner: []int{1, 2}}, embeddedOther: &embeddedOther{Shared: "b", Other: "c"}, Number: "4", Flag: true, Ptr: &seven, Any: []int{1}},
		struct {
			Bad  string `json:"bad name\""`
			Good string `json:"good-name"`
		}{"x", "y"},
		&cyclicNode{Name: "a", Next: &cyclicNode{Name: "b"}},
		promotedMapHolder{mapHolder{Counts: map[string]any{"a": 1, "bb": "two", "ccc": []int{3}}}},
	}
	for i, value := range values {
		body, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("case %d (%#v): marshal failed: %v", i, value, err)
		}
		if exceedsSerializedBudget(value, len(body)) {
			t.Fatalf("case %d (%#v): count passed the %d bytes Marshal emits: %s", i, value, len(body), body)
		}
	}
}

func TestExceedsSerializedBudget_CountsMapsReachedThroughUnexportedEmbedding(t *testing.T) {
	value := promotedMapHolder{mapHolder{Counts: map[string]any{"words": make([]float64, 5_000_000)}}}
	if !exceedsSerializedBudget(value, MaxSerializedValueBytes) {
		t.Fatal("a map promoted from an unexported embedded struct should still be counted")
	}
}

func TestExceedsSerializedBudget_StopsOnPointerCycle(t *testing.T) {
	node := &cyclicNode{Name: "loop"}
	node.Next = node
	if exceedsSerializedBudget(node, MaxSerializedValueBytes) {
		t.Fatal("a small cyclic value should not pass the cap")
	}
	got, dropped := capValueReport(node)
	if got != node || len(dropped) != 0 {
		t.Fatalf("a cyclic value should be left for the boundary sanitizer, got %#v, %v", got, dropped)
	}
}

type benchSmall struct {
	ID     string
	Name   string
	Count  int
	Active bool
}

type benchMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
	Name    string `json:"name,omitempty"`
}

type benchLLMRequest struct {
	Model    string         `json:"model"`
	Messages []benchMessage `json:"messages"`
	Params   map[string]any `json:"params"`
}

type benchRow struct {
	ID    int     `json:"id"`
	Label string  `json:"label"`
	Score float64 `json:"score"`
	Tags  []string
}

func newBenchLLMRequest() benchLLMRequest {
	messages := make([]benchMessage, 50)
	for i := range messages {
		messages[i] = benchMessage{Role: "user", Content: strings.Repeat("lorem ipsum ", 42)}
	}
	return benchLLMRequest{
		Model:    "model-name",
		Messages: messages,
		Params:   map[string]any{"temperature": 0.2, "max_tokens": 1024, "stop": []string{"\n\n"}, "stream": false},
	}
}

func newBenchRows() []benchRow {
	rows := make([]benchRow, 10_000)
	for i := range rows {
		rows[i] = benchRow{ID: i, Label: strings.Repeat("x", 60), Score: float64(i) / 3, Tags: []string{"alpha", "beta"}}
	}
	return rows
}

func benchmarkCapValue(b *testing.B, value any) {
	b.ReportAllocs()
	for b.Loop() {
		if _, dropped := capValueReport(value); len(dropped) != 0 {
			b.Fatal("value was stubbed")
		}
	}
}

func BenchmarkCapValueSmallStruct(b *testing.B) {
	benchmarkCapValue(b, benchSmall{ID: "abc-123", Name: "example", Count: 42, Active: true})
}

func BenchmarkCapValueLLMPayload(b *testing.B) {
	benchmarkCapValue(b, newBenchLLMRequest())
}

func BenchmarkCapValueMediumRows(b *testing.B) {
	benchmarkCapValue(b, newBenchRows())
}

type sizeBuildFailurePayload struct {
	Name string
}

type sizeBuildFailureWrapper struct {
	Inner sizeBuildFailurePayload
}

func TestCapValue_FallsBackWhenSizeFunctionBuildPanics(t *testing.T) {
	failing := reflect.TypeFor[sizeBuildFailurePayload]()
	buildSizeFunc = func(t reflect.Type) sizeFunc {
		if t == failing {
			panic("forced build failure")
		}
		return newSizeFunc(t)
	}
	defer func() { buildSizeFunc = nil }()

	values := []any{
		sizeBuildFailureWrapper{Inner: sizeBuildFailurePayload{Name: "first"}},
		sizeBuildFailurePayload{Name: "second"},
		sizeBuildFailureWrapper{Inner: sizeBuildFailurePayload{Name: "third"}},
	}
	for i, value := range values {
		done := make(chan struct{})
		var got any
		var dropped []string
		go func() {
			defer close(done)
			got, dropped = capValueReport(value)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("capture %d hung after a failed size function build", i)
		}
		if !reflect.DeepEqual(got, value) || len(dropped) != 0 {
			t.Fatalf("capture %d should fall back to encoding and keep the value, got %#v, %v", i, got, dropped)
		}
	}

	var waiting sync.WaitGroup
	for range 8 {
		waiting.Add(1)
		go func() {
			defer waiting.Done()
			capValueReport(sizeBuildFailurePayload{Name: "concurrent"})
		}()
	}
	finished := make(chan struct{})
	go func() {
		waiting.Wait()
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent captures hung after a failed size function build")
	}
}

type nonZeroHolder struct {
	Items []float64
}

func (h nonZeroHolder) IsZero() bool { return len(h.Items) == 0 }

func TestExceedsSerializedBudget_CountsOmitzeroFieldWhoseIsZeroSaysNonZero(t *testing.T) {
	value := struct {
		Holder nonZeroHolder `json:"holder,omitzero"`
	}{Holder: nonZeroHolder{Items: make([]float64, 5_000_000)}}
	if !exceedsSerializedBudget(value, MaxSerializedValueBytes) {
		t.Fatal("an omitzero field whose IsZero reports false must be counted")
	}
	empty := struct {
		Holder nonZeroHolder `json:"holder,omitzero"`
	}{}
	body, err := json.Marshal(empty)
	if err != nil {
		t.Fatal(err)
	}
	if exceedsSerializedBudget(empty, len(body)) {
		t.Fatalf("counted more than json.Marshal wrote (%s)", body)
	}
}
