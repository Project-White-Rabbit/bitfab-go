package instrument

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func writeFixtureFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestOverlay_RecordsSystemOneCallsAsLlmSpans(t *testing.T) {
	dir := t.TempDir()
	sdk, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	client := filepath.Join(dir, "jevclient")
	app := filepath.Join(dir, "app")
	writeFixtureFile(t, filepath.Join(client, "go.mod"), "module example.com/jevclient\n\ngo 1.26.0\n")
	writeFixtureFile(t, filepath.Join(client, "client.go"), jevClientPackage)
	module := "module example.com/jevapp\n\ngo 1.26.0\n\nrequire (\n\t" + SDKImport + " v0.0.0\n\texample.com/jevclient v0.0.0\n)\n\nreplace " + SDKImport + " => " + sdk + "\n\nreplace example.com/jevclient => " + client + "\n"
	writeFixtureFile(t, filepath.Join(app, "go.mod"), module)
	writeFixtureFile(t, filepath.Join(app, "main.go"), jevApplication)
	writeFixtureFile(t, filepath.Join(app, "main_test.go"), "package main\nimport \"testing\"\nfunc TestGenerated(t *testing.T) { main() }\n")
	cmd := exec.Command("go", "mod", "tidy")
	cmd.Dir = app
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("tidy: %v\n%s", err, out)
	}
	overlay, cleanup, err := Overlay(app)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	for _, args := range [][]string{{"run", "-overlay", overlay, "."}, {"test", "-overlay", overlay, "."}} {
		cmd = exec.Command("go", args...)
		cmd.Dir = app
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("go %s: %v\n%s", args[0], err, out)
		}
		if args[0] == "run" && !strings.Contains(string(out), "jev capture passed") {
			t.Fatalf("output=%s", out)
		}
	}
}

const jevClientPackage = `package jevclient

import (
	"context"
	"errors"
)

type Usage struct {
	InputTokens  int ` + "`json:\"input_tokens\"`" + `
	OutputTokens int ` + "`json:\"output_tokens\"`" + `
}

type Reply struct {
	Model  string ` + "`json:\"model\"`" + `
	Usage  Usage  ` + "`json:\"usage\"`" + `
	Answer string ` + "`json:\"answer\"`" + `
}

type Client struct{}

func (c *Client) SystemOne(ctx context.Context, prompt string) (Reply, error) {
	if ctx == nil {
		return Reply{}, errors.New("missing context")
	}
	if prompt == "fail" {
		return Reply{}, errors.New("jev unavailable")
	}
	return Reply{Model: "jev-1", Usage: Usage{InputTokens: 12, OutputTokens: 5}, Answer: "yes " + prompt}, nil
}

type Plain struct{}

func (Plain) SystemOne(ctx context.Context, prompt string) (map[string]any, error) {
	return map[string]any{"answer": prompt}, nil
}
`

const jevApplication = `package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"

	bitfab "github.com/Project-White-Rabbit/bitfab-go"
	"example.com/jevclient"
)

type ownClient struct{ prefix string }

func prepare(prompt string) string { return "prepared " + prompt }

func (c *ownClient) SystemOne(ctx context.Context, prompt string) (map[string]any, error) {
	if bitfab.GetCurrentSpan(ctx) == nil {
		panic("missing span context inside SystemOne")
	}
	return map[string]any{"model": "jev-own", "usage": map[string]any{"input_tokens": 3, "output_tokens": 4}, "answer": prepare(c.prefix + prompt)}, nil
}

type asker interface {
	SystemOne(context.Context, string) (jevclient.Reply, error)
}

type collected struct {
	mu    sync.Mutex
	spans map[string][]map[string]any
}

func (c *collected) handler(w http.ResponseWriter, r *http.Request) {
	var reader io.Reader = r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			panic(err)
		}
		defer gz.Close()
		reader = gz
	}
	var envelope map[string]any
	_ = json.NewDecoder(reader).Decode(&envelope)
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, resource := range envelope["resourceSpans"].([]any) {
		for _, scope := range resource.(map[string]any)["scopeSpans"].([]any) {
			for _, span := range scope.(map[string]any)["spans"].([]any) {
				for _, attr := range span.(map[string]any)["attributes"].([]any) {
					a := attr.(map[string]any)
					if a["key"] != "bitfab.payload" {
						continue
					}
					var payload map[string]any
					_ = json.Unmarshal([]byte(a["value"].(map[string]any)["stringValue"].(string)), &payload)
					raw, ok := payload["rawSpan"].(map[string]any)
					if !ok {
						continue
					}
					key, _ := payload["traceFunctionKey"].(string)
					c.spans[key] = append(c.spans[key], raw)
				}
			}
		}
	}
	_, _ = w.Write([]byte("{}"))
}

func (c *collected) trace(key string) map[string]map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	byName := map[string]map[string]any{}
	for _, raw := range c.spans[key] {
		data := raw["span_data"].(map[string]any)
		data["id"] = raw["id"]
		data["parent_id"] = raw["parent_id"]
		byName[data["name"].(string)] = data
	}
	return byName
}

func check(ok bool, format string, args ...any) {
	if !ok {
		panic(fmt.Sprintf(format, args...))
	}
}

func contexts(span map[string]any) []any {
	values, _ := span["contexts"].([]any)
	return values
}

func checkJevContext(span map[string]any, model string, input, output float64) {
	values := contexts(span)
	check(len(values) == 1, "expected one context on %v, got %v", span["name"], values)
	entry := values[0].(map[string]any)
	_, timed := entry["durationMs"].(float64)
	check(timed && entry["provider"] == "typesafe" && entry["model"] == model && entry["inputTokens"] == input && entry["outputTokens"] == output, "wrong context %v", entry)
}

func main() {
	sink := &collected{spans: map[string][]map[string]any{}}
	server := httptest.NewServer(http.HandlerFunc(sink.handler))
	defer server.Close()
	client := bitfab.NewClient("test", bitfab.WithServiceURL(server.URL), bitfab.WithSimulationPlan(false))
	configured := bitfab.NewClient("test", bitfab.WithServiceURL(server.URL), bitfab.WithSimulationPlan(false))
	if err := configured.Node((*jevclient.Client).SystemOne, bitfab.NodeOptions{Name: "ask-jev"}); err != nil {
		panic(err)
	}
	third := &jevclient.Client{}
	own := &ownClient{prefix: "p:"}
	run := func(c *bitfab.Client, key string, opts bitfab.TraceOptions, fn func(context.Context) (any, error)) (any, error) {
		opts.Name = key
		opts.Exclude = append(opts.Exclude, "check")
		return c.Trace(context.Background(), key, fn, opts)
	}
	_, _ = run(client, "own", bitfab.TraceOptions{}, func(ctx context.Context) (any, error) {
		reply, err := own.SystemOne(ctx, "hi")
		check(err == nil && reply["answer"] == "prepared p:hi", "own reply changed: %v %v", reply, err)
		return nil, nil
	})
	_, _ = run(client, "third", bitfab.TraceOptions{}, func(ctx context.Context) (any, error) {
		reply, err := third.SystemOne(ctx, "hi")
		check(err == nil && reply.Answer == "yes hi", "third reply changed: %v %v", reply, err)
		var through asker = third
		reply, err = through.SystemOne(ctx, "again")
		check(err == nil && reply.Answer == "yes again", "interface reply changed: %v %v", reply, err)
		return nil, nil
	})
	_, _ = run(client, "plain", bitfab.TraceOptions{}, func(ctx context.Context) (any, error) {
		reply, err := jevclient.Plain{}.SystemOne(ctx, "hi")
		check(err == nil && reply["answer"] == "hi", "plain reply changed: %v %v", reply, err)
		return nil, nil
	})
	_, _ = run(client, "fail", bitfab.TraceOptions{}, func(ctx context.Context) (any, error) {
		_, err := third.SystemOne(ctx, "fail")
		check(err != nil && err.Error() == "jev unavailable", "error changed: %v", err)
		return nil, nil
	})
	wrapped := bitfab.WrapJev(func(ctx context.Context, prompt string) (jevclient.Reply, error) {
		return third.SystemOne(ctx, prompt)
	})
	_, _ = run(client, "wrapped", bitfab.TraceOptions{}, func(ctx context.Context) (any, error) {
		reply, err := wrapped(ctx, "hi")
		check(err == nil && reply.Answer == "yes hi", "wrapped reply changed: %v %v", reply, err)
		return nil, nil
	})
	_, _ = run(client, "excluded", bitfab.TraceOptions{Exclude: []string{"SystemOne"}}, func(ctx context.Context) (any, error) {
		_, err := third.SystemOne(ctx, "hi")
		return nil, err
	})
	_, _ = run(client, "excluded-wrapped", bitfab.TraceOptions{Exclude: []string{"SystemOne"}}, func(ctx context.Context) (any, error) {
		_, err := wrapped(ctx, "hi")
		return nil, err
	})
	_, _ = run(configured, "configured", bitfab.TraceOptions{}, func(ctx context.Context) (any, error) {
		_, err := third.SystemOne(ctx, "hi")
		return nil, err
	})
	_, _ = third.SystemOne(context.Background(), "untraced")
	check(client.FlushTraces(5e9) && configured.FlushTraces(5e9), "flush failed")

	spans := sink.trace("own")
	llm := spans["(*ownClient).SystemOne"]
	check(len(spans) == 3 && llm != nil && llm["type"] == "llm", "own spans: %v", spans)
	checkJevContext(llm, "jev-own", 3, 4)
	check(spans["prepare"]["parent_id"] == llm["id"] && llm["parent_id"] == spans["own"]["id"], "own parentage: %v", spans)

	spans = sink.trace("third")
	llm = spans["(*Client).SystemOne"]
	through := spans["asker.SystemOne"]
	check(len(spans) == 3 && llm != nil && llm["type"] == "llm" && through != nil && through["type"] == "llm", "third spans: %v", spans)
	checkJevContext(llm, "jev-1", 12, 5)
	checkJevContext(through, "jev-1", 12, 5)
	check(len(contexts(spans["third"])) == 0, "root got a context: %v", spans["third"])

	spans = sink.trace("plain")
	llm = spans["Plain.SystemOne"]
	check(len(spans) == 2 && llm != nil && llm["type"] == "llm" && len(contexts(llm)) == 0, "plain spans: %v", spans)

	spans = sink.trace("fail")
	llm = spans["(*Client).SystemOne"]
	check(len(spans) == 2 && llm != nil && llm["error"] == "jev unavailable" && len(contexts(llm)) == 0, "fail spans: %v", spans)

	spans = sink.trace("wrapped")
	llm = spans["(*Client).SystemOne"]
	check(len(spans) == 2 && llm != nil, "wrapped spans: %v", spans)
	checkJevContext(llm, "jev-1", 12, 5)
	check(len(contexts(spans["wrapped"])) == 0, "WrapJev recorded twice: %v", spans["wrapped"])

	spans = sink.trace("excluded")
	check(len(spans) == 1 && len(contexts(spans["excluded"])) == 0, "excluded spans: %v", spans)

	spans = sink.trace("excluded-wrapped")
	check(len(spans) == 1, "excluded wrapped spans: %v", spans)
	checkJevContext(spans["excluded-wrapped"], "jev-1", 12, 5)

	spans = sink.trace("configured")
	llm = spans["ask-jev"]
	check(len(spans) == 2 && llm != nil && llm["type"] == "llm", "configured spans: %v", spans)
	checkJevContext(llm, "jev-1", 12, 5)

	fmt.Println("jev capture passed")
}
`
