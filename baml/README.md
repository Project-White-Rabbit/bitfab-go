# Native BAML adapter

This optional Go module enables local BAML execution and generated-client collector wrappers without adding native dependencies to ordinary Bitfab imports. It uses upstream BAML 0.225.0 and requires cgo and a platform supported by its native library.

```go
client := bitfab.NewClient(apiKey, bitfab.WithBAMLExecutor(bamladapter.Execute))
result, err := client.Call(ctx, "function-name", map[string]any{"text": "hello"})
```

For generated functions, use their `NewCollector` factory and `WithCollector` option. The typed wrapper preserves the argument and result types.

```go
wrapped := bamladapter.WrapBAML(
    baml_client.NewCollector,
    func(ctx context.Context, text string, collector native.Collector) (string, error) {
        return baml_client.Summarize(ctx, text, baml_client.WithCollector(collector))
    },
    nil,
)
result, err := wrapped.Call(ctx, "hello")
collector := wrapped.Collector()
```

Here `native` imports `github.com/boundaryml/baml/engine/language_client_go/pkg`. Run the wrapper inside a Bitfab span or node to record rendered prompt, model, provider, token counts, and duration. The third argument is an optional callback receiving the native collector after each successful invocation.

Run native integration checks with `go test -race ./...` in this directory. They use a local HTTP server and require no provider credentials. The module replacements target the current monorepo checkout for local development.
