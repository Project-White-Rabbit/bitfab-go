package bitfab

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// BAMLProvider describes the provider clients available to a stored prompt.
type BAMLProvider struct {
	Provider  string      `json:"provider"`
	APIKeyEnv string      `json:"apiKeyEnv"`
	Models    []BAMLModel `json:"models"`
}

// BAMLModel identifies one provider model.
type BAMLModel struct {
	Model       string `json:"model"`
	Description string `json:"description"`
}

// BAMLRequest contains a prepared source and coerced inputs for local execution.
type BAMLRequest struct {
	Source       string
	FunctionName string
	Inputs       map[string]any
	EnvVars      map[string]string
}

// BAMLResult preserves parsed output and the native execution collector.
type BAMLResult struct {
	Result       any
	RawCollector map[string]any
}

// BAMLExecutor executes prepared BAML locally. The optional baml adapter supplies the native implementation.
type BAMLExecutor func(context.Context, BAMLRequest) (BAMLResult, error)

// WithBAMLExecutor enables local BAML execution without imposing a native dependency on core imports.
func WithBAMLExecutor(executor BAMLExecutor) Option {
	return func(c *Client) { c.bamlExecutor = executor }
}

// WithEnvVars supplies provider credentials for local execution. Only OPENAI_API_KEY is forwarded, matching canonical SDKs.
func WithEnvVars(env map[string]string) Option {
	return func(c *Client) {
		c.bamlEnv = make(map[string]string, len(env))
		for k, v := range env {
			c.bamlEnv[k] = v
		}
	}
}

var bamlFunctionPattern = regexp.MustCompile(`function\s+(\w+)\s*\(([^)]*)\)\s*->`)

// PrepareBAML applies canonical provider definitions, input coercion, and credential filtering.
func PrepareBAML(source string, inputs map[string]any, providers []BAMLProvider, env map[string]string) (BAMLRequest, error) {
	match := bamlFunctionPattern.FindStringSubmatch(source)
	if match == nil {
		return BAMLRequest{}, fmt.Errorf("bitfab: no function found in BAML source")
	}
	types := map[string]string{}
	depth, start := 0, 0
	signature := match[2] + ","
	for i, char := range signature {
		switch char {
		case '<':
			depth++
		case '>':
			depth--
		case ',':
			if depth == 0 {
				part := strings.SplitN(signature[start:i], ":", 2)
				if len(part) == 2 {
					types[strings.TrimSpace(part[0])] = strings.TrimSuffix(strings.TrimSpace(part[1]), "?")
				}
				start = i + 1
			}
		}
	}
	coerced := make(map[string]any, len(inputs))
	for k, v := range inputs {
		if text, ok := v.(string); ok && types[k] != "" {
			v = coerceBAMLInput(text, types[k])
		}
		coerced[k] = v
	}
	if !strings.Contains(source, "client<llm> OpenAI_") {
		var clients []string
		for _, p := range providers {
			for _, m := range p.Models {
				prefix := map[string]string{"openai": "OpenAI", "anthropic": "Anthropic", "google": "Google"}[p.Provider]
				if prefix == "" {
					prefix = p.Provider
					if len(prefix) > 0 {
						prefix = strings.ToUpper(prefix[:1]) + prefix[1:]
					}
				}
				modelName := m.Model
				if strings.HasPrefix(modelName, "gpt-") {
					modelName = "GPT" + strings.TrimPrefix(modelName, "gpt-")
				}
				modelName = strings.NewReplacer(".", "_", "-", "_").Replace(modelName)
				temp := ""
				if p.Provider == "openai" && (strings.HasPrefix(m.Model, "gpt-4.1") || strings.HasPrefix(m.Model, "gpt-4o")) {
					temp = "\n    temperature 0"
				}
				quoted, _ := json.Marshal(m.Model)
				clients = append(clients, fmt.Sprintf("client<llm> %s_%s {\n  provider %s\n  options {\n    model %s\n    api_key env.%s%s\n  }\n}", prefix, modelName, p.Provider, quoted, p.APIKeyEnv, temp))
			}
		}
		source = strings.Join(clients, "\n\n") + "\n\n" + source
	}
	filtered := map[string]string{}
	if value, ok := env["OPENAI_API_KEY"]; ok {
		filtered["OPENAI_API_KEY"] = value
	}
	return BAMLRequest{Source: source, FunctionName: match[1], Inputs: coerced, EnvVars: filtered}, nil
}

func coerceBAMLInput(value, kind string) any {
	switch kind {
	case "string":
		return value
	case "int":
		if v, e := strconv.ParseInt(strings.TrimSpace(value), 10, 64); e == nil {
			return v
		}
	case "float":
		if v, e := strconv.ParseFloat(strings.TrimSpace(value), 64); e == nil {
			return v
		}
	case "bool":
		if strings.EqualFold(value, "true") {
			return true
		}
		if strings.EqualFold(value, "false") {
			return false
		}
	default:
		var parsed any
		if json.Unmarshal([]byte(value), &parsed) == nil {
			if strings.HasSuffix(kind, "[]") {
				if _, ok := parsed.([]any); !ok {
					return value
				}
			}
			return parsed
		}
	}
	return value
}

// Call fetches a stored function's BAML prompt and executes it locally.
func (c *Client) Call(ctx context.Context, methodName string, inputs map[string]any) (any, error) {
	if c.bamlExecutor == nil {
		return nil, fmt.Errorf("bitfab: local BAML execution requires WithBAMLExecutor; use the optional bitfab-go/baml native adapter")
	}
	version, err := c.httpClient.request(ctx, "/api/sdk/functions/lookup", map[string]any{"name": methodName}, c.requestTimeout)
	if err != nil {
		return nil, err
	}
	id, _ := version["id"].(string)
	source, _ := version["prompt"].(string)
	if id == "" {
		return nil, fmt.Errorf("bitfab: function %q not found; create it at %s/functions", methodName, c.serviceURL)
	}
	if source == "" {
		return nil, fmt.Errorf("bitfab: function %q has no prompt configured", methodName)
	}
	var providers []BAMLProvider
	providerJSON, err := json.Marshal(version["providers"])
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(providerJSON, &providers); err != nil {
		return nil, fmt.Errorf("bitfab: invalid BAML provider definitions: %w", err)
	}
	env := c.bamlEnv
	if env == nil {
		env = map[string]string{}
		if value, ok := os.LookupEnv("OPENAI_API_KEY"); ok {
			env["OPENAI_API_KEY"] = value
		}
	}
	request, err := PrepareBAML(source, inputs, providers, env)
	if err != nil {
		return nil, err
	}
	execution, err := c.bamlExecutor(ctx, request)
	if err != nil {
		return nil, err
	}
	output, ok := execution.Result.(string)
	if !ok {
		encoded, e := json.Marshal(execution.Result)
		if e != nil {
			output = fmt.Sprint(execution.Result)
		} else {
			output = string(encoded)
		}
	}
	payload := map[string]any{"functionId": id, "result": output, "source": "go-sdk"}
	if len(inputs) > 0 {
		payload["inputs"] = inputs
	}
	if execution.RawCollector != nil {
		payload["rawCollector"] = execution.RawCollector
	}
	c.httpClient.submit(operationInternalTrace, payload, carrierMeta{})
	return execution.Result, nil
}
