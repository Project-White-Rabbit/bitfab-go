package bitfab

// DefaultServiceURL is the default Bitfab API base URL.
const DefaultServiceURL = "https://bitfab.ai"

// Version is the SDK version string sent with every API request.
const Version = "0.12.2"

// Valid span types matching the backend enum.
var validSpanTypes = map[string]bool{
	"llm":       true,
	"agent":     true,
	"function":  true,
	"guardrail": true,
	"handoff":   true,
	"custom":    true,
}
