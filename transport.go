package bitfab

import "time"

func createTraceTransport(apiKey apiKeyResolver, directSender directBatchSender) traceTransport {
	return createOtelTransport(apiKey, directSender)
}

func flushTraceTransports(timeout time.Duration) bool {
	return flushOtelTransports(timeout)
}

func shutdownTraceTransports(timeout time.Duration) bool {
	return shutdownOtelTransports(timeout)
}
