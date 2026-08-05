package bitfab

import "time"

func createTraceTransport(directSender directBatchSender) traceTransport {
	return createOtelTransport(directSender)
}

func flushTraceTransports(timeout time.Duration) bool {
	return flushOtelTransports(timeout)
}

func shutdownTraceTransports(timeout time.Duration) bool {
	return shutdownOtelTransports(timeout)
}
