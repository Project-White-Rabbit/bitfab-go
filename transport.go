package bitfab

import "time"

func createTraceTransport(directSender directBatchSender, onDelivered deliveredCarrierListener) traceTransport {
	return createOtelTransport(directSender, onDelivered)
}

func flushTraceTransports(timeout time.Duration) bool {
	return flushOtelTransports(timeout)
}

func shutdownTraceTransports(timeout time.Duration) bool {
	return shutdownOtelTransports(timeout)
}
