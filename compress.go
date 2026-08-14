package bitfab

import (
	"bytes"
	"compress/gzip"
	"os"
)

const disableCompressionEnv = "BITFAB_DISABLE_COMPRESSION"

// minCompressedBytes is the size below which compressing costs more than the
// saved bytes are worth, so small requests (function lookups, replay status
// polls, single-span batches) ride uncompressed.
const minCompressedBytes = 8192

type preparedRequest struct {
	body            []byte
	contentEncoding string
	rawBytes        int
	wireBytes       int
}

func prepareRequestBody(body []byte) preparedRequest {
	encoded, contentEncoding := encodeRequestBody(body)
	return preparedRequest{
		body:            encoded,
		contentEncoding: contentEncoding,
		rawBytes:        len(body),
		wireBytes:       len(encoded),
	}
}

// encodeRequestBody returns the body to send and the Content-Encoding it
// carries, or an empty encoding when the body is sent as-is. Compression is
// best-effort: any failure sends the original body rather than dropping the
// span.
func encodeRequestBody(body []byte) ([]byte, string) {
	if os.Getenv(disableCompressionEnv) != "" {
		return body, ""
	}
	if len(body) < minCompressedBytes {
		return body, ""
	}

	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(body); err != nil {
		return body, ""
	}
	if err := writer.Close(); err != nil {
		return body, ""
	}
	if compressed.Len() >= len(body) {
		return body, ""
	}
	return compressed.Bytes(), "gzip"
}
