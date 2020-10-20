package compression

import (
	"bytes"
)

type MessageCompressor interface {
	Compress(uncompressedMessage *bytes.Buffer) (compressedMessage *bytes.Buffer, err error)
	Decompress(compressedMessage *bytes.Buffer) (decompressedMessage *bytes.Buffer, err error)
}
