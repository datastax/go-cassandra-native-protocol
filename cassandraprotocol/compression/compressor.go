package compression

import "io"

type MessageCompressor interface {
	Compress(uncompressedMessage io.Reader) (compressedMessage []byte, err error)
	Decompress(compressedMessage io.Reader) (decompressedMessage []byte, err error)
}
