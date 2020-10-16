package compression

type MessageCompressor interface {
	Compress(uncompressedMessage []byte) ([]byte, error)
	Decompress(compressedMessage []byte) ([]byte, error)
}
