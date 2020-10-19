package compression

type MessageCompressor interface {
	Compress(uncompressedMessage []byte) (compressedMessage []byte, err error)
	Decompress(compressedMessage []byte) (decompressedMessage []byte, err error)
}
