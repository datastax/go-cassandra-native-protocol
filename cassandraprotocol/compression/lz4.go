package compression

import (
	"errors"
	"github.com/pierrec/lz4"
)

type Lz4Compressor struct{}

func (l Lz4Compressor) Compress(uncompressedMessage []byte) ([]byte, error) {
	uncompressedLength := len(uncompressedMessage)
	compressedMessage := make([]byte, uncompressedLength)
	ht := make([]int, 64<<10) // buffer for the compression table
	n, err := lz4.CompressBlock(uncompressedMessage, compressedMessage, ht)
	if err != nil {
		return nil, err
	}
	if n >= uncompressedLength {
		return nil, errors.New("incompressible buffer")
	}
	return compressedMessage[:n], nil
}

func (l Lz4Compressor) Decompress(compressedMessage []byte) ([]byte, error) {
	decompressedMessage := make([]byte, 10*len(compressedMessage))
	n, err := lz4.UncompressBlock(compressedMessage, decompressedMessage)
	if err != nil {
		return nil, err
	}
	return decompressedMessage[:n], nil
}
