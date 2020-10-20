package compression

import (
	"bytes"
	"errors"
	"github.com/pierrec/lz4"
)

type Lz4Compressor struct{}

func (l Lz4Compressor) Compress(uncompressedMessage *bytes.Buffer) (*bytes.Buffer, error) {
	uncompressedLength := uncompressedMessage.Len()
	compressedMessage := make([]byte, uncompressedLength)
	ht := make([]int, 64<<10) // buffer for the compression table
	n, err := lz4.CompressBlock(uncompressedMessage.Bytes(), compressedMessage, ht)
	if err != nil {
		return nil, err
	}
	if n >= uncompressedLength {
		return nil, errors.New("incompressible buffer")
	}
	return bytes.NewBuffer(compressedMessage[:n]), nil
}

func (l Lz4Compressor) Decompress(compressedMessage *bytes.Buffer) (*bytes.Buffer, error) {
	decompressedMessage := make([]byte, 10*compressedMessage.Len())
	n, err := lz4.UncompressBlock(compressedMessage.Bytes(), decompressedMessage)
	if err != nil {
		return nil, err
	}
	return bytes.NewBuffer(decompressedMessage[:n]), nil
}
