package compression

import (
	"bytes"
	"github.com/pierrec/lz4"
)

type Lz4Compressor struct{}

func (l Lz4Compressor) Compress(uncompressedMessage *bytes.Buffer) (*bytes.Buffer, error) {
	compressedMessage := make([]byte, lz4.CompressBlockBound(uncompressedMessage.Len()))
	n, err := lz4.CompressBlock(uncompressedMessage.Bytes(), compressedMessage, nil)
	if err != nil {
		return nil, err
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
