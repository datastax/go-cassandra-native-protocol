package compression

import (
	"bytes"
	"github.com/golang/snappy"
)

type SnappyCompressor struct{}

func (l SnappyCompressor) Algorithm() string {
	return "SNAPPY"
}

func (l SnappyCompressor) Compress(uncompressedMessage *bytes.Buffer) (*bytes.Buffer, error) {
	compressedMessage := snappy.Encode(nil, uncompressedMessage.Bytes())
	return bytes.NewBuffer(compressedMessage), nil
}

func (l SnappyCompressor) Decompress(compressedMessage *bytes.Buffer) (*bytes.Buffer, error) {
	decompressedMessage, err := snappy.Decode(nil, compressedMessage.Bytes())
	if err != nil {
		return nil, err
	}
	return bytes.NewBuffer(decompressedMessage), nil
}
