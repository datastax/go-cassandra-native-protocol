package snappy

import (
	"bytes"
	"github.com/golang/snappy"
)

type Compressor struct{}

func (l Compressor) Algorithm() string {
	return "SNAPPY"
}

func (l Compressor) Compress(uncompressedMessage *bytes.Buffer) (*bytes.Buffer, error) {
	compressedMessage := snappy.Encode(nil, uncompressedMessage.Bytes())
	return bytes.NewBuffer(compressedMessage), nil
}

func (l Compressor) Decompress(compressedMessage *bytes.Buffer) (*bytes.Buffer, error) {
	decompressedMessage, err := snappy.Decode(nil, compressedMessage.Bytes())
	if err != nil {
		return nil, err
	}
	return bytes.NewBuffer(decompressedMessage), nil
}
