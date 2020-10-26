package compression

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/pierrec/lz4"
)

// Lz4Compressor satisfies MessageCompressor for the LZ4 algorithm.
// Note: Cassandra expects lz4-compressed bodies to start with a 4-byte integer holding the decompressed message length.
// The Go implementation of lz4 used here does not include that, so we need to do it manually when encoding and
// decoding.
type Lz4Compressor struct{}

func (l Lz4Compressor) Algorithm() string {
	return "LZ4"
}

func (l Lz4Compressor) Compress(uncompressedMessage *bytes.Buffer) (*bytes.Buffer, error) {
	maxCompressedSize := lz4.CompressBlockBound(uncompressedMessage.Len())
	// allocate enough space for the max compressed size + 4 bytes for the decompressed length
	compressedMessage := make([]byte, maxCompressedSize+4)
	// write the decompressed length in the 4 first bytes
	binary.BigEndian.PutUint32(compressedMessage, uint32(uncompressedMessage.Len()))
	// compress the message and write the result to the destination buffer starting at offset 4;
	// note that for empty messages, this results in a single byte being written and written = 1;
	// this is normal and is what Cassandra expects for empty compressed messages.
	if written, err := lz4.CompressBlock(uncompressedMessage.Bytes(), compressedMessage[4:], nil); err != nil {
		return nil, err
	} else {
		// cap the compressed buffer at n + 4 bytes
		return bytes.NewBuffer(compressedMessage[:written+4]), nil
	}
}

func (l Lz4Compressor) Decompress(compressedMessage *bytes.Buffer) (*bytes.Buffer, error) {
	// read the decompressed length first
	var decompressedLength uint32
	if err := binary.Read(compressedMessage, binary.BigEndian, &decompressedLength); err != nil {
		return nil, err
	} else {
		// if decompressed length is zero, the remaining buffer will contain a single byte that should be discarded
		if decompressedLength == 0 {
			return &bytes.Buffer{}, nil
		}
		// decompress the message
		var decompressedMessage []byte
		var written int
		compressedLength := compressedMessage.Len()
		remaining := compressedMessage.Bytes()
		// try destination buffers of increased length to avoid allocating too much space, starting with twice the
		// compressed length and up to eight times the compressed length
		for i := compressedLength * 2; i <= compressedLength*8; i *= 2 {
			decompressedMessage = make([]byte, i)
			if written, err = lz4.UncompressBlock(remaining, decompressedMessage); err == nil {
				break
			}
		}
		if err != nil {
			return nil, err
		} else if written != int(decompressedLength) {
			return nil, fmt.Errorf("decompressed body length mismatch, expected %d, got: %d", decompressedLength, written)
		} else {
			return bytes.NewBuffer(decompressedMessage[:written]), nil
		}
	}
}
