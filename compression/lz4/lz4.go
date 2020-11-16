// Copyright 2020 DataStax
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lz4

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/pierrec/lz4/v4"
	"io"
	"io/ioutil"
)

// BodyCompressor satisfies frame.BodyCompressor for the LZ4 algorithm.
// Note: Cassandra expects lz4-compressed bodies to start with a 4-byte integer holding the decompressed message length.
// The Go implementation of lz4 used here does not include that, so we need to do it manually when encoding and
// decoding.
type BodyCompressor struct{}

func (l BodyCompressor) Algorithm() string {
	return "LZ4"
}

func (l BodyCompressor) Compress(source io.Reader, dest io.Writer) error {
	var uncompressedMessage *bytes.Buffer
	switch s := source.(type) {
	case *bytes.Buffer:
		uncompressedMessage = s
	default:
		uncompressedMessage = &bytes.Buffer{}
		if _, err := uncompressedMessage.ReadFrom(s); err != nil {
			return fmt.Errorf("cannot read uncompressed body: %w", err)
		}
	}
	maxCompressedSize := lz4.CompressBlockBound(uncompressedMessage.Len())
	// allocate enough space for the max compressed size + 4 bytes for the decompressed length
	compressedMessage := make([]byte, maxCompressedSize+4)
	// write the decompressed length in the 4 first bytes
	binary.BigEndian.PutUint32(compressedMessage, uint32(uncompressedMessage.Len()))
	// compress the message and write the result to the destination buffer starting at offset 4;
	// note that for empty messages, this results in a single byte being written and written = 1;
	// this is normal and is what Cassandra expects for empty compressed messages.
	if written, err := lz4.CompressBlock(uncompressedMessage.Bytes(), compressedMessage[4:], nil); err != nil {
		return fmt.Errorf("cannot compress body: %w", err)
		// cap the compressed buffer at n + 4 bytes
	} else if _, err := dest.Write(compressedMessage[:written+4]); err != nil {
		return fmt.Errorf("cannot write compressed body: %w", err)
	}
	return nil
}

func (l BodyCompressor) Decompress(source io.Reader, dest io.Writer) error {
	// read the decompressed length first
	var decompressedLength uint32
	if err := binary.Read(source, binary.BigEndian, &decompressedLength); err != nil {
		return fmt.Errorf("cannot read compressed length: %w", err)
	} else {
		// if decompressed length is zero, the remaining buffer will contain a single byte that should be discarded
		if decompressedLength == 0 {
			if _, err = io.CopyN(ioutil.Discard, source, 1); err != nil {
				return fmt.Errorf("cannot read empty body: %w", err)
			}
			return nil
		}
		// decompress the message
		var compressedMessage *bytes.Buffer
		switch s := source.(type) {
		case *bytes.Buffer:
			compressedMessage = s
		default:
			compressedMessage = &bytes.Buffer{}
			if _, err := compressedMessage.ReadFrom(s); err != nil {
				return fmt.Errorf("cannot read compressed body: %w", err)
			}
		}
		compressedLength := compressedMessage.Len()
		remaining := compressedMessage.Bytes()
		// try destination buffers of increased length to avoid allocating too much space, starting with twice the
		// compressed length and up to eight times the compressed length
		var decompressedMessage []byte
		var written int
		for i := compressedLength * 2; i <= compressedLength*8; i *= 2 {
			decompressedMessage = make([]byte, i)
			if written, err = lz4.UncompressBlock(remaining, decompressedMessage); err == nil {
				break
			}
		}
		if err != nil {
			return fmt.Errorf("cannot decompress body: %w", err)
		} else if written != int(decompressedLength) {
			return fmt.Errorf("decompressed length mismatch, expected %d, got: %d", decompressedLength, written)
		} else if _, err := dest.Write(decompressedMessage[:written]); err != nil {
			return fmt.Errorf("cannot write decompressed body: %w", err)
		}
		return nil
	}
}
