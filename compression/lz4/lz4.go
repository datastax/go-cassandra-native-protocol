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
	"encoding/binary"
	"fmt"
	"github.com/pierrec/lz4/v4"
)

// Compressor satisfies compression.Compressor for the LZ4 algorithm.
// Note: Cassandra expects lz4-compressed bodies to start with a 4-byte integer holding the decompressed message length.
// The Go implementation of lz4 used here does not include that, so we need to do it manually when encoding and
// decoding.
type Compressor struct{}

func (c Compressor) CompressFrame(uncompressed []byte) ([]byte, error) {
	maxCompressedSize := lz4.CompressBlockBound(len(uncompressed))
	// allocate enough space for the max compressed size + 4 bytes for the decompressed length
	const SizeOfLength = 4
	compressed := make([]byte, maxCompressedSize+SizeOfLength)
	// write the decompressed length in the 4 first bytes
	binary.BigEndian.PutUint32(compressed, uint32(len(uncompressed)))
	// compress the message and write the result to the destination buffer starting at offset 4;
	// note that for empty messages, this results in a single byte being written and written = 1;
	// this is normal and is what Cassandra expects for empty compressed messages.
	written, err := lz4.CompressBlock(uncompressed, compressed[SizeOfLength:], nil)
	if err != nil {
		return nil, fmt.Errorf("cannot compress message: %w", err)
	}
	return compressed[:written+SizeOfLength], nil
}

func (c Compressor) DecompressFrame(compressed []byte) ([]byte, error) {
	// read the decompressed length first
	const SizeOfLength = 4
	if len(compressed) < SizeOfLength {
		return nil, fmt.Errorf("cannot read compressed length")
	}
	decompressedLength := binary.BigEndian.Uint32(compressed)
	if decompressedLength == 0 {
		// if decompressed length is zero, the remaining buffer will contain a single byte that should be discarded
		return []byte{}, nil
	}
	uncompressed := make([]byte, decompressedLength)
	written, err := lz4.UncompressBlock(compressed[SizeOfLength:], uncompressed)
	if err != nil {
		return nil, err
	}
	if uint32(written) != decompressedLength {
		return nil, fmt.Errorf("decompressed size mismatch, expected %d, actual %d", decompressedLength, written)
	}
	return uncompressed[:written], nil
}

func (c Compressor) CompressSegment(uncompressed []byte) ([]byte, error) {
	maxCompressedSize := lz4.CompressBlockBound(len(uncompressed))
	// allocate enough space for the max compressed size
	compressed := make([]byte, maxCompressedSize)
	// note that for empty slice, this results in a single byte being written and written = 1;
	// this is normal and is what Cassandra expects for empty compressed messages.
	written, err := lz4.CompressBlock(uncompressed, compressed, nil)
	if err != nil {
		return nil, fmt.Errorf("cannot compress message: %w", err)
	}
	return compressed[:written], nil
}

func (c Compressor) DecompressSegment(compressed []byte, uncompressed []byte) error {
	ul := len(uncompressed)
	if ul == 0 {
		return nil
	}
	written, err := lz4.UncompressBlock(compressed, uncompressed)
	if err != nil {
		return err
	}
	if written != ul {
		return fmt.Errorf("decompressed size mismatch, expected %d, actual %d", ul, written)
	}
	return nil
}
