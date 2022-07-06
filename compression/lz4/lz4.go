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
	"io"
	"io/ioutil"

	"github.com/pierrec/lz4/v4"
)

// Compressor satisfies frame.BodyCompressor and segment.PayloadCompressor for the LZ4 algorithm.
// Note: Cassandra expects lz4-compressed bodies to start with a 4-byte integer holding the decompressed message length.
// The Go implementation of lz4 used here does not include that, so we need to do it manually when encoding and
// decoding.
type Compressor struct{}

func (c Compressor) Compress(source io.Reader, dest io.Writer) error {
	if uncompressedMessage, err := bufferFromReader(source); err != nil {
		return fmt.Errorf("cannot read uncompressed message: %w", err)
	} else {
		maxCompressedSize := lz4.CompressBlockBound(len(uncompressedMessage))
		// allocate enough space for the max compressed size
		compressedMessage := make([]byte, maxCompressedSize)
		// compress the message and write the result to the destination buffer;
		// note that for empty messages, this results in a single byte being written and written = 1;
		// this is normal and is what Cassandra expects for empty compressed messages.
		if written, err := lz4.CompressBlock(uncompressedMessage, compressedMessage, nil); err != nil {
			return fmt.Errorf("cannot compress message: %w", err)
		} else if _, err := dest.Write(compressedMessage[:written]); err != nil {
			return fmt.Errorf("cannot write compressed message: %w", err)
		}
		return nil
	}
}

func (c Compressor) CompressWithLength(source io.Reader, dest io.Writer) error {
	if uncompressedMessage, err := bufferFromReader(source); err != nil {
		return err
	} else {
		maxCompressedSize := lz4.CompressBlockBound(len(uncompressedMessage))
		// allocate enough space for the max compressed size + 4 bytes for the decompressed length
		const SizeOfLength = 4
		compressedMessage := make([]byte, maxCompressedSize+SizeOfLength)
		// write the decompressed length in the 4 first bytes
		binary.BigEndian.PutUint32(compressedMessage, uint32(len(uncompressedMessage)))
		// compress the message and write the result to the destination buffer starting at offset 4;
		// note that for empty messages, this results in a single byte being written and written = 1;
		// this is normal and is what Cassandra expects for empty compressed messages.
		if written, err := lz4.CompressBlock(uncompressedMessage, compressedMessage[SizeOfLength:], nil); err != nil {
			return fmt.Errorf("cannot compress message: %w", err)
		} else if _, err := dest.Write(compressedMessage[:written+SizeOfLength]); err != nil {
			return fmt.Errorf("cannot write compressed message: %w", err)
		}
		return nil
	}
}

func (c Compressor) Decompress(source io.Reader, dest io.Writer) error {
	if compressedMessage, err := bufferFromReader(source); err != nil {
		return fmt.Errorf("cannot read compressed message: %w", err)
	} else if decompressedMessage, err := decompress(compressedMessage); err != nil {
		return fmt.Errorf("cannot decompress message: %w", err)
	} else if _, err := dest.Write(decompressedMessage); err != nil {
		return fmt.Errorf("cannot write decompressed message: %w", err)
	}
	return nil
}

func (c Compressor) DecompressWithLength(source io.Reader, dest io.Writer) error {
	// read the decompressed length first
	var decompressedLength uint32
	if err := binary.Read(source, binary.BigEndian, &decompressedLength); err != nil {
		return fmt.Errorf("cannot read compressed length: %w", err)
	} else if decompressedLength == 0 {
		// if decompressed length is zero, the remaining buffer will contain a single byte that should be discarded
		if _, err = io.CopyN(ioutil.Discard, source, 1); err != nil {
			return fmt.Errorf("cannot read empty message: %w", err)
		}
		return nil
	}
	return c.Decompress(source, dest)
}

func decompress(source []byte) (dest []byte, err error) {
	// try destination buffers of increased length to avoid allocating too much space, starting with twice the
	// compressed length and up to eight times the compressed length
	compressedLength := len(source)
	var written int
	for i := compressedLength * 2; i <= compressedLength*8; i *= 2 {
		dest = make([]byte, i)
		if written, err = lz4.UncompressBlock(source, dest); err == nil {
			break
		}
	}
	return dest[:written], err
}

func bufferFromReader(source io.Reader) ([]byte, error) {
	var buf *bytes.Buffer
	switch s := source.(type) {
	case *bytes.Buffer:
		buf = s
	default:
		buf = &bytes.Buffer{}
		if _, err := buf.ReadFrom(s); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}
