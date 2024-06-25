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

package snappy

import (
	"bytes"
	"fmt"
	"io"

	"github.com/golang/snappy"
)

// Compressor satisfies frame.BodyCompressor for the SNAPPY algorithm.
type Compressor struct{}

func (l Compressor) CompressWithLength(source io.Reader, dest io.Writer) error {
	if uncompressedMessage, err := bufferFromReader(source); err != nil {
		return fmt.Errorf("cannot read uncompressed message: %w", err)
	} else {
		compressedMessage := snappy.Encode(nil, uncompressedMessage.Bytes())
		if _, err := dest.Write(compressedMessage); err != nil {
			return fmt.Errorf("cannot write compressed message: %w", err)
		}
		return nil
	}
}

func (l Compressor) DecompressWithLength(source io.Reader, dest io.Writer) error {
	if compressedMessage, err := bufferFromReader(source); err != nil {
		return fmt.Errorf("cannot read compressed message: %w", err)
	} else {
		if decompressedMessage, err := snappy.Decode(nil, compressedMessage.Bytes()); err != nil {
			return fmt.Errorf("cannot decompress message: %w", err)
		} else if _, err := dest.Write(decompressedMessage); err != nil {
			return fmt.Errorf("cannot write decompressed message: %w", err)
		}
		return nil
	}
}

func bufferFromReader(source io.Reader) (*bytes.Buffer, error) {
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
	return buf, nil
}
