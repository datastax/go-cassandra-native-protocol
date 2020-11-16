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
	"github.com/golang/snappy"
	"io"
)

// BodyCompressor satisfies frame.BodyCompressor for the SNAPPY algorithm.
type BodyCompressor struct{}

func (l BodyCompressor) Algorithm() string {
	return "SNAPPY"
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
	compressedMessage := snappy.Encode(nil, uncompressedMessage.Bytes())
	if _, err := dest.Write(compressedMessage); err != nil {
		return fmt.Errorf("cannot write compressed body: %w", err)
	}
	return nil
}

func (l BodyCompressor) Decompress(source io.Reader, dest io.Writer) error {
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
	if decompressedMessage, err := snappy.Decode(nil, compressedMessage.Bytes()); err != nil {
		return fmt.Errorf("cannot decompress body: %w", err)
	} else if _, err := dest.Write(decompressedMessage); err != nil {
		return fmt.Errorf("cannot write decompressed body: %w", err)
	}
	return nil
}
