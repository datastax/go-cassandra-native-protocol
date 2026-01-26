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
	"fmt"
	"github.com/golang/snappy"
)

// Compressor satisfies compression.Compressor for the SNAPPY algorithm.
type Compressor struct{}

func (c Compressor) CompressFrame(uncompressed []byte) ([]byte, error) {
	compressed := snappy.Encode(nil, uncompressed)
	return compressed, nil
}

func (c Compressor) DecompressFrame(compressed []byte) ([]byte, error) {
	decompressedMessage, err := snappy.Decode(nil, compressed)
	if err != nil {
		return nil, fmt.Errorf("cannot decompress message: %w", err)
	}
	return decompressedMessage, nil
}

func (c Compressor) CompressSegment(uncompressed []byte) ([]byte, error) {
	return nil, fmt.Errorf("snappy compression is not supported for protocol v5+")
}

func (c Compressor) DecompressSegment(compressed []byte, uncompressed []byte) error {
	return fmt.Errorf("snappy decompression is not supported for protocol v5+")
}
