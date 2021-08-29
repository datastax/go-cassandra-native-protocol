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

package segment

import (
	"io"
)

const (
	UncompressedHeaderLength = 3
	CompressedHeaderLength   = 5
)

const (
	Crc24Length = 3
	Crc32Length = 4
)

type Encoder interface {

	// EncodeSegment encodes the entire segment.
	EncodeSegment(segment *Segment, dest io.Writer) error
}

type Decoder interface {

	// DecodeSegment decodes the entire segment.
	DecodeSegment(source io.Reader) (*Segment, error)
}

// Codec exposes basic encoding and decoding operations for Segment instances. It should be the preferred interface to
// use in typical client applications such as drivers.
type Codec interface {
	Encoder
	Decoder
}

type codec struct {
	compressor PayloadCompressor
}

func NewCodec() Codec {
	return NewCodecWithCompression(nil)
}

func NewCodecWithCompression(compressor PayloadCompressor) Codec {
	return &codec{compressor: compressor}
}
