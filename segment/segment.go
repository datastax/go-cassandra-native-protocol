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
	"bytes"
	"encoding/hex"
	"fmt"
)

// Segment is a checksummed payload containing one or more frames (self-contained segment), or just a portion of a
// larger frame spanning multiple segments (multi-segment part). It can also be optionally compressed with Lz4.
// Segments were introduced in protocol v5 to improve compression efficiency (especially for small messages) and to
// introduce error detection in the native protocol.
// +k8s:deepcopy-gen=true
type Segment struct {
	Header  *Header
	Payload *Payload
}

// Header is the header of a Segment.
// +k8s:deepcopy-gen=true
type Header struct {
	// IsSelfContained is true when this segment's payload contains one or more Frames (self-contained segment); when it
	// is false, this segment's payload contains just a portion of a larger frame spanning multiple segments
	// (multi-segment part).
	IsSelfContained bool
	// UncompressedPayloadLength is the length of the uncompressed payload data. The value of this field should always
	// be equal to the length of Payload.UncompressedData. This is a computed value that users should not set
	// themselves. When encoding a segment, this field is not read but is rather dynamically computed from the actual
	// payload length. When decoding a segment, this field is always correctly set to the exact decoded payload length.
	UncompressedPayloadLength int32
	// CompressedPayloadLength is the length of the compressed payload data, if compression is being used; it is 0
	// otherwise. This is a computed value that users should not set themselves. When encoding a segment, this field is
	// not read but is rather dynamically computed from the actual compressed payload length. When decoding a segment,
	// this field is always correctly set to the exact compressed payload length.
	CompressedPayloadLength int32
	// Crc24 is the CRC-24 checksum of the encoded header. This is a computed value that users should not set
	// themselves. When encoding a segment, this field is not read but is rather dynamically computed from the actual
	// encoded header. When decoding a segment, the CRC-24 present in the header trailer is verified against that of
	// the actual decoded header, then set to that value if it matches.
	Crc24 uint32
}

// Payload is the payload of a Segment.
// +k8s:deepcopy-gen=true
type Payload struct {
	// UncompressedData holds the uncompressed data forming one or more frames.
	UncompressedData []byte
	// Crc32 is the CRC-32 checksum of the payload. This is a computed value that users should not set
	// themselves. When encoding a segment, this field is not read but is rather dynamically computed from the actual
	// payload. When decoding a segment, the CRC-32 present in the payload trailer is verified against the actual
	// decoded payload, then set to that value if it matches.
	Crc32 uint32
}

func (s *Segment) String() string {
	return fmt.Sprintf("{header: %v, payload: %v}", s.Header, s.Payload)
}

func (h *Header) String() string {
	return fmt.Sprintf(
		"{self-contained: %v, uncompressed length: %v, compressed length: %v, crc24: %v}",
		h.IsSelfContained,
		h.UncompressedPayloadLength,
		h.CompressedPayloadLength,
		h.Crc24,
	)
}

func (p *Payload) String() string {
	return fmt.Sprintf("{length: %v, crc32: %v}", len(p.UncompressedData), p.Crc32)
}

// Dump encodes and dumps the contents of this segment, for debugging purposes.
func (s *Segment) Dump() (string, error) {
	buffer := bytes.Buffer{}
	if err := NewCodec().EncodeSegment(s, &buffer); err != nil {
		return "", err
	} else {
		return hex.Dump(buffer.Bytes()), nil
	}
}
