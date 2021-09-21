// Copyright 2021 DataStax
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
	"github.com/datastax/go-cassandra-native-protocol/compression/lz4"
	"github.com/datastax/go-cassandra-native-protocol/crc"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_codec_DecodeSegment(t *testing.T) {
	tests := []struct {
		name       string
		compressor PayloadCompressor
		source     []byte
		expected   *Segment
		expectErr  bool
	}{
		{
			"payload 1 byte, self-contained, uncompressed",
			nil,
			[]byte{
				// header
				0b_00000001,      // payload length bits 0-7 = 1
				0b_00000000,      // payload length bits 8-15 = 0
				0b_000000_1_0,    // from right to left: payload length bit 16 + self-contained flag + header padding (6 bits)
				0x72, 0x56, 0xac, // crc24
				// payload
				0b_00000001,            // actual payload
				0x0b, 0x2b, 0x9b, 0xba, // crc32 (already tested separately)
			},
			&Segment{
				Header: &Header{
					IsSelfContained:           true,
					UncompressedPayloadLength: 1,
					CompressedPayloadLength:   0,
					Crc24:                     crc.ChecksumKoopman(0b_00000010_00000000_00000001, 3),
				},
				Payload: &Payload{
					UncompressedData: []byte{1},
					Crc32:            crc.ChecksumIEEE([]byte{1}),
				},
			},
			false,
		},
		{
			"payload 1 byte, multi-part, uncompressed",
			nil,
			[]byte{
				// header
				0b_00000001,      // payload length bits 0-7 = 1
				0b_00000000,      // payload length bits 8-15 = 0
				0b_000000_0_0,    // from right to left: payload length bit 16 + self-contained flag + header padding (6 bits)
				0x6f, 0x87, 0x15, // crc24
				// payload
				0b_00000001,            // actual payload
				0x0b, 0x2b, 0x9b, 0xba, // crc32 (already tested separately)
			},
			&Segment{
				Header: &Header{
					IsSelfContained:           false,
					UncompressedPayloadLength: 1,
					CompressedPayloadLength:   0,
					Crc24:                     crc.ChecksumKoopman(0b_00000000_00000000_00000001, 3),
				},
				Payload: &Payload{
					UncompressedData: []byte{1},
					Crc32:            crc.ChecksumIEEE([]byte{1}),
				},
			},
			false,
		},
		{
			"payload 10 bytes, self-contained, uncompressed",
			nil,
			[]byte{
				// header
				0b_00001010,      // payload length bits 0-7 = 10
				0b_00000000,      // payload length bits 8-15 = 0
				0b_000000_1_0,    // from right to left: payload length bit 16 + self-contained flag + header padding (6 bits)
				0x8c, 0x68, 0x79, // crc24
				// payload
				0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
				0xa8, 0x32, 0xfa, 0xdf, // crc32 (already tested separately)
			},
			&Segment{
				Header: &Header{
					IsSelfContained:           true,
					UncompressedPayloadLength: 10,
					CompressedPayloadLength:   0,
					Crc24:                     crc.ChecksumKoopman(0b_00000010_00000000_00001010, 3),
				},
				Payload: &Payload{
					UncompressedData: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
					Crc32:            crc.ChecksumIEEE([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}),
				},
			},
			false,
		},
		{
			"payload 1 byte, self-contained, compressed",
			lz4.Compressor{},
			[]byte{
				// header
				0b_00000010,     // compressed payload length bits 0-7 = 2
				0b_00000000,     // compressed payload length bits 8-15
				0b_0000001_0,    // from right to left: compressed payload length bit 16 + uncompressed payload length bits 0-6 = 1
				0b_00000000,     // uncompressed payload length bits 7-14
				0b_00000_1_00,   // from right to left: uncompressed payload length bits 15-16 + self-contained flag + header padding (5 bits)
				0x7a, 0x6, 0x9a, // crc24
				// payload
				0x10, 0x1, // compressed payload = {10, 1}
				0xa8, 0xbe, 0xb4, 0x61, // crc32 (already tested separately)
			},
			&Segment{
				Header: &Header{
					IsSelfContained:           true,
					UncompressedPayloadLength: 1,
					CompressedPayloadLength:   2,
					Crc24:                     crc.ChecksumKoopman(0b_00000100_00000000_00000010_00000000_00000010, 5),
				},
				Payload: &Payload{
					UncompressedData: []byte{1},
					Crc32:            crc.ChecksumIEEE([]byte{0x10, 0x1}),
				},
			},
			false,
		},
		{
			"payload 1 byte, multi-part, compressed",
			lz4.Compressor{},
			[]byte{
				// header
				0b_00000010,      // compressed payload length bits 0-7 = 2
				0b_00000000,      // compressed payload length bits 8-15
				0b_0000001_0,     // from right to left: compressed payload length bit 16 + uncompressed payload length bits 0-6 = 1
				0b_00000000,      // uncompressed payload length bits 7-14
				0b_00000_0_00,    // from right to left: uncompressed payload length bits 15-16 + self-contained flag + header padding (5 bits)
				0x4b, 0xeb, 0x7e, // crc24
				// payload
				0x10, 0x1, // compressed payload = {10, 1}
				0xa8, 0xbe, 0xb4, 0x61, // crc32 (already tested separately)
			},
			&Segment{
				Header: &Header{
					IsSelfContained:           false,
					UncompressedPayloadLength: 1,
					CompressedPayloadLength:   2,
					Crc24:                     crc.ChecksumKoopman(0b_00000000_00000000_00000010_00000000_00000010, 5),
				},
				Payload: &Payload{
					UncompressedData: []byte{1},
					Crc32:            crc.ChecksumIEEE([]byte{0x10, 0x1}),
				},
			},
			false,
		},
		{
			"payload 10 bytes, self-contained, compressed",
			lz4.Compressor{},
			[]byte{
				// header
				0b_00001011,      // compressed payload length bits 0-7 = 11
				0b_00000000,      // compressed payload length bits 8-15
				0b_0001010_0,     // from right to left: compressed payload length bit 16 + uncompressed payload length bits 0-6 = 10
				0b_00000000,      // uncompressed payload length bits 7-14
				0b_00000_1_00,    // from right to left: uncompressed payload length bits 15-16 + self-contained flag + header padding (5 bits)
				0x74, 0xcd, 0x7c, // crc24
				// payload
				0xa0, 0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, // compressed payload
				0x0e, 0xed, 0x34, 0x7b, // crc32 (already tested separately)
			},
			&Segment{
				Header: &Header{
					IsSelfContained:           true,
					UncompressedPayloadLength: 10,
					CompressedPayloadLength:   11,
					Crc24:                     crc.ChecksumKoopman(0b_00000100_00000000_00010100_00000000_00001011, 5),
				},
				Payload: &Payload{
					UncompressedData: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
					Crc32:            crc.ChecksumIEEE([]byte{0xa0, 0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9}),
				},
			},
			false,
		},
		{
			"payload 10 bytes, self-contained, compressed header format but uncompressed",
			lz4.Compressor{},
			[]byte{
				// header
				0b_00001011,      // compressed payload length bits 0-7 = 11
				0b_00000000,      // compressed payload length bits 8-15
				0b_0000000_0,     // from right to left: compressed payload length bit 16 + uncompressed payload length bits 0-6 = 0
				0b_00000000,      // uncompressed payload length bits 7-14
				0b_00000_1_00,    // from right to left: uncompressed payload length bits 15-16 + self-contained flag + header padding (5 bits)
				0xb3, 0x3f, 0x91, // crc24
				// payload
				0xa0, 0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, // uncompressed payload
				0x0e, 0xed, 0x34, 0x7b, // crc32 (already tested separately)
			},
			&Segment{
				Header: &Header{
					IsSelfContained:           true,
					UncompressedPayloadLength: 11,
					CompressedPayloadLength:   0,
					Crc24:                     crc.ChecksumKoopman(0b_00000100_00000000_00000000_00000000_00001011, 5),
				},
				Payload: &Payload{
					UncompressedData: []byte{0xa0, 0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9},
					Crc32:            crc.ChecksumIEEE([]byte{0xa0, 0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9}),
				},
			},
			false,
		},
		{
			"wrong header CRC",
			nil,
			[]byte{
				// header
				0b_00000001,   // payload length bits 0-7 = 1
				0b_00000000,   // payload length bits 8-15 = 0
				0b_000000_1_0, // from right to left: payload length bit 16 + self-contained flag + header padding (6 bits)
				0x0, 0x0, 0x0, // crc24
				// payload
				0b_00000001,           // actual payload
				0x1b, 0xdf, 0x5, 0xa5, // crc32 (already tested separately)
			},
			nil,
			true,
		},
		{
			"wrong payload CRC",
			nil,
			[]byte{
				// header
				0b_00000001,      // payload length bits 0-7 = 1
				0b_00000000,      // payload length bits 8-15 = 0
				0b_000000_1_0,    // from right to left: payload length bit 16 + self-contained flag + header padding (6 bits)
				0x72, 0x56, 0xac, // crc24
				// payload
				0b_00000001,        // actual payload
				0x0, 0x0, 0x0, 0x0, // crc32
			},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &codec{compressor: tt.compressor}
			actual, err := c.DecodeSegment(bytes.NewReader(tt.source))
			if tt.expectErr {
				assert.Error(t, err)
				assert.Nil(t, actual)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, actual)
			}
		})
	}
}

func Test_codec_decodeSegmentHeader(t *testing.T) {
	tests := []struct {
		name       string
		compressor PayloadCompressor
		source     []byte
		expected   *Header
		expectErr  bool
	}{
		{
			"payload length 5, self contained, uncompressed",
			nil,
			[]byte{
				0b_00000101,      // payload length bits 0-7 = 5
				0b_00000000,      // payload length bits 8-15 = 0
				0b_000000_1_0,    // from right to left: payload length bit 16 + self-contained flag + header padding (6 bits)
				0x19, 0x99, 0x9a, // crc24
			},
			&Header{
				IsSelfContained:           true,
				UncompressedPayloadLength: 5,
				CompressedPayloadLength:   0,
				Crc24:                     crc.ChecksumKoopman(0b_00000010_00000000_00000101, 3),
			},
			false,
		},
		{
			"payload length 5, multi-part, uncompressed",
			nil,
			[]byte{
				0b_00000101,     // payload length bits 0-7 = 5
				0b_00000000,     // payload length bits 8-15 = 0
				0b_000000_0_0,   // from right to left: payload length bit 16 + self-contained flag + header padding (6 bits)
				0x4, 0x48, 0x23, // crc24
			},
			&Header{
				IsSelfContained:           false,
				UncompressedPayloadLength: 5,
				CompressedPayloadLength:   0,
				Crc24:                     crc.ChecksumKoopman(0b_00000000_00000000_00000101, 3),
			},
			false,
		},
		{
			"payload length max, uncompressed",
			nil,
			[]byte{
				0b_11111111,      // payload length bits 0-7
				0b_11111111,      // payload length bits 8-15
				0b_000000_1_1,    // from right to left: payload length bit 16 + self-contained flag + header padding (6 bits)
				0x25, 0x40, 0x47, // crc24
			},
			&Header{
				IsSelfContained:           true,
				UncompressedPayloadLength: MaxPayloadLength,
				CompressedPayloadLength:   0,
				Crc24:                     crc.ChecksumKoopman(0b_00000011_11111111_11111111, 3),
			},
			false,
		},
		{
			"payload length 5, self contained, compressed",
			lz4.Compressor{},
			[]byte{
				0b_00000101,     // compressed payload length bits 0-7 = 5
				0b_00000000,     // compressed payload length bits 8-15
				0b_0001100_0,    // from right to left: compressed payload length bit 16 + uncompressed payload length bits 0-6 = 12
				0b_00000000,     // uncompressed payload length bits 7-14
				0b_00000_1_00,   // from right to left: uncompressed payload length bits 15-16 + self-contained flag + header padding (5 bits)
				0x9e, 0x8, 0x38, // crc24
			},
			&Header{
				IsSelfContained:           true,
				UncompressedPayloadLength: 12,
				CompressedPayloadLength:   5,
				Crc24:                     crc.ChecksumKoopman(0b_00000100_00000000_00011000_00000000_00000101, 5),
			},
			false,
		},
		{
			"payload length 5, multi-part, compressed",
			lz4.Compressor{},
			[]byte{
				0b_00000101,      // compressed payload length bits 0-7 = 5
				0b_00000000,      // compressed payload length bits 8-15
				0b_0001100_0,     // from right to left: compressed payload length bit 16 + uncompressed payload length bits 0-6 = 12
				0b_00000000,      // uncompressed payload length bits 7-14
				0b_00000_0_00,    // from right to left: uncompressed payload length bits 15-16 + self-contained flag + header padding (5 bits)
				0xaf, 0xe5, 0xdc, // crc24
			},
			&Header{
				IsSelfContained:           false,
				UncompressedPayloadLength: 12,
				CompressedPayloadLength:   5,
				Crc24:                     crc.ChecksumKoopman(0b_00000000_00000000_00011000_00000000_00000101, 5),
			},
			false,
		},
		{
			"payload length max, compressed",
			lz4.Compressor{},
			[]byte{
				0b_01010000,      // compressed payload length bits 0-7
				0b_11000011,      // compressed payload length bits 8-15
				0b_1111111_0,     // from right to left: compressed payload length bit 16 + uncompressed payload length bits 0-6 = 12
				0b_11111111,      // uncompressed payload length bits 7-14
				0b_00000_1_11,    // from right to left: uncompressed payload length bits 15-16 + self-contained flag + header padding (5 bits)
				0xb4, 0x72, 0xaf, // crc24
			},
			&Header{
				IsSelfContained:           true,
				UncompressedPayloadLength: MaxPayloadLength,
				CompressedPayloadLength:   50_000, // 0_11000011_01010000
				Crc24:                     crc.ChecksumKoopman(0b_00000111_11111111_11111110_11000011_01010000, 5),
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &codec{
				compressor: tt.compressor,
			}
			actual, err := c.decodeSegmentHeader(bytes.NewReader(tt.source))
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, actual)
			}
		})
	}
}
