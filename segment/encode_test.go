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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/datastax/go-cassandra-native-protocol/compression/lz4"
)

func Test_codec_EncodeSegment(t *testing.T) {
	tests := []struct {
		name       string
		compressor PayloadCompressor
		segment    *Segment
		expected   []byte
		expectErr  bool
	}{
		{
			"payload 1 byte, self-contained, uncompressed",
			nil,
			&Segment{
				Header:  &Header{IsSelfContained: true},
				Payload: &Payload{UncompressedData: []byte{1}},
			},
			[]byte{
				// header
				0b_00000001,      // payload length bits 0-7 = 1
				0b_00000000,      // payload length bits 8-15 = 0
				0b_000000_1_0,    // from right to left: payload length bit 16 + self-contained flag + header padding (6 bits)
				0x72, 0x56, 0xac, // crc24 (already tested separately)
				// payload
				0b_00000001,           // actual payload
				0xb, 0x2b, 0x9b, 0xba, // crc32 (already tested separately)
			},
			false,
		},
		{
			"payload 1 byte, multi-part, uncompressed",
			nil,
			&Segment{
				Header:  &Header{IsSelfContained: false},
				Payload: &Payload{UncompressedData: []byte{1}},
			},
			[]byte{
				// header
				0b_00000001,      // payload length bits 0-7 = 1
				0b_00000000,      // payload length bits 8-15 = 0
				0b_000000_0_0,    // from right to left: payload length bit 16 + self-contained flag + header padding (6 bits)
				0x6f, 0x87, 0x15, // crc24 (already tested separately)
				// payload
				0b_00000001,           // actual payload
				0xb, 0x2b, 0x9b, 0xba, // crc32 (already tested separately)
			},
			false,
		},
		{
			"payload 10 bytes, self-contained, uncompressed",
			nil,
			&Segment{
				Header:  &Header{IsSelfContained: true},
				Payload: &Payload{UncompressedData: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
			},
			[]byte{
				// header
				0b_00001010,      // payload length bits 0-7 = 10
				0b_00000000,      // payload length bits 8-15 = 0
				0b_000000_1_0,    // from right to left: payload length bit 16 + self-contained flag + header padding (6 bits)
				0x8c, 0x68, 0x79, // crc24 (already tested separately)
				// payload
				0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
				0xa8, 0x32, 0xfa, 0xdf, // crc32 (already tested separately)
			},
			false,
		},
		{
			"payload 1 byte, self-contained, compressed header format but uncompressed",
			lz4.Compressor{},
			&Segment{
				Header:  &Header{IsSelfContained: true},
				Payload: &Payload{UncompressedData: []byte{1}},
			},
			[]byte{
				// header
				0b_00000001,     // compressed payload length bits 0-7 = 1
				0b_00000000,     // compressed payload length bits 8-15
				0b_0000000_0,    // from right to left: compressed payload length bit 16 + uncompressed payload length bits 0-6 = 0
				0b_00000000,     // uncompressed payload length bits 7-14
				0b_00000_1_00,   // from right to left: uncompressed payload length bits 15-16 + self-contained flag + header padding (5 bits)
				0x6, 0xa5, 0x87, // crc24 (already tested separately)
				// payload
				0x1,                   // uncompressed payload
				0xb, 0x2b, 0x9b, 0xba, // crc32 (already tested separately)
			},
			false,
		},
		{
			"payload 1 byte, multi-part, compressed header format but uncompressed",
			lz4.Compressor{},
			&Segment{
				Header:  &Header{IsSelfContained: false},
				Payload: &Payload{UncompressedData: []byte{1}},
			},
			[]byte{
				// header
				0b_00000001,      // compressed payload length bits 0-7 = 1
				0b_00000000,      // compressed payload length bits 8-15
				0b_0000000_0,     // from right to left: compressed payload length bit 16 + uncompressed payload length bits 0-6 = 0
				0b_00000000,      // uncompressed payload length bits 7-14
				0b_00000_0_00,    // from right to left: uncompressed payload length bits 15-16 + self-contained flag + header padding (5 bits)
				0x37, 0x48, 0x63, // crc24 (already tested separately)
				// payload
				0x1,                   // uncompressed payload
				0xb, 0x2b, 0x9b, 0xba, // crc32 (already tested separately)
			},
			false,
		},
		{
			"payload 10 bytes, self-contained, compressed header format but uncompressed",
			lz4.Compressor{},
			&Segment{
				Header:  &Header{IsSelfContained: true},
				Payload: &Payload{UncompressedData: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
			},
			[]byte{
				// header
				0b_00001010,      // compressed payload length bits 0-7 = 10
				0b_00000000,      // compressed payload length bits 8-15
				0b_0000000_0,     // from right to left: compressed payload length bit 16 + uncompressed payload length bits 0-6 = 0
				0b_00000000,      // uncompressed payload length bits 7-14
				0b_00000_1_00,    // from right to left: uncompressed payload length bits 15-16 + self-contained flag + header padding (5 bits)
				0x8e, 0xdb, 0x58, // crc24 (already tested separately)
				// payload
				0, 1, 2, 3, 4, 5, 6, 7, 8, 9, // uncompressed payload
				0xa8, 0x32, 0xfa, 0xdf, // crc32 (already tested separately)
			},
			false,
		},
		{
			"payload 100 bytes, self-contained, compressed",
			lz4.Compressor{},
			&Segment{
				Header:  &Header{IsSelfContained: true},
				Payload: &Payload{UncompressedData: make([]byte, 100)},
			},
			[]byte{
				// header
				0b_00011010,      // compressed payload length bits 0-7 = 26
				0b_00000000,      // compressed payload length bits 8-15
				0b_1100100_0,     // from right to left: compressed payload length bit 16 + uncompressed payload length bits 0-6 = 100
				0b_00000000,      // uncompressed payload length bits 7-14
				0b_00000_1_00,    // from right to left: uncompressed payload length bits 15-16 + self-contained flag + header padding (5 bits)
				0x3a, 0xc7, 0xd7, // crc24 (already tested separately)
				// payload
				0x1f, 0x0, 0x1, 0x0, 0x3a, 0x0, 0x2, 0x0, 0x0, 0x2, 0x0, 0xe0, 0x0,
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // compressed payload
				0x3e, 0x52, 0x70, 0xe4, // crc32 (already tested separately)
			},
			false,
		},
		{
			"payload too large",
			lz4.Compressor{},
			&Segment{
				Header:  &Header{IsSelfContained: true},
				Payload: &Payload{UncompressedData: make([]byte, MaxPayloadLength+1)},
			},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &codec{compressor: tt.compressor}
			actual := &bytes.Buffer{}
			err := c.EncodeSegment(tt.segment, actual)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, actual.Bytes())
			}
		})
	}
}

func Test_codec_encodeHeaderUncompressed(t *testing.T) {
	tests := []struct {
		name      string
		header    *Header
		expected  []byte
		expectErr bool
	}{
		{
			"payload length 5, self contained",
			&Header{
				IsSelfContained:           true,
				UncompressedPayloadLength: 5,
			},
			[]byte{
				0b_00000101,      // payload length bits 0-7 = 5
				0b_00000000,      // payload length bits 8-15 = 0
				0b_000000_1_0,    // from right to left: payload length bit 16 + self-contained flag + header padding (6 bits)
				0x19, 0x99, 0x9a, // crc24 (already tested separately)
			},
			false,
		},
		{
			"payload length 5, multi-part",
			&Header{
				IsSelfContained:           false,
				UncompressedPayloadLength: 5,
			},
			[]byte{
				0b_00000101,     // payload length bits 0-7 = 5
				0b_00000000,     // payload length bits 8-15 = 0
				0b_000000_0_0,   // from right to left: payload length bit 16 + self-contained flag + header padding (6 bits)
				0x4, 0x48, 0x23, // crc24 (already tested separately)
			},
			false,
		},
		{
			"payload length max",
			&Header{
				IsSelfContained:           true,
				UncompressedPayloadLength: MaxPayloadLength,
			},
			[]byte{
				0b_11111111,      // payload length bits 0-7
				0b_11111111,      // payload length bits 8-15
				0b_000000_1_1,    // from right to left: payload length bit 16 + self-contained flag + header padding (6 bits)
				0x25, 0x40, 0x47, // crc24 (already tested separately)
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &codec{}
			actual := &bytes.Buffer{}
			err := c.encodeHeaderUncompressed(tt.header, actual)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, actual.Bytes())
			}
		})
	}
}

func Test_codec_encodeHeaderCompressed(t *testing.T) {
	tests := []struct {
		name      string
		header    *Header
		expected  []byte
		expectErr bool
	}{
		{
			"payload length 5, self contained",
			&Header{
				IsSelfContained:           true,
				UncompressedPayloadLength: 12,
				CompressedPayloadLength:   5,
			},
			[]byte{
				0b_00000101,     // compressed payload length bits 0-7 = 5
				0b_00000000,     // compressed payload length bits 8-15
				0b_0001100_0,    // from right to left: compressed payload length bit 16 + uncompressed payload length bits 0-6 = 12
				0b_00000000,     // uncompressed payload length bits 7-14
				0b_00000_1_00,   // from right to left: uncompressed payload length bits 15-16 + self-contained flag + header padding (5 bits)
				0x9e, 0x8, 0x38, // crc24 (already tested separately)
			},
			false,
		},
		{
			"payload length 5, multi-part",
			&Header{
				IsSelfContained:           false,
				UncompressedPayloadLength: 12,
				CompressedPayloadLength:   5,
			},
			[]byte{
				0b_00000101,      // compressed payload length bits 0-7 = 5
				0b_00000000,      // compressed payload length bits 8-15
				0b_0001100_0,     // from right to left: compressed payload length bit 16 + uncompressed payload length bits 0-6 = 12
				0b_00000000,      // uncompressed payload length bits 7-14
				0b_00000_0_00,    // from right to left: uncompressed payload length bits 15-16 + self-contained flag + header padding (5 bits)
				0xaf, 0xe5, 0xdc, // crc24 (already tested separately)
			},
			false,
		},
		{
			"payload length max",
			&Header{
				IsSelfContained:           true,
				UncompressedPayloadLength: MaxPayloadLength,
				CompressedPayloadLength:   50_000, // 0_11000011_01010000
			},
			[]byte{
				0b_01010000,      // compressed payload length bits 0-7
				0b_11000011,      // compressed payload length bits 8-15
				0b_1111111_0,     // from right to left: compressed payload length bit 16 + uncompressed payload length bits 0-6 = 12
				0b_11111111,      // uncompressed payload length bits 7-14
				0b_00000_1_11,    // from right to left: uncompressed payload length bits 15-16 + self-contained flag + header padding (5 bits)
				0xb4, 0x72, 0xaf, // crc24 (already tested separately)
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &codec{}
			actual := &bytes.Buffer{}
			err := c.encodeHeaderCompressed(tt.header, actual)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, actual.Bytes())
			}
		})
	}
}
