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
	"encoding/binary"
	"fmt"
	"io"

	"github.com/datastax/go-cassandra-native-protocol/crc"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

func (c *codec) DecodeSegment(source io.Reader) (*Segment, error) {
	if header, err := c.decodeSegmentHeader(source); err != nil {
		return nil, fmt.Errorf("cannot decode segment header: %w", err)
	} else if payload, err := c.decodeSegmentPayload(header, source); err != nil {
		return nil, fmt.Errorf("cannot decode segment payload: %w", err)
	} else {
		return &Segment{
			Header:  header,
			Payload: payload,
		}, nil
	}
}

func (c *codec) decodeSegmentHeader(source io.Reader) (*Header, error) {
	// Read header data (little endian)
	var headerData uint64
	headerLength := c.headerLength()
	for i := 0; i < headerLength; i++ {
		if b, err := primitive.ReadByte(source); err != nil {
			return nil, fmt.Errorf("cannot read segment header at byte %v: %w", i, err)
		} else {
			headerData |= uint64(b) << (8 * i)
		}
	}
	// Read CRC (little endian) and check it
	var expectedHeaderCrc uint32
	for i := 0; i < Crc24Length; i++ {
		if b, err := primitive.ReadByte(source); err != nil {
			return nil, fmt.Errorf("cannot read segment header CRC: %w", err)
		} else {
			expectedHeaderCrc |= uint32(b) << (8 * i)
		}
	}
	actualHeaderCrc := crc.ChecksumKoopman(headerData, headerLength)
	if actualHeaderCrc != expectedHeaderCrc {
		return nil, fmt.Errorf(
			"crc mismatch on header %x: received %x, computed %x",
			headerData,
			expectedHeaderCrc,
			actualHeaderCrc)
	}
	header := &Header{Crc24: actualHeaderCrc}
	if c.compressor == nil {
		header.CompressedPayloadLength = 0
		header.UncompressedPayloadLength = int32(headerData & MaxPayloadLength)
	} else {
		header.CompressedPayloadLength = int32(headerData & MaxPayloadLength)
		headerData >>= 17
		header.UncompressedPayloadLength = int32(headerData & MaxPayloadLength)
		if header.UncompressedPayloadLength == 0 {
			// the server chose not to compress
			header.UncompressedPayloadLength = header.CompressedPayloadLength
			header.CompressedPayloadLength = 0
		}
	}
	headerData >>= 17
	header.IsSelfContained = (headerData & 1) == 1
	return header, nil
}

func (c *codec) decodeSegmentPayload(header *Header, source io.Reader) (*Payload, error) {
	// Extract payload
	var length int32
	if c.compressor == nil || header.CompressedPayloadLength == 0 {
		length = header.UncompressedPayloadLength
	} else {
		length = header.CompressedPayloadLength
	}
	encodedPayload := make([]byte, length)
	if _, err := io.ReadFull(source, encodedPayload); err != nil {
		return nil, fmt.Errorf("cannot read encoded payload: %w", err)
	}
	// Read and check CRC
	var expectedPayloadCrc uint32
	if err := binary.Read(source, binary.LittleEndian, &expectedPayloadCrc); err != nil {
		return nil, fmt.Errorf("cannot read segment payload CRC: %w", err)
	}
	actualPayloadCrc := crc.ChecksumIEEE(encodedPayload)
	if actualPayloadCrc != expectedPayloadCrc {
		return nil, fmt.Errorf(
			"crc mismatch on payload: received %x, computed %x",
			expectedPayloadCrc, actualPayloadCrc)
	}
	payload := &Payload{Crc32: actualPayloadCrc}
	// Decompress payload if needed
	if c.compressor == nil || header.CompressedPayloadLength == 0 {
		payload.UncompressedData = encodedPayload
	} else {
		rawData := bytes.NewBuffer(make([]byte, 0, length))
		if err := c.compressor.Decompress(bytes.NewReader(encodedPayload), rawData); err != nil {
			return nil, fmt.Errorf("cannot decompress segment payload: %w", err)
		}
		payload.UncompressedData = rawData.Bytes()
	}
	return payload, nil
}

func (c *codec) headerLength() int {
	if c.compressor == nil {
		return UncompressedHeaderLength
	}
	return CompressedHeaderLength
}
