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
	"encoding/binary"
	"fmt"
	"io"

	"github.com/datastax/go-cassandra-native-protocol/crc"
)

// MaxPayloadLength is the maximum payload length a Segment can contain. Since the payload length header field contains
// 17 bits, the maximum payload length is 2ˆ17-1 = 131,071.
const MaxPayloadLength = 131_071

func (c *codec) EncodeSegment(segment *Segment, dest io.Writer) error {
	payloadLength := len(segment.Payload.UncompressedData)
	segment.Header.UncompressedPayloadLength = int32(payloadLength)
	if payloadLength > MaxPayloadLength {
		return fmt.Errorf("paload length exceeds maximum allowed: %v > %v", payloadLength, MaxPayloadLength)
	} else {
		if c.compressor == nil {
			return c.encodeSegmentUncompressed(segment, dest)
		} else {
			return c.encodeSegmentCompressed(segment, dest)
		}
	}
}

func (c *codec) encodeSegmentUncompressed(segment *Segment, dest io.Writer) error {
	segment.Header.CompressedPayloadLength = 0
	segment.Payload.Crc32 = crc.ChecksumIEEE(segment.Payload.UncompressedData)
	if err := c.encodeHeaderUncompressed(segment.Header, dest); err != nil {
		return fmt.Errorf("cannot encode segment header: %w", err)
	} else if _, err := dest.Write(segment.Payload.UncompressedData); err != nil {
		return fmt.Errorf("cannot write encoded segment payload: %w", err)
	} else if err := c.writePayloadCrc(segment.Payload.Crc32, dest); err != nil {
		return fmt.Errorf("cannot write encoded segment payload CRC: %w", err)
	}
	return nil
}

func (c *codec) encodeSegmentCompressed(segment *Segment, dest io.Writer) error {
	uncompressedPayload := bytes.NewBuffer(segment.Payload.UncompressedData)
	compressedPayload := bytes.NewBuffer(make([]byte, 0, len(segment.Payload.UncompressedData)))
	if err := c.compressor.Compress(uncompressedPayload, compressedPayload); err != nil {
		return fmt.Errorf("cannot compress segment payload: %w", err)
	} else {
		var payload *bytes.Buffer
		segment.Header.CompressedPayloadLength = int32(compressedPayload.Len())
		if segment.Header.CompressedPayloadLength <= segment.Header.UncompressedPayloadLength {
			payload = compressedPayload
		} else {
			// compression is not worth it
			payload = uncompressedPayload
			segment.Header.CompressedPayloadLength = segment.Header.UncompressedPayloadLength
			segment.Header.UncompressedPayloadLength = 0
		}
		segment.Payload.Crc32 = crc.ChecksumIEEE(payload.Bytes())
		if err := c.encodeHeaderCompressed(segment.Header, dest); err != nil {
			return fmt.Errorf("cannot encode segment header: %w", err)
		} else if _, err := payload.WriteTo(dest); err != nil {
			return fmt.Errorf("cannot write encoded segment payload: %w", err)
		} else if err := c.writePayloadCrc(segment.Payload.Crc32, dest); err != nil {
			return fmt.Errorf("cannot write encoded segment payload CRC: %w", err)
		}
		return nil
	}
}

func (c *codec) encodeHeaderUncompressed(header *Header, dest io.Writer) error {
	const headerLength = UncompressedHeaderLength
	const flagOffset = 17
	headerData := uint64(header.UncompressedPayloadLength)
	if header.IsSelfContained {
		headerData |= 1 << flagOffset
	}
	return c.writeHeaderDataAndCrc(headerData, headerLength, dest)
}

func (c *codec) encodeHeaderCompressed(header *Header, dest io.Writer) error {
	const headerLength = CompressedHeaderLength
	const flagOffset = 34
	headerData := uint64(header.CompressedPayloadLength)
	headerData |= uint64(header.UncompressedPayloadLength) << 17
	if header.IsSelfContained {
		headerData |= 1 << flagOffset
	}
	return c.writeHeaderDataAndCrc(headerData, headerLength, dest)
}

func (c *codec) writeHeaderDataAndCrc(headerData uint64, headerLength int, dest io.Writer) error {
	headerCrc := crc.ChecksumKoopman(headerData, headerLength)
	for i := 0; i < headerLength; i++ {
		if err := binary.Write(dest, binary.LittleEndian, (byte)(headerData)); err != nil {
			return fmt.Errorf("cannot write encoded segment header data: %w", err)
		}
		headerData >>= 8
	}
	for i := 0; i < Crc24Length; i++ {
		if err := binary.Write(dest, binary.LittleEndian, (byte)(headerCrc)); err != nil {
			return fmt.Errorf("cannot write encoded segment header CRC: %w", err)
		}
		headerCrc >>= 8
	}
	return nil
}

func (c *codec) writePayloadCrc(payloadCrc uint32, dest io.Writer) error {
	if err := binary.Write(dest, binary.LittleEndian, payloadCrc); err != nil {
		return fmt.Errorf("cannot write encoded segment payload CRC: %w", err)
	}
	return nil
}
