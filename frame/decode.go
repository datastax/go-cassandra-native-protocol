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

package frame

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

func (c *codec) DecodeFrame(source io.Reader) (*Frame, error) {
	if header, err := c.DecodeHeader(source); err != nil {
		return nil, fmt.Errorf("cannot decode frame header: %w", err)
	} else if body, err := c.DecodeBody(header, source); err != nil {
		return nil, fmt.Errorf("cannot decode frame body: %w", err)
	} else {
		return &Frame{Header: header, Body: body}, nil
	}
}

func (c *codec) DecodeRawFrame(source io.Reader) (*RawFrame, error) {
	if header, err := c.DecodeHeader(source); err != nil {
		return nil, fmt.Errorf("cannot decode frame header: %w", err)
	} else if body, err := c.DecodeRawBody(header, source); err != nil {
		return nil, fmt.Errorf("cannot read frame body: %w", err)
	} else {
		return &RawFrame{Header: header, Body: body}, nil
	}
}

func (c *codec) DecodeHeader(source io.Reader) (*Header, error) {
	if versionAndDirection, err := primitive.ReadByte(source); err != nil {
		return nil, fmt.Errorf("cannot decode header version and direction: %w", err)
	} else {
		isResponse := (versionAndDirection & 0b1000_0000) > 0
		version := primitive.ProtocolVersion(versionAndDirection & 0b0111_1111)
		header := &Header{
			IsResponse: isResponse,
			Version:    version,
		}

		var flags uint8
		var err error
		if flags, err = primitive.ReadByte(source); err != nil {
			return nil, fmt.Errorf("cannot decode header flags: %w", err)
		}
		useBetaFlag := primitive.HeaderFlag(flags).Contains(primitive.HeaderFlagUseBeta)

		var opCode uint8
		if err = primitive.CheckSupportedProtocolVersion(version); err != nil {
			return nil, NewProtocolVersionErr(err.Error(), version, useBetaFlag)
		} else if version.IsBeta() && !useBetaFlag {
			return nil, NewProtocolVersionErr("expected USE_BETA flag to be set", version, useBetaFlag)
		} else if header.StreamId, err = primitive.ReadStreamId(source, version); err != nil {
			return nil, fmt.Errorf("cannot decode header stream id: %w", err)
		} else if opCode, err = primitive.ReadByte(source); err != nil {
			return nil, fmt.Errorf("cannot decode header opcode: %w", err)
		} else if header.BodyLength, err = primitive.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot decode header body length: %w", err)
		}
		header.Flags = primitive.HeaderFlag(flags)
		header.OpCode = primitive.OpCode(opCode)
		if err := primitive.CheckValidOpCode(header.OpCode); err != nil {
			return nil, err
		} else if isResponse {
			if err := primitive.CheckResponseOpCode(header.OpCode); err != nil {
				return nil, err
			}
		} else {
			if err := primitive.CheckRequestOpCode(header.OpCode); err != nil {
				return nil, err
			}
		}
		return header, err
	}
}

func (c *codec) DecodeBody(header *Header, source io.Reader) (body *Body, err error) {
	if compressed := header.Flags.Contains(primitive.HeaderFlagCompressed); compressed {
		if c.compressor == nil {
			return nil, errors.New("cannot decompress body: no compressor available")
		} else {
			decompressedBody := &bytes.Buffer{}
			if err := c.compressor.DecompressWithLength(io.LimitReader(source, int64(header.BodyLength)), decompressedBody); err != nil {
				return nil, fmt.Errorf("cannot decompress body: %w", err)
			} else {
				source = decompressedBody
			}
		}
	}
	body = &Body{}
	if header.IsResponse && header.Flags.Contains(primitive.HeaderFlagTracing) {
		if body.TracingId, err = primitive.ReadUuid(source); err != nil {
			return nil, fmt.Errorf("cannot decode body tracing id: %w", err)
		}
	}
	if header.Flags.Contains(primitive.HeaderFlagCustomPayload) {
		if body.CustomPayload, err = primitive.ReadBytesMap(source); err != nil {
			return nil, fmt.Errorf("cannot decode body custom payload: %w", err)
		}
	}
	if header.IsResponse && header.Flags.Contains(primitive.HeaderFlagWarning) {
		if body.Warnings, err = primitive.ReadStringList(source); err != nil {
			return nil, fmt.Errorf("cannot decode body warnings: %w", err)
		}
	}
	if decoder, err := c.findMessageCodec(header.OpCode); err != nil {
		return nil, err
	} else if body.Message, err = decoder.Decode(source, header.Version); err != nil {
		return nil, fmt.Errorf("cannot decode body message: %w", err)
	}
	return body, err
}

func (c *codec) DecodeRawBody(header *Header, source io.Reader) (body []byte, err error) {
	if header.BodyLength < 0 {
		return nil, fmt.Errorf("invalid body length: %d", header.BodyLength)
	} else if header.BodyLength == 0 {
		return []byte{}, nil
	}
	count := int64(header.BodyLength)
	buf := bytes.NewBuffer(make([]byte, 0, count))
	if _, err := io.CopyN(buf, source, count); err != nil {
		return nil, fmt.Errorf("cannot decode raw body: %w", err)
	}
	return buf.Bytes(), nil
}

func (c *codec) DiscardBody(header *Header, source io.Reader) (err error) {
	if header.BodyLength < 0 {
		return fmt.Errorf("invalid body length: %d", header.BodyLength)
	} else if header.BodyLength == 0 {
		return nil
	}
	count := int64(header.BodyLength)
	switch s := source.(type) {
	case io.Seeker:
		_, err = s.Seek(count, io.SeekCurrent)
	default:
		_, err = io.CopyN(ioutil.Discard, s, count)
	}
	if err != nil {
		err = fmt.Errorf("cannot discard body; %w", err)
	}
	return err
}
