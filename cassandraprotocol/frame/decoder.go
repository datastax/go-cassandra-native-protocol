package frame

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/message"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitives"
	"io"
)

func (c *codec) Decode(frame *bytes.Buffer) (*Frame, error) {
	frameLength := frame.Len()
	if isResponse, version, flags, streamId, opCode, declaredBodyLength, err := c.decodeHeader(frame); err != nil {
		return nil, fmt.Errorf("cannot decode frame header: %w", err)
	} else {
		actualBodyLength := frameLength - encodedHeaderLength
		if int(declaredBodyLength) != actualBodyLength {
			return nil, errors.New(fmt.Sprintf(
				"declared body length in header (%d) does not match actual body length (%d)",
				declaredBodyLength,
				actualBodyLength))
		}
		if compressed := flags&cassandraprotocol.HeaderFlagCompressed > 0; compressed {
			if frame, err = c.compressor.Decompress(frame); err != nil {
				return nil, fmt.Errorf("cannot decompress frame body: %w", err)
			}
		}
		if tracingId, customPayload, warnings, msg, err := c.decodeBody(isResponse, version, flags, opCode, frame); err != nil {
			return nil, fmt.Errorf("cannot decode frame body: %w", err)
		} else {
			header := &Header{
				Version:          version,
				StreamId:         int16(streamId & 0xFFFF),
				TracingRequested: tracingId != nil || flags&cassandraprotocol.HeaderFlagTracing != 0,
			}
			body := Body{
				TracingId:     tracingId,
				CustomPayload: customPayload,
				Warnings:      warnings,
				Message:       msg,
			}
			return &Frame{Header: header, Body: &body}, nil
		}
	}
}

func (c *codec) decodeHeader(source io.Reader) (
	isResponse bool,
	version cassandraprotocol.ProtocolVersion,
	flags cassandraprotocol.HeaderFlag,
	streamId uint16,
	opCode cassandraprotocol.OpCode,
	bodyLength int32,
	err error,
) {
	if versionAndDirection, err := primitives.ReadByte(source); err != nil {
		err = fmt.Errorf("cannot decode header version and direction: %w", err)
	} else {
		isResponse = (versionAndDirection & 0b1000_0000) > 0
		version = versionAndDirection & 0b0111_1111
		if flags, err = primitives.ReadByte(source); err != nil {
			err = fmt.Errorf("cannot decode header flags: %w", err)
		} else if version == cassandraprotocol.ProtocolVersionBeta && flags&cassandraprotocol.HeaderFlagUseBeta == 0 {
			err = fmt.Errorf("expected USE_BETA flag to be set for protocol version %v", version)
		} else if streamId, err = primitives.ReadShort(source); err != nil {
			err = fmt.Errorf("cannot decode header stream id: %w", err)
		} else if opCode, err = primitives.ReadByte(source); err != nil {
			err = fmt.Errorf("cannot decode header opcode: %w", err)
		} else if bodyLength, err = primitives.ReadInt(source); err != nil {
			err = fmt.Errorf("cannot decode header body length: %w", err)
		}
	}
	return isResponse, version, flags, streamId, opCode, bodyLength, err
}

func (c *codec) decodeBody(
	isResponse bool,
	version cassandraprotocol.ProtocolVersion,
	flags cassandraprotocol.HeaderFlag,
	opCode cassandraprotocol.OpCode,
	source io.Reader,
) (
	tracingId *primitives.UUID,
	customPayload map[string][]byte,
	warnings []string,
	message message.Message,
	err error,
) {
	if isResponse && flags&cassandraprotocol.HeaderFlagTracing > 0 {
		if tracingId, err = primitives.ReadUuid(source); err != nil {
			err = fmt.Errorf("cannot decode body tracing id: %w", err)
		}
	}
	if err == nil && flags&cassandraprotocol.HeaderFlagCustomPayload > 0 {
		if customPayload, err = primitives.ReadBytesMap(source); err != nil {
			err = fmt.Errorf("cannot decode body custom payload: %w", err)
		}
	}
	if err == nil && isResponse && flags&cassandraprotocol.HeaderFlagWarning > 0 {
		if warnings, err = primitives.ReadStringList(source); err != nil {
			err = fmt.Errorf("cannot decode body warnings: %w", err)
		}
	}
	if err == nil {
		if decoder, found := c.codecs[opCode]; !found {
			err = errors.New(fmt.Sprintf("unsupported opcode %d", opCode))
		} else if message, err = decoder.Decode(source, version); err != nil {
			err = fmt.Errorf("cannot decode body message: %w", err)
		}
	}
	return tracingId, customPayload, warnings, message, err
}
