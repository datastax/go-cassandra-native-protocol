package frame

import (
	"bytes"
	"errors"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/message"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
	"io"
)

func (c *Codec) Decode(frame []byte) (*Frame, error) {
	actualLength := len(frame)
	frameBuffer := bytes.NewBuffer(frame)
	if isResponse, version, flags, streamId, opCode, bodyLength, err := c.decodeHeader(frameBuffer); err != nil {
		return nil, fmt.Errorf("cannot decode frame header: %w", err)
	} else {
		if int(bodyLength) != actualLength {
			return nil, errors.New(fmt.Sprintf(
				"declared length in header (%d) does not match actual length (%d)",
				bodyLength,
				actualLength))
		}
		if compressed := flags&cassandraprotocol.HeaderFlagCompressed > 0; compressed {
			if frame, err := c.compressor.Decompress(frameBuffer); err != nil {
				return nil, fmt.Errorf("cannot decompress frame body: %w", err)
			} else {
				frameBuffer = bytes.NewBuffer(frame)
			}
		}
		if tracingId, customPayload, warnings, msg, err := c.decodeBody(isResponse, version, flags, opCode, frameBuffer); err != nil {
			return nil, fmt.Errorf("cannot decompress frame body: %w", err)
		} else {
			header := &Header{
				Version:          version,
				StreamId:         int16(streamId & 0xFFFF),
				TracingRequested: tracingId != nil,
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

func (c *Codec) decodeHeader(source io.Reader) (
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

func (c *Codec) decodeBody(
	isResponse bool,
	version cassandraprotocol.ProtocolVersion,
	flags cassandraprotocol.HeaderFlag,
	opCode cassandraprotocol.OpCode,
	source io.Reader,
) (
	tracingId *cassandraprotocol.UUID,
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
