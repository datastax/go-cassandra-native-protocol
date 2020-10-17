package frame

import (
	"errors"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/message"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
)

func (c *Codec) Decode(source []byte) (*Frame, error) {
	actualLength := len(source)
	if isResponse, version, flags, streamId, opCode, bodyLength, source, err := c.decodeHeader(source); err != nil {
		return nil, fmt.Errorf("cannot decode frame header: %w", err)
	} else {
		if int(bodyLength) != actualLength {
			return nil, errors.New(fmt.Sprintf(
				"declared length in header (%d) does not match actual length (%d)",
				bodyLength,
				actualLength))
		}
		if compressed := flags&cassandraprotocol.HeaderFlagCompressed > 0; compressed {
			if source, err = c.compressor.Decompress(source); err != nil {
				return nil, fmt.Errorf("cannot decompress frame body: %w", err)
			}
		}
		if tracingId, customPayload, warnings, msg, err := c.decodeBody(isResponse, version, flags, opCode, source); err != nil {
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

func (c *Codec) decodeHeader(source []byte) (
	isResponse bool,
	version cassandraprotocol.ProtocolVersion,
	flags cassandraprotocol.HeaderFlag,
	streamId uint16,
	opCode cassandraprotocol.OpCode,
	bodyLength int32,
	remaining []byte,
	err error,
) {
	var versionAndDirection byte
	if versionAndDirection, source, err = primitives.ReadByte(source); err != nil {
		err = fmt.Errorf("cannot decode header version and direction: %w", err)
	} else {
		isResponse = (versionAndDirection & 0b1000_0000) > 0
		version = versionAndDirection & 0b0111_1111
		if flags, source, err = primitives.ReadByte(source); err != nil {
			err = fmt.Errorf("cannot decode header flags: %w", err)
		} else if version == cassandraprotocol.ProtocolVersionBeta && flags&cassandraprotocol.HeaderFlagUseBeta == 0 {
			err = errors.New("expected USE_BETA flag to be set for protocol version " + string(version))
		} else if streamId, source, err = primitives.ReadShort(source); err != nil {
			err = fmt.Errorf("cannot decode header stream id: %w", err)
		} else if opCode, source, err = primitives.ReadByte(source); err != nil {
			err = fmt.Errorf("cannot decode header opcode: %w", err)
		} else if bodyLength, source, err = primitives.ReadInt(source); err != nil {
			err = fmt.Errorf("cannot decode header body length: %w", err)
		}
	}
	return isResponse, version, flags, streamId, opCode, bodyLength, remaining, err
}

func (c *Codec) decodeBody(
	isResponse bool,
	version cassandraprotocol.ProtocolVersion,
	flags cassandraprotocol.HeaderFlag,
	opCode cassandraprotocol.OpCode,
	source []byte,
) (
	tracingId *cassandraprotocol.UUID,
	customPayload map[string][]byte,
	warnings []string,
	message message.Message,
	err error,
) {
	if isResponse && flags&cassandraprotocol.HeaderFlagTracing > 0 {
		if tracingId, source, err = primitives.ReadUuid(source); err != nil {
			err = fmt.Errorf("cannot decode body tracing id: %w", err)
		}
	}
	if err == nil && flags&cassandraprotocol.HeaderFlagCustomPayload > 0 {
		if customPayload, source, err = primitives.ReadBytesMap(source); err != nil {
			err = fmt.Errorf("cannot decode body custom payload: %w", err)
		}
	}
	if err == nil && isResponse && flags&cassandraprotocol.HeaderFlagWarning > 0 {
		if warnings, source, err = primitives.ReadStringList(source); err != nil {
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
