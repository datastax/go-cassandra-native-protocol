package frame

import (
	"encoding/hex"
	"errors"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/message"
)

type Frame struct {
	Header *Header
	Body   *Body
}

// Flags return the header flags for this frame. Flags are dynamically computed from the frame's internal state.
// It is a method declared at frame level even if the flags are encoded in the header, because some flags also
// affect how the body is encoded.
func (f *Frame) Flags(compress bool) cassandraprotocol.HeaderFlag {
	var flags cassandraprotocol.HeaderFlag = 0
	if compress && f.IsCompressible() {
		flags |= cassandraprotocol.HeaderFlagCompressed
	}
	if f.Body.TracingId != nil || f.Header.TracingRequested {
		flags |= cassandraprotocol.HeaderFlagTracing
	}
	if f.Body.CustomPayload != nil {
		flags |= cassandraprotocol.HeaderFlagCustomPayload
	}
	if f.Body.Warnings != nil {
		flags |= cassandraprotocol.HeaderFlagWarning
	}
	if f.Header.Version == cassandraprotocol.ProtocolVersionBeta {
		flags |= cassandraprotocol.HeaderFlagUseBeta
	}
	return flags
}

// IsCompressible returns true if the frame contains a body that can be compressed. Bodies containing STARTUP and
// OPTIONS messages should indeed never be compressed, even if compression is in use.
func (f *Frame) IsCompressible() bool {
	opCode := f.Body.Message.GetOpCode()
	return opCode != cassandraprotocol.OpCodeStartup && opCode != cassandraprotocol.OpCodeOptions
}

// Dump encodes and dumps the contents of this frame, for debugging purposes.
func (f *Frame) Dump() (string, error) {
	if encoded, err := NewCodec().Encode(f); err != nil {
		return "", err
	} else {
		return hex.Dump(encoded.Bytes()), nil
	}
}

type Header struct {
	Version cassandraprotocol.ProtocolVersion
	// The protocol spec states that the stream id is a [short], but this is wrong: the stream id
	// is signed and can be negative, which is why it has type int16.
	StreamId int16
	// Whether tracing should be activated for this request. Only valid for request frames, ignored for response frames.
	// Note that only QUERY, PREPARE and EXECUTE queries support tracing. Other requests will simply ignore the tracing flag if set.
	TracingRequested bool
}

type Body struct {
	// The tracing ID. Only valid for response frames, ignored otherwise.
	TracingId *cassandraprotocol.UUID
	// Custom payloads are only valid from Protocol Version 4 onwards.
	CustomPayload map[string][]byte
	// Query warnings, if any. Query warnings are only valid for response frames, and only from Protocol Version 4 onwards.
	Warnings []string
	// The body message
	Message message.Message
}

func NewRequestFrame(
	version cassandraprotocol.ProtocolVersion,
	streamId int16,
	tracing bool,
	customPayload map[string][]byte,
	message message.Message,
) (*Frame, error) {
	if message.IsResponse() {
		return nil, errors.New("message is not a request: opcode " + string(message.GetOpCode()))
	}
	return newFrame(
		&Header{
			Version:          version,
			StreamId:         streamId,
			TracingRequested: tracing,
		},
		&Body{
			CustomPayload: customPayload,
			Message:       message,
		})
}

func NewResponseFrame(
	version cassandraprotocol.ProtocolVersion,
	streamId int16,
	tracingId *cassandraprotocol.UUID,
	customPayload map[string][]byte,
	warnings []string,
	message message.Message,
) (*Frame, error) {
	if !message.IsResponse() {
		return nil, errors.New("message is not a response: opcode " + string(message.GetOpCode()))
	}
	return newFrame(
		&Header{
			Version:          version,
			StreamId:         streamId,
			TracingRequested: tracingId != nil,
		},
		&Body{
			TracingId:     tracingId,
			CustomPayload: customPayload,
			Warnings:      warnings,
			Message:       message,
		})
}

func newFrame(header *Header, body *Body) (*Frame, error) {
	// Check header and body global conformity with protocol specs
	if header.Version < cassandraprotocol.ProtocolVersionMin || header.Version > cassandraprotocol.ProtocolVersionMax {
		return nil, fmt.Errorf("unsupported protocol version: %v", header.Version)
	}
	if body.CustomPayload != nil && header.Version < cassandraprotocol.ProtocolVersion4 {
		return nil, errors.New("custom payloads require protocol version 4 or higher")
	}
	if body.Warnings != nil && header.Version < cassandraprotocol.ProtocolVersion4 {
		return nil, errors.New("warnings require protocol version 4 or higher")
	}
	// Body message conformity with protocol specs will be tested by the message codecs
	return &Frame{header, body}, nil
}

func (f *Frame) String() string {
	return fmt.Sprintf(
		"{header: %v, flags: %08b, body: %v}",
		f.Header,
		f.Flags(false),
		f.Body,
	)
}

func (h *Header) String() string {
	return fmt.Sprintf("{version: %v, streamdid: %v, tracing: %v}", h.Version, h.StreamId, h.TracingRequested)
}

func (b *Body) String() string {
	return fmt.Sprintf("{tracingid: %v, payload: %v, warnings: %v, message: %v}", b.TracingId, b.CustomPayload, b.Warnings, b.Message)
}
