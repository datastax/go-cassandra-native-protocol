package frame

import (
	"errors"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/message"
)

type Frame struct {
	Header *Header
	Body   *Body
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
			Version:  version,
			StreamId: streamId,
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
	return fmt.Sprintf("{header: %v, body: %v}", f.Header, f.Body)
}

func (h *Header) String() string {
	return fmt.Sprintf("{version: %v, streamdid: %v, tracing: %v}", h.Version, h.StreamId, h.TracingRequested)
}

func (b *Body) String() string {
	return fmt.Sprintf("{tracingid: %v, payload: %v, warnings: %v, message: %v}", b.TracingId, b.CustomPayload, b.Warnings, b.Message)
}
