package frame

import (
	"errors"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/message"
)

type Frame struct {
	Version cassandraprotocol.ProtocolVersion

	// The protocol spec states that the stream id is a [short], but this is wrong: the stream id
	// is signed and can be negative, which is why it has type int16.
	StreamId int16

	// Whether tracing should be activated for this request. Only valid for request frames, ignored for response frames.
	// Note that only QUERY, PREPARE and EXECUTE queries support tracing. Other requests will simply ignore the tracing flag if set.
	TracingRequested bool

	// The tracing ID. Only valid for response frames, ignored otherwise.
	TracingId *cassandraprotocol.UUID

	CustomPayload map[string][]byte

	Warnings []string

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
		panic("NewRequestFrame cannot be used with response messages")
	}
	return NewFrame(
		version,
		streamId,
		tracing,
		nil,
		customPayload,
		nil,
		message)
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
		panic("NewResponseFrame cannot be used with request messages")
	}
	return NewFrame(
		version,
		streamId,
		tracingId != nil,
		tracingId,
		customPayload,
		warnings,
		message)
}

// NewFrame is mainly intended for internal use. If you want to build
// frames to pass for encoding, see NewRequestFrame or NewResponseFrame.
func NewFrame(
	version cassandraprotocol.ProtocolVersion,
	streamId int16,
	tracingRequested bool,
	tracingId *cassandraprotocol.UUID,
	customPayload map[string][]byte,
	warnings []string,
	message message.Message,
) (*Frame, error) {
	if customPayload != nil && version < cassandraprotocol.ProtocolVersion4 {
		return nil, errors.New("custom payloads require protocol version 4 or higher")
	}
	if warnings != nil && version < cassandraprotocol.ProtocolVersion4 {
		return nil, errors.New("warnings require protocol version 4 or higher")
	}
	return &Frame{
		version,
		streamId,
		tracingRequested,
		tracingId,
		customPayload,
		warnings,
		message,
	}, nil
}
