package frame

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/message"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitive"
)

type Frame struct {
	Header *Header
	Body   *Body
}

// Flags return the header flags for this frame. Flags are dynamically computed from the frame's internal state.
// It is a method declared at frame level even if the flags are encoded in the header, because some flags also
// affect how the body is encoded.
func (f *Frame) Flags(compress bool) primitive.HeaderFlag {
	var flags primitive.HeaderFlag = 0
	if compress && f.IsCompressible() {
		flags |= primitive.HeaderFlagCompressed
	}
	if f.Body.TracingId != nil || f.Header.TracingRequested {
		flags |= primitive.HeaderFlagTracing
	}
	if f.Body.CustomPayload != nil {
		flags |= primitive.HeaderFlagCustomPayload
	}
	if f.Body.Warnings != nil {
		flags |= primitive.HeaderFlagWarning
	}
	if primitive.IsProtocolVersionBeta(f.Header.Version) {
		flags |= primitive.HeaderFlagUseBeta
	}
	return flags
}

// IsCompressible returns true if the frame contains a body that can be compressed. Bodies containing STARTUP
// should never be compressed. Empty messages like OPTIONS and READY also should not be compressed,
// even if compression is in use.
func (f *Frame) IsCompressible() bool {
	return isCompressible(f.Body.Message.GetOpCode())
}

// Dump encodes and dumps the contents of this frame, for debugging purposes.
func (f *Frame) Dump() (string, error) {
	buffer := bytes.Buffer{}
	if err := NewCodec().EncodeFrame(f, &buffer); err != nil {
		return "", err
	} else {
		return hex.Dump(buffer.Bytes()), nil
	}
}

type Header struct {
	Version primitive.ProtocolVersion
	// The stream id. The protocol spec states that the stream id is a [short], but this is wrong: the stream id
	// is signed and can be negative, which is why it has type int16.
	StreamId int16
	// For request frames, indicates that tracing should be activated for this request.
	// For response frames, this will be set to true if the frame body contains a tracing id.
	// Note that only QUERY, PREPARE and EXECUTE messages support tracing.
	// Cassandra will simply ignore the tracing flag if set for other message types.
	TracingRequested bool
}

type Body struct {
	// The tracing id. Only valid for response frames, ignored otherwise.
	TracingId *primitive.UUID
	// The custom payload, or nil if no custom payload is defined.
	// Custom payloads are only valid from Protocol Version 4 onwards.
	CustomPayload map[string][]byte
	// Query warnings, if any. Query warnings are only valid for response frames, and only from Protocol Version 4 onwards.
	Warnings []string
	// The body message.
	Message message.Message
}

func NewRequestFrame(
	version primitive.ProtocolVersion,
	streamId int16,
	tracing bool,
	customPayload map[string][]byte,
	message message.Message,
) (*Frame, error) {
	if message.IsResponse() {
		return nil, errors.New("message is not a request: opcode " + string(message.GetOpCode()))
	}
	return &Frame{
		Header: &Header{
			Version:          version,
			StreamId:         streamId,
			TracingRequested: tracing,
		},
		Body: &Body{
			CustomPayload: customPayload,
			Message:       message,
		},
	}, nil
}

func NewResponseFrame(
	version primitive.ProtocolVersion,
	streamId int16,
	tracingId *primitive.UUID,
	customPayload map[string][]byte,
	warnings []string,
	message message.Message,
) (*Frame, error) {
	if !message.IsResponse() {
		return nil, errors.New("message is not a response: opcode " + string(message.GetOpCode()))
	}
	return &Frame{
		Header: &Header{
			Version:          version,
			StreamId:         streamId,
			TracingRequested: tracingId != nil,
		},
		Body: &Body{
			TracingId:     tracingId,
			CustomPayload: customPayload,
			Warnings:      warnings,
			Message:       message,
		},
	}, nil
}

func (f *Frame) String() string {
	return fmt.Sprintf("{header: %v, body: %v}", f.Header, f.Body)
}

func (h *Header) String() string {
	return fmt.Sprintf("{version: %v, stream id: %v, tracing: %v}", h.Version, h.StreamId, h.TracingRequested)
}

func (b *Body) String() string {
	return fmt.Sprintf("{tracing id: %v, payload: %v, warnings: %v, message: %v}", b.TracingId, b.CustomPayload, b.Warnings, b.Message)
}

func isCompressible(opCode primitive.OpCode) bool {
	// STARTUP should never be compressed as per protocol specs
	return opCode != primitive.OpCodeStartup &&
		// OPTIONS and READY are empty and as such do not benefit from compression
		opCode != primitive.OpCodeOptions &&
		opCode != primitive.OpCodeReady
}
