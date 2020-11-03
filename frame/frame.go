package frame

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

// A high-level representation of a frame, where the body is fully decoded.
type Frame struct {
	Header *Header
	Body   *Body
}

// A low-level representation of a frame, where the body is not decoded.
type RawFrame struct {
	Header *Header
	Body   []byte
}

type Header struct {
	IsResponse bool
	Version    primitive.ProtocolVersion
	Flags      primitive.HeaderFlag
	// The stream id. The protocol spec states that the stream id is a [short], but this is wrong: the stream id
	// is signed and can be negative, which is why it has type int16.
	StreamId int16
	OpCode   primitive.OpCode
	// The encoded body length. When encoding a frame, this field is not read but is rather dynamically computed from
	// the actual body length. When decoding a frame, this field is always correctly set to the exact decoded body
	// length.
	BodyLength int32
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
	compress bool,
) (*Frame, error) {
	if message.IsResponse() {
		return nil, fmt.Errorf("message is not a request: opcode %d", message.GetOpCode())
	}
	var flags primitive.HeaderFlag = 0
	if tracing {
		flags |= primitive.HeaderFlagTracing
	}
	if customPayload != nil {
		flags |= primitive.HeaderFlagCustomPayload
	}
	if primitive.IsProtocolVersionBeta(version) {
		flags |= primitive.HeaderFlagUseBeta
	}
	if compress && isCompressible(message.GetOpCode()) {
		flags |= primitive.HeaderFlagCompressed
	}
	return &Frame{
		Header: &Header{
			IsResponse: false,
			Version:    version,
			Flags:      flags,
			StreamId:   streamId,
			OpCode:     message.GetOpCode(),
			BodyLength: 0, // will be set later when encoding
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
	compress bool,
) (*Frame, error) {
	if !message.IsResponse() {
		return nil, fmt.Errorf("message is not a response: opcode %d", message.GetOpCode())
	}
	var flags primitive.HeaderFlag = 0
	if tracingId != nil {
		flags |= primitive.HeaderFlagTracing
	}
	if customPayload != nil {
		flags |= primitive.HeaderFlagCustomPayload
	}
	if warnings != nil {
		flags |= primitive.HeaderFlagWarning
	}
	if primitive.IsProtocolVersionBeta(version) {
		flags |= primitive.HeaderFlagUseBeta
	}
	if compress && isCompressible(message.GetOpCode()) {
		flags |= primitive.HeaderFlagCompressed
	}
	return &Frame{
		Header: &Header{
			IsResponse: true,
			Version:    version,
			Flags:      flags,
			StreamId:   streamId,
			OpCode:     message.GetOpCode(),
			BodyLength: 0, // will be set later when encoding
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

func (f *RawFrame) String() string {
	return fmt.Sprintf("{header: %v, body: %v}", f.Header, f.Body)
}

func (h *Header) String() string {
	return fmt.Sprintf("{response: %v, version: %v, flags: %08b, stream id: %v, opcode: %v, body length: %v}",
		h.IsResponse, h.Version, h.Flags, h.StreamId, h.OpCode, h.BodyLength)
}

func (b *Body) String() string {
	return fmt.Sprintf("{tracing id: %v, payload: %v, warnings: %v, message: %v}",
		b.TracingId, b.CustomPayload, b.Warnings, b.Message)
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

// Dump encodes and dumps the contents of this frame, for debugging purposes.
func (f *RawFrame) Dump() (string, error) {
	buffer := bytes.Buffer{}
	if err := NewRawCodec().EncodeRawFrame(f, &buffer); err != nil {
		return "", err
	} else {
		return hex.Dump(buffer.Bytes()), nil
	}
}

func isCompressible(opCode primitive.OpCode) bool {
	// STARTUP should never be compressed as per protocol specs
	return opCode != primitive.OpCodeStartup &&
		// OPTIONS and READY are empty and as such do not benefit from compression
		opCode != primitive.OpCodeOptions &&
		opCode != primitive.OpCodeReady
}
