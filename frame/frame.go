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
	"encoding/hex"
	"fmt"

	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

// Frame is a high-level representation of a frame, where the body is fully decoded.
// Note that frames are called "envelopes" in protocol v5 specs.
// +k8s:deepcopy-gen=true
type Frame struct {
	Header *Header
	Body   *Body
}

// RawFrame is a low-level representation of a frame, where the body is not decoded.
// Note that frames are called "envelopes" in protocol v5 specs.
// +k8s:deepcopy-gen=true
type RawFrame struct {
	Header *Header
	Body   []byte
}

// Header is the header of a frame.
// +k8s:deepcopy-gen=true
type Header struct {
	IsResponse bool
	Version    primitive.ProtocolVersion
	Flags      primitive.HeaderFlag
	// The stream id. A stream id is a signed byte (protocol versions 1 and 2) or a signed 16-bit integer (protocol
	// versions 3 and higher). Note that the protocol specs refer to the stream id as a primitive [short] integer,
	// but in fact stream ids are signed integers. Indeed, server-initiated messages, such as EVENT messages, have
	// negative stream ids. For this reason, stream ids are represented as signed 16-bit integers in this library.
	StreamId int16
	// The OpCode is an unsigned byte that distinguishes the type of payload that a frame contains.
	OpCode primitive.OpCode
	// The encoded body length. This is a computed value that users should not set themselves. When encoding a frame,
	// this field is not read but is rather dynamically computed from the actual body length. When decoding a frame,
	// this field is always correctly set to the exact decoded body length.
	BodyLength int32
}

// Body is the body of a frame.
// +k8s:deepcopy-gen=true
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

// NewFrame Creates a new Frame with the given version, stream id and message.
func NewFrame(version primitive.ProtocolVersion, streamId int16, message message.Message) *Frame {
	var flags primitive.HeaderFlag
	if version.IsBeta() {
		flags = flags.Add(primitive.HeaderFlagUseBeta)
	}
	return &Frame{
		Header: &Header{
			IsResponse: message.IsResponse(),
			Version:    version,
			Flags:      flags,
			StreamId:   streamId,
			OpCode:     message.GetOpCode(),
			BodyLength: 0, // will be set later when encoding
		},
		Body: &Body{
			Message: message,
		},
	}
}

// SetCustomPayload Sets a new custom payload on this frame, adjusting the header flags accordingly. If nil, the existing payload,
// if any, will be removed along with the corresponding header flag.
// Note: custom payloads cannot be used with protocol versions lesser than 4.
func (f *Frame) SetCustomPayload(customPayload map[string][]byte) {
	if len(customPayload) > 0 {
		f.Header.Flags = f.Header.Flags.Add(primitive.HeaderFlagCustomPayload)
	} else {
		f.Header.Flags = f.Header.Flags.Remove(primitive.HeaderFlagCustomPayload)
	}
	f.Body.CustomPayload = customPayload
}

// SetWarnings Sets new query warnings on this frame, adjusting the header flags accordingly. If nil, the existing warnings,
// if any, will be removed along with the corresponding header flag.
// Note: query warnings cannot be used with protocol versions lesser than 4.
func (f *Frame) SetWarnings(warnings []string) {
	if len(warnings) > 0 {
		f.Header.Flags = f.Header.Flags.Add(primitive.HeaderFlagWarning)
	} else {
		f.Header.Flags = f.Header.Flags.Remove(primitive.HeaderFlagWarning)
	}
	f.Body.Warnings = warnings
}

// SetTracingId Sets a new tracing id on this frame, adjusting the header flags accordingly. If nil, the existing tracing id,
// if any, will be removed along with the corresponding header flag.
// Note: tracing ids can only be used with response frames.
func (f *Frame) SetTracingId(tracingId *primitive.UUID) {
	if tracingId != nil {
		f.Header.Flags = f.Header.Flags.Add(primitive.HeaderFlagTracing)
	} else {
		f.Header.Flags = f.Header.Flags.Remove(primitive.HeaderFlagTracing)
	}
	f.Body.TracingId = tracingId
}

// RequestTracingId Configures this frame to request a tracing id from the server, adjusting the header flags accordingly.
// Note: this method should only be used for request frames.
func (f *Frame) RequestTracingId(tracing bool) {
	if tracing {
		f.Header.Flags = f.Header.Flags.Add(primitive.HeaderFlagTracing)
	} else {
		f.Header.Flags = f.Header.Flags.Remove(primitive.HeaderFlagTracing)
	}
}

// SetCompress Configures this frame to use compression, adjusting the header flags accordingly.
// Note: this method will not enable compression on frames that cannot be compressed.
// Also, enabling compression on a frame does not guarantee that the frame will be properly compressed:
// the frame codec must also be configured to use a BodyCompressor.
func (f *Frame) SetCompress(compress bool) {
	if compress && isCompressible(f.Body.Message.GetOpCode()) {
		f.Header.Flags = f.Header.Flags.Add(primitive.HeaderFlagCompressed)
	} else {
		f.Header.Flags = f.Header.Flags.Remove(primitive.HeaderFlagCompressed)
	}
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
