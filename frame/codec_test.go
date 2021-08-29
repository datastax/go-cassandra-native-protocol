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
	"github.com/datastax/go-cassandra-native-protocol/compression/lz4"
	"github.com/datastax/go-cassandra-native-protocol/compression/snappy"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

// The tests in this file are meant to focus on encoding / decoding of frame headers and other common parts of
// the frame body, such as custom payloads, query warnings and tracing ids. They do not focus on encoding / decoding
// specific messages.

func TestFrameEncodeDecode(t *testing.T) {
	codecs := createCodecs()
	for _, version := range primitive.SupportedProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
			request, response := createFrames(version)
			for algorithm, codec := range codecs {
				t.Run(algorithm, func(t *testing.T) {
					tests := []struct {
						name  string
						frame *Frame
					}{
						{"request", request},
						{"response", response},
					}
					for _, test := range tests {
						t.Run(test.name, func(t *testing.T) {
							encodedFrame := bytes.Buffer{}
							err := codec.EncodeFrame(test.frame, &encodedFrame)
							require.Nil(t, err)
							decodedFrame, err := codec.DecodeFrame(&encodedFrame)
							require.Nil(t, err)
							require.Equal(t, test.frame, decodedFrame)
						})
					}
				})
			}
		})
	}
}

func TestRawFrameEncodeDecode(t *testing.T) {
	codecs := createCodecs()
	for _, version := range primitive.SupportedProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
			request, response := createFrames(version)
			for algorithm, codec := range codecs {
				t.Run(algorithm, func(t *testing.T) {
					tests := []struct {
						name  string
						frame *Frame
					}{
						{"request", request},
						{"response", response},
					}
					for _, test := range tests {
						t.Run(test.name, func(t *testing.T) {
							rawFrame, err := codec.ConvertToRawFrame(test.frame)
							require.Nil(t, err)
							encodedFrame := &bytes.Buffer{}
							err = codec.EncodeRawFrame(rawFrame, encodedFrame)
							require.Nil(t, err)
							decodedFrame, err := codec.DecodeRawFrame(encodedFrame)
							require.Nil(t, err)
							assert.Equal(t, rawFrame, decodedFrame)
						})
					}
				})
			}
		})
	}
}

func TestConvertToRawFrame(t *testing.T) {
	codec := NewRawCodec()
	for _, version := range primitive.SupportedProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {
			request, response := createFrames(version)
			tests := []struct {
				name  string
				frame *Frame
			}{
				{"request", request},
				{"response", response},
			}
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					var rawFrame *RawFrame
					var err error
					rawFrame, err = codec.ConvertToRawFrame(test.frame)
					assert.Nil(t, err)
					assert.Equal(t, test.frame.Header, rawFrame.Header)
					assert.Equal(t, test.frame.Body.Message.GetOpCode(), rawFrame.Header.OpCode)
					assert.Equal(t, test.frame.Body.Message.IsResponse(), rawFrame.Header.IsResponse)

					encodedBody := &bytes.Buffer{}
					err = codec.EncodeBody(test.frame.Header, test.frame.Body, encodedBody)
					assert.Nil(t, err)
					encodedBodyBytes := encodedBody.Bytes()
					assert.Equal(t, encodedBodyBytes, rawFrame.Body)
					assert.Equal(t, int32(encodedBody.Len()), rawFrame.Header.BodyLength)

					rawBody, err := codec.DecodeRawBody(test.frame.Header, encodedBody)
					assert.Nil(t, err)
					assert.Equal(t, encodedBodyBytes, rawBody)

					fullFrame, err := codec.ConvertFromRawFrame(rawFrame)
					assert.Nil(t, err)
					assert.Equal(t, test.frame, fullFrame)
				})
			}
		})
	}
}

func createCodecs() map[string]RawCodec {
	codecs := map[string]RawCodec{
		"NONE":   NewRawCodec(),
		"LZ4":    NewRawCodecWithCompression(lz4.Compressor{}),
		"SNAPPY": NewRawCodecWithCompression(snappy.Compressor{}),
	}
	return codecs
}

func createFrames(version primitive.ProtocolVersion) (*Frame, *Frame) {
	var request = NewFrame(version, 1, message.NewStartup())
	var response = NewFrame(version, 1, &message.RowsResult{
		Metadata: &message.RowsMetadata{ColumnCount: 1},
		Data:     [][][]byte{},
	})
	request.RequestTracingId(true)
	var uuid = primitive.UUID{0xC0, 0xD1, 0xD2, 0x1E, 0xBB, 0x01, 0x41, 0x96, 0x86, 0xDB, 0xBC, 0x31, 0x7B, 0xC1, 0x79, 0x6A}
	response.SetTracingId(&uuid)
	if version >= primitive.ProtocolVersion4 {
		request.SetCustomPayload(map[string][]byte{"hello": {0xca, 0xfe, 0xba, 0xbe}})
		response.SetCustomPayload(map[string][]byte{"hello": {0xca, 0xfe, 0xba, 0xbe}})
		response.SetWarnings([]string{"I'm warning you!!"})
	}
	return request, response
}
