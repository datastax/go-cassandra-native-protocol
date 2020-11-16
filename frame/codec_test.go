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
	"testing"
)

var uuid = primitive.UUID{0xC0, 0xD1, 0xD2, 0x1E, 0xBB, 0x01, 0x41, 0x96, 0x86, 0xDB, 0xBC, 0x31, 0x7B, 0xC1, 0x79, 0x6A}

// The tests in this file are meant to focus on encoding / decoding of frame headers and other common parts of
// the frame body, such as custom payloads, query warnings and tracing ids. They do not focus on encoding / decoding
// specific messages.

func TestFrameEncodeDecode(t *testing.T) {
	var request, _ = NewRequestFrame(
		primitive.ProtocolVersion4,
		1,
		true,
		map[string][]byte{"hello": {0xca, 0xfe, 0xba, 0xbe}},
		message.NewStartup(),
		false,
	)
	var response, _ = NewResponseFrame(
		primitive.ProtocolVersion4,
		1,
		&uuid,
		map[string][]byte{"hello": {0xca, 0xfe, 0xba, 0xbe}},
		[]string{"I'm warning you!!"},
		&message.RowsResult{
			Metadata: &message.RowsMetadata{ColumnCount: 1},
			Data:     [][][]byte{},
		},
		false,
	)
	codec := NewCodec()
	tests := []struct {
		name  string
		frame *Frame
		err   error
	}{
		{"request", request, nil},
		{"response", response, nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encodedFrame := bytes.Buffer{}
			if err := codec.EncodeFrame(test.frame, &encodedFrame); err != nil {
				assert.Equal(t, test.err, err)
			} else if decodedFrame, err := codec.DecodeFrame(&encodedFrame); err != nil {
				assert.Equal(t, test.err, err)
			} else {
				assert.Equal(t, test.frame, decodedFrame)
			}
		})
	}
}

func TestRawFrameEncodeDecode(t *testing.T) {
	var request, _ = NewRequestFrame(
		primitive.ProtocolVersion4,
		1,
		true,
		map[string][]byte{"hello": {0xca, 0xfe, 0xba, 0xbe}},
		message.NewStartup(),
		false,
	)
	var response, _ = NewResponseFrame(
		primitive.ProtocolVersion4,
		1,
		&uuid,
		map[string][]byte{"hello": {0xca, 0xfe, 0xba, 0xbe}},
		[]string{"I'm warning you!!"},
		&message.RowsResult{
			Metadata: &message.RowsMetadata{ColumnCount: 1},
			Data:     [][][]byte{},
		},
		false,
	)
	codec := NewRawCodec()
	tests := []struct {
		name  string
		frame *Frame
		err   error
	}{
		{"request", request, nil},
		{"response", response, nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var rawFrame *RawFrame
			var err error
			rawFrame, err = codec.ConvertToRawFrame(test.frame)
			assert.Equal(t, test.err, err)
			encodedFrame := &bytes.Buffer{}
			err = codec.EncodeRawFrame(rawFrame, encodedFrame)
			assert.Equal(t, test.err, err)
			decodedFrame, err := codec.DecodeRawFrame(encodedFrame)
			assert.Equal(t, test.err, err)
			assert.Equal(t, rawFrame, decodedFrame)
		})
	}
}

func TestFrameEncodeDecodeWithCompression(t *testing.T) {
	var request, _ = NewRequestFrame(
		primitive.ProtocolVersion4,
		1,
		true,
		map[string][]byte{"hello": {0xca, 0xfe, 0xba, 0xbe}},
		message.NewStartup(),
		true,
	)
	var response, _ = NewResponseFrame(
		primitive.ProtocolVersion4,
		1,
		&uuid,
		map[string][]byte{"hello": {0xca, 0xfe, 0xba, 0xbe}},
		[]string{"I'm warning you!!"},
		&message.RowsResult{
			Metadata: &message.RowsMetadata{ColumnCount: 1},
			Data:     [][][]byte{},
		},
		true,
	)
	codecs := map[string]Codec{
		"lz4":    NewCodec(),
		"snappy": NewCodec(),
	}
	codecs["lz4"].SetBodyCompressor(lz4.BodyCompressor{})
	codecs["snappy"].SetBodyCompressor(snappy.BodyCompressor{})
	tests := []struct {
		name  string
		frame *Frame
		err   error
	}{
		{"request", request, nil},
		{"response", response, nil},
	}
	for algorithm, codec := range codecs {
		t.Run(algorithm, func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					encodedFrame := bytes.Buffer{}
					if err := codec.EncodeFrame(test.frame, &encodedFrame); err != nil {
						assert.Equal(t, test.err, err)
					} else if decodedFrame, err := codec.DecodeFrame(&encodedFrame); err != nil {
						assert.Equal(t, test.err, err)
					} else {
						assert.Equal(t, test.frame, decodedFrame)
					}
				})
			}
		})
	}
}

func TestRawFrameEncodeDecodeWithCompression(t *testing.T) {
	var request, _ = NewRequestFrame(
		primitive.ProtocolVersion4,
		1,
		true,
		map[string][]byte{"hello": {0xca, 0xfe, 0xba, 0xbe}},
		message.NewStartup(),
		true,
	)
	var response, _ = NewResponseFrame(
		primitive.ProtocolVersion4,
		1,
		&uuid,
		map[string][]byte{"hello": {0xca, 0xfe, 0xba, 0xbe}},
		[]string{"I'm warning you!!"},
		&message.RowsResult{
			Metadata: &message.RowsMetadata{ColumnCount: 1},
			Data:     message.RowSet{},
		},
		true,
	)
	codecs := map[string]RawCodec{
		"lz4":    NewRawCodec(),
		"snappy": NewRawCodec(),
	}
	codecs["lz4"].SetBodyCompressor(lz4.BodyCompressor{})
	codecs["snappy"].SetBodyCompressor(snappy.BodyCompressor{})
	tests := []struct {
		name  string
		frame *Frame
		err   error
	}{
		{"request", request, nil},
		{"response", response, nil},
	}
	for algorithm, codec := range codecs {
		t.Run(algorithm, func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					var rawFrame *RawFrame
					var err error
					rawFrame, err = codec.ConvertToRawFrame(test.frame)
					assert.Equal(t, test.err, err)
					encodedFrame := &bytes.Buffer{}
					err = codec.EncodeRawFrame(rawFrame, encodedFrame)
					assert.Equal(t, test.err, err)
					decodedFrame, err := codec.DecodeRawFrame(encodedFrame)
					assert.Equal(t, test.err, err)
					assert.Equal(t, rawFrame, decodedFrame)
				})
			}
		})
	}
}

func TestConvertToRawFrame(t *testing.T) {
	var request, _ = NewRequestFrame(
		primitive.ProtocolVersion4,
		1,
		true,
		map[string][]byte{"hello": {0xca, 0xfe, 0xba, 0xbe}},
		message.NewStartup(),
		false,
	)
	var response, _ = NewResponseFrame(
		primitive.ProtocolVersion4,
		1,
		&uuid,
		map[string][]byte{"hello": {0xca, 0xfe, 0xba, 0xbe}},
		[]string{"I'm warning you!!"},
		&message.RowsResult{
			Metadata: &message.RowsMetadata{ColumnCount: 1},
			Data:     message.RowSet{},
		},
		false,
	)
	codec := NewRawCodec()
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
}
