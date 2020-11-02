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
	codec := NewCodec(nil)
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
	codec := NewRawCodec(nil)
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
		"lz4":    NewCodec(lz4.Compressor{}),
		"snappy": NewCodec(snappy.Compressor{}),
	}
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
			Data:     [][][]byte{},
		},
		true,
	)
	codecs := map[string]RawCodec{
		"lz4":    NewRawCodec(lz4.Compressor{}),
		"snappy": NewRawCodec(snappy.Compressor{}),
	}
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
			Data:     [][][]byte{},
		},
		false,
	)
	codec := NewRawCodec(nil)
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
			assert.Equal(t, encodedBody.Bytes(), rawFrame.Body)
			assert.Equal(t, int32(encodedBody.Len()), rawFrame.Header.BodyLength)

			var fullFrame *Frame
			fullFrame, err = codec.ConvertFromRawFrame(rawFrame)
			assert.Equal(t, test.frame, fullFrame)
		})
	}
}
