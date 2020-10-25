package frame

import (
	"bytes"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/compression"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/message"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitives"
	"github.com/stretchr/testify/assert"
	"testing"
)

var uuid = primitives.UUID{0xC0, 0xD1, 0xD2, 0x1E, 0xBB, 0x01, 0x41, 0x96, 0x86, 0xDB, 0xBC, 0x31, 0x7B, 0xC1, 0x79, 0x6A}

var request, _ = NewRequestFrame(
	cassandraprotocol.ProtocolVersion4,
	1,
	true,
	map[string][]byte{"hello": {0xca, 0xfe, 0xba, 0xbe}},
	message.NewStartup(),
)

var response, _ = NewResponseFrame(
	cassandraprotocol.ProtocolVersion4,
	1,
	&uuid,
	map[string][]byte{"hello": {0xca, 0xfe, 0xba, 0xbe}},
	[]string{"I'm warning you!!"},
	&message.RowsResult{
		Metadata: message.NewRowsMetadata(message.NoColumnMetadata(1)),
		Data:     [][][]byte{},
	})

// The tests in this file are meant to focus on encoding / decoding of frame headers and other common parts of
// the frame body, such as custom payloads, query warnings and tracing ids. They do not focus on encoding / decoding
// specific messages.

func TestFrameEncodeDecode(t *testing.T) {
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
			if err := codec.Encode(test.frame, &encodedFrame); err != nil {
				assert.Equal(t, test.err, err)
			} else if decodedFrame, err := codec.Decode(&encodedFrame); err != nil {
				assert.Equal(t, test.err, err)
			} else {
				assert.Equal(t, test.frame, decodedFrame)
			}
		})
	}
}

func TestFrameEncodeDecodeWithCompression(t *testing.T) {
	codecs := map[string]*Codec{
		"lz4":    NewCodec(WithCompressor(compression.Lz4Compressor{})),
		"snappy": NewCodec(WithCompressor(compression.SnappyCompressor{})),
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
					if err := codec.Encode(test.frame, &encodedFrame); err != nil {
						assert.Equal(t, test.err, err)
					} else if decodedFrame, err := codec.Decode(&encodedFrame); err != nil {
						assert.Equal(t, test.err, err)
					} else {
						assert.Equal(t, test.frame, decodedFrame)
					}
				})
			}
		})
	}
}
