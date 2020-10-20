package frame

import (
	"github.com/stretchr/testify/assert"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/compression"
	"go-cassandra-native-protocol/cassandraprotocol/message"
	"testing"
)

func TestFrameEncodeDecode(t *testing.T) {
	var uuid = cassandraprotocol.UUID{0xC0, 0xD1, 0xD2, 0x1E, 0xBB, 0x01, 0x41, 0x96, 0x86, 0xDB, 0xBC, 0x31, 0x7B, 0xC1, 0x79, 0x6A}
	startup, _ := NewRequestFrame(
		cassandraprotocol.ProtocolVersion4,
		1,
		true,
		map[string][]byte{"hello": {0xca, 0xfe, 0xba, 0xbe}},
		message.NewStartup(),
	)
	rows, _ := NewResponseFrame(
		cassandraprotocol.ProtocolVersion4,
		1,
		&uuid,
		map[string][]byte{"hello": {0xca, 0xfe, 0xba, 0xbe}},
		[]string{"you naughty boy"},
		&message.Rows{
			Metadata: message.NewRowsMetadata(message.WithoutColumnSpecs(1, nil, nil, nil)),
			Data:     [][][]byte{},
		})
	codec := NewCodec()
	tests := []struct {
		name  string
		frame *Frame
		err   error
	}{
		{"request", startup, nil},
		{"response", rows, nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if encodedFrame, err := codec.Encode(test.frame); err != nil {
				assert.Equal(t, test.err, err)
			} else if decodedFrame, err := codec.Decode(encodedFrame); err != nil {
				assert.Equal(t, test.err, err)
			} else {
				assert.Equal(t, test.frame, decodedFrame)
			}
		})
	}
}

func TestFrameEncodeDecodeWithCompression(t *testing.T) {
	var uuid = cassandraprotocol.UUID{0xC0, 0xD1, 0xD2, 0x1E, 0xBB, 0x01, 0x41, 0x96, 0x86, 0xDB, 0xBC, 0x31, 0x7B, 0xC1, 0x79, 0x6A}
	startup, _ := NewRequestFrame(
		cassandraprotocol.ProtocolVersion4,
		1,
		true,
		map[string][]byte{"hello": {0xca, 0xfe, 0xba, 0xbe}},
		message.NewStartup(),
	)
	rows, _ := NewResponseFrame(
		cassandraprotocol.ProtocolVersion4,
		1,
		&uuid,
		map[string][]byte{"hello": {0xca, 0xfe, 0xba, 0xbe}},
		[]string{"you naughty boy"},
		&message.Rows{
			Metadata: message.NewRowsMetadata(message.WithoutColumnSpecs(1, nil, nil, nil)),
			Data:     [][][]byte{},
		})
	codec := NewCodec(WithCompressor(compression.Lz4Compressor{}))
	tests := []struct {
		name  string
		frame *Frame
		err   error
	}{
		{"request", startup, nil},
		{"response", rows, nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if encodedFrame, err := codec.Encode(test.frame); err != nil {
				assert.Equal(t, test.err, err)
			} else if decodedFrame, err := codec.Decode(encodedFrame); err != nil {
				assert.Equal(t, test.err, err)
			} else {
				assert.Equal(t, test.frame, decodedFrame)
			}
		})
	}
}
