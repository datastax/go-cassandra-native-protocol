package message

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"go-cassandra-native-protocol/cassandraprotocol"
	"testing"
)

func TestReadyCodec_Encode(t *testing.T) {
	codec := &ReadyCodec{}
	for version := cassandraprotocol.ProtocolVersionMin; version <= cassandraprotocol.ProtocolVersionBeta; version++ {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []struct {
				name     string
				input    Message
				expected []byte
				err      error
			}{
				{
					"ready simple",
					&Ready{},
					nil,
					nil,
				},
				{
					"not a ready",
					&Options{},
					nil,
					errors.New("expected *message.Ready, got *message.Options"),
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					dest := &bytes.Buffer{}
					err := codec.Encode(tt.input, dest, version)
					assert.Equal(t, tt.expected, dest.Bytes())
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
}

func TestReadyCodec_EncodedLength(t *testing.T) {
	codec := &ReadyCodec{}
	for version := cassandraprotocol.ProtocolVersionMin; version <= cassandraprotocol.ProtocolVersionBeta; version++ {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []struct {
				name     string
				input    Message
				expected int
				err      error
			}{
				{
					"ready simple",
					&Ready{},
					0,
					nil,
				},
				{
					"not a ready",
					&Options{},
					-1,
					errors.New("expected *message.Ready, got *message.Options"),
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					actual, err := codec.EncodedLength(tt.input, version)
					assert.Equal(t, tt.expected, actual)
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
}

func TestReadyCodec_Decode(t *testing.T) {
	codec := &ReadyCodec{}
	for version := cassandraprotocol.ProtocolVersionMin; version <= cassandraprotocol.ProtocolVersionBeta; version++ {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
			tests := []struct {
				name     string
				input    []byte
				expected Message
				err      error
			}{
				{
					"ready simple",
					[]byte{},
					&Ready{},
					nil,
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					source := bytes.NewBuffer(tt.input)
					actual, err := codec.Decode(source, version)
					assert.Equal(t, tt.expected, actual)
					assert.Equal(t, tt.err, err)
				})
			}
		})
	}
}
