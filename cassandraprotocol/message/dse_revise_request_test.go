package message

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitives"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReviseCodec_Encode(t *testing.T) {
	codec := &ReviseCodec{}
	version := cassandraprotocol.ProtocolVersionDse1
	t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
		tests := []encodeTestCase{
			{
				"simple revise",
				&Revise{
					RevisionType:   cassandraprotocol.DseRevisionTypeCancelContinuousPaging,
					TargetStreamId: 123,
				},
				[]byte{
					0, 0, 0, 1, // revision type
					0, 0, 0, 123, // stream id
				},
				nil,
			},
			{
				"not a revise",
				&AuthChallenge{[]byte{0xca, 0xfe, 0xba, 0xbe}},
				nil,
				errors.New("expected *message.Revise, got *message.AuthChallenge"),
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
	version = cassandraprotocol.ProtocolVersionDse2
	t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
		tests := []encodeTestCase{
			{
				"revise cancel",
				&Revise{
					RevisionType:   cassandraprotocol.DseRevisionTypeCancelContinuousPaging,
					TargetStreamId: 123,
				},
				[]byte{
					0, 0, 0, 1, // revision type
					0, 0, 0, 123, // stream id
				},
				nil,
			},
			{
				"revise more",
				&Revise{
					RevisionType:   cassandraprotocol.DseRevisionTypeMoreContinuousPages,
					TargetStreamId: 123,
					NextPages:      4,
				},
				[]byte{
					0, 0, 0, 2, // revision type
					0, 0, 0, 123, // stream id
					0, 0, 0, 4, // next pages
				},
				nil,
			},
			{
				"not a revise",
				&AuthChallenge{[]byte{0xca, 0xfe, 0xba, 0xbe}},
				nil,
				errors.New("expected *message.Revise, got *message.AuthChallenge"),
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

func TestReviseCodec_EncodedLength(t *testing.T) {
	codec := &ReviseCodec{}
	version := cassandraprotocol.ProtocolVersionDse1
	t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
		tests := []encodedLengthTestCase{
			{
				"simple revise",
				&Revise{
					RevisionType:   cassandraprotocol.DseRevisionTypeCancelContinuousPaging,
					TargetStreamId: 123,
				},
				primitives.LengthOfInt * 2,
				nil,
			},
			{
				"not a revise",
				&AuthChallenge{[]byte{0xca, 0xfe, 0xba, 0xbe}},
				-1,
				errors.New("expected *message.Revise, got *message.AuthChallenge"),
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
	version = cassandraprotocol.ProtocolVersionDse2
	t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
		tests := []encodedLengthTestCase{
			{
				"revise cancel",
				&Revise{
					RevisionType:   cassandraprotocol.DseRevisionTypeCancelContinuousPaging,
					TargetStreamId: 123,
				},
				primitives.LengthOfInt * 2,
				nil,
			},
			{
				"revise more",
				&Revise{
					RevisionType:   cassandraprotocol.DseRevisionTypeMoreContinuousPages,
					TargetStreamId: 123,
					NextPages:      4,
				},
				primitives.LengthOfInt * 3,
				nil,
			},
			{
				"not a revise",
				&AuthChallenge{[]byte{0xca, 0xfe, 0xba, 0xbe}},
				-1,
				errors.New("expected *message.Revise, got *message.AuthChallenge"),
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

func TestReviseCodec_Decode(t *testing.T) {
	codec := &ReviseCodec{}
	version := cassandraprotocol.ProtocolVersionDse1
	t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
		tests := []decodeTestCase{
			{
				"simple revise",
				[]byte{
					0, 0, 0, 1, // revision type
					0, 0, 0, 123, // stream id
				},
				&Revise{
					RevisionType:   cassandraprotocol.DseRevisionTypeCancelContinuousPaging,
					TargetStreamId: 123,
				},
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
	version = cassandraprotocol.ProtocolVersionDse2
	t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {
		tests := []decodeTestCase{
			{
				"revise cancel",
				[]byte{
					0, 0, 0, 1, // revision type
					0, 0, 0, 123, // stream id
				},
				&Revise{
					RevisionType:   cassandraprotocol.DseRevisionTypeCancelContinuousPaging,
					TargetStreamId: 123,
				},
				nil,
			},
			{
				"revise more",
				[]byte{
					0, 0, 0, 2, // revision type
					0, 0, 0, 123, // stream id
					0, 0, 0, 4, // next pages
				},
				&Revise{
					RevisionType:   cassandraprotocol.DseRevisionTypeMoreContinuousPages,
					TargetStreamId: 123,
					NextPages:      4,
				},
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
