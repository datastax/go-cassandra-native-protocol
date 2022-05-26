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

package message

import (
	"bytes"
	"errors"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRevise_DeepCopy(t *testing.T) {
	obj := &Revise{
		RevisionType:   primitive.DseRevisionTypeCancelContinuousPaging,
		TargetStreamId: 5,
		NextPages:      10,
	}
	cloned := obj.DeepCopy()
	assert.Equal(t, obj, cloned)
	cloned.RevisionType = primitive.DseRevisionTypeMoreContinuousPages
	cloned.TargetStreamId = 6
	cloned.NextPages = 7
	assert.NotEqual(t, obj, cloned)
	assert.Equal(t, primitive.DseRevisionTypeCancelContinuousPaging, obj.RevisionType)
	assert.EqualValues(t, 5, obj.TargetStreamId)
	assert.EqualValues(t, 10, obj.NextPages)
	assert.Equal(t, primitive.DseRevisionTypeMoreContinuousPages, cloned.RevisionType)
	assert.EqualValues(t, 6, cloned.TargetStreamId)
	assert.EqualValues(t, 7, cloned.NextPages)
}

func TestReviseCodec_Encode(t *testing.T) {
	codec := &reviseCodec{}
	version := primitive.ProtocolVersionDse1
	t.Run(version.String(), func(t *testing.T) {
		tests := []encodeTestCase{
			{
				"simple revise",
				&Revise{
					RevisionType:   primitive.DseRevisionTypeCancelContinuousPaging,
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
	version = primitive.ProtocolVersionDse2
	t.Run(version.String(), func(t *testing.T) {
		tests := []encodeTestCase{
			{
				"revise cancel",
				&Revise{
					RevisionType:   primitive.DseRevisionTypeCancelContinuousPaging,
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
					RevisionType:   primitive.DseRevisionTypeMoreContinuousPages,
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
	codec := &reviseCodec{}
	version := primitive.ProtocolVersionDse1
	t.Run(version.String(), func(t *testing.T) {
		tests := []encodedLengthTestCase{
			{
				"simple revise",
				&Revise{
					RevisionType:   primitive.DseRevisionTypeCancelContinuousPaging,
					TargetStreamId: 123,
				},
				primitive.LengthOfInt * 2,
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
	version = primitive.ProtocolVersionDse2
	t.Run(version.String(), func(t *testing.T) {
		tests := []encodedLengthTestCase{
			{
				"revise cancel",
				&Revise{
					RevisionType:   primitive.DseRevisionTypeCancelContinuousPaging,
					TargetStreamId: 123,
				},
				primitive.LengthOfInt * 2,
				nil,
			},
			{
				"revise more",
				&Revise{
					RevisionType:   primitive.DseRevisionTypeMoreContinuousPages,
					TargetStreamId: 123,
					NextPages:      4,
				},
				primitive.LengthOfInt * 3,
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
	codec := &reviseCodec{}
	version := primitive.ProtocolVersionDse1
	t.Run(version.String(), func(t *testing.T) {
		tests := []decodeTestCase{
			{
				"simple revise",
				[]byte{
					0, 0, 0, 1, // revision type
					0, 0, 0, 123, // stream id
				},
				&Revise{
					RevisionType:   primitive.DseRevisionTypeCancelContinuousPaging,
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
	version = primitive.ProtocolVersionDse2
	t.Run(version.String(), func(t *testing.T) {
		tests := []decodeTestCase{
			{
				"revise cancel",
				[]byte{
					0, 0, 0, 1, // revision type
					0, 0, 0, 123, // stream id
				},
				&Revise{
					RevisionType:   primitive.DseRevisionTypeCancelContinuousPaging,
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
					RevisionType:   primitive.DseRevisionTypeMoreContinuousPages,
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
