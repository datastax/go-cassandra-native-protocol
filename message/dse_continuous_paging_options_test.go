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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

func TestContinuousPagingOptions_DeepCopy(t *testing.T) {
	obj := &ContinuousPagingOptions{
		MaxPages:       1,
		PagesPerSecond: 2,
		NextPages:      3,
	}
	cloned := obj.DeepCopy()
	assert.Equal(t, obj, cloned)
	cloned.MaxPages = 5
	cloned.PagesPerSecond = 6
	cloned.NextPages = 7
	assert.NotEqual(t, obj, cloned)
	assert.EqualValues(t, 1, obj.MaxPages)
	assert.EqualValues(t, 2, obj.PagesPerSecond)
	assert.EqualValues(t, 3, obj.NextPages)
	assert.EqualValues(t, 5, cloned.MaxPages)
	assert.EqualValues(t, 6, cloned.PagesPerSecond)
	assert.EqualValues(t, 7, cloned.NextPages)
}

func TestContinuousPagingOptions_Encode(t *testing.T) {
	version := primitive.ProtocolVersionDse1
	t.Run(version.String(), func(t *testing.T) {
		tests := []struct {
			name     string
			input    *ContinuousPagingOptions
			expected []byte
			err      error
		}{
			{
				"empty options",
				&ContinuousPagingOptions{},
				[]byte{
					0, 0, 0, 0, // max pages
					0, 0, 0, 0, // pages per sec
				},
				nil,
			},
			{
				"simple options",
				&ContinuousPagingOptions{
					MaxPages:       200,
					PagesPerSecond: 150,
				},
				[]byte{
					0, 0, 0, 200, // max pages
					0, 0, 0, 150, // pages per sec
				},
				nil,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				dest := &bytes.Buffer{}
				err := EncodeContinuousPagingOptions(tt.input, dest, version)
				assert.Equal(t, tt.expected, dest.Bytes())
				assert.Equal(t, tt.err, err)
			})
		}
	})
	version = primitive.ProtocolVersionDse2
	t.Run(version.String(), func(t *testing.T) {
		tests := []struct {
			name     string
			input    *ContinuousPagingOptions
			expected []byte
			err      error
		}{
			{
				"empty options",
				&ContinuousPagingOptions{},
				[]byte{
					0, 0, 0, 0, // max pages
					0, 0, 0, 0, // pages per sec
					0, 0, 0, 0, // next pages
				},
				nil,
			},
			{
				"simple options",
				&ContinuousPagingOptions{
					MaxPages:       200,
					PagesPerSecond: 150,
					NextPages:      100,
				},
				[]byte{
					0, 0, 0, 200, // max pages
					0, 0, 0, 150, // pages per sec
					0, 0, 0, 100, // next pages
				},
				nil,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				dest := &bytes.Buffer{}
				err := EncodeContinuousPagingOptions(tt.input, dest, version)
				assert.Equal(t, tt.expected, dest.Bytes())
				assert.Equal(t, tt.err, err)
			})
		}
	})
}

func TestContinuousPagingOptions_EncodedLength(t *testing.T) {
	version := primitive.ProtocolVersionDse1
	t.Run(version.String(), func(t *testing.T) {
		tests := []struct {
			name     string
			input    *ContinuousPagingOptions
			expected int
			err      error
		}{
			{
				"empty options",
				&ContinuousPagingOptions{},
				primitive.LengthOfInt * 2,
				nil,
			},
			{
				"simple options",
				&ContinuousPagingOptions{
					MaxPages:       200,
					PagesPerSecond: 150,
				},
				primitive.LengthOfInt * 2,
				nil,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				actual, err := LengthOfContinuousPagingOptions(tt.input, version)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
	version = primitive.ProtocolVersionDse2
	t.Run(version.String(), func(t *testing.T) {
		tests := []struct {
			name     string
			input    *ContinuousPagingOptions
			expected int
			err      error
		}{
			{
				"empty options",
				&ContinuousPagingOptions{},
				primitive.LengthOfInt * 3,
				nil,
			},
			{
				"simple options",
				&ContinuousPagingOptions{
					MaxPages:       200,
					PagesPerSecond: 150,
					NextPages:      100,
				},
				primitive.LengthOfInt * 3,
				nil,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				actual, err := LengthOfContinuousPagingOptions(tt.input, version)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
}

func TestContinuousPagingOptions_Decode(t *testing.T) {
	version := primitive.ProtocolVersionDse1
	t.Run(version.String(), func(t *testing.T) {
		tests := []struct {
			name     string
			input    []byte
			expected *ContinuousPagingOptions
			err      error
		}{
			{
				"empty options",
				[]byte{
					0, 0, 0, 0, // max pages
					0, 0, 0, 0, // pages per sec
				},
				&ContinuousPagingOptions{},
				nil,
			},
			{
				"simple options",
				[]byte{
					0, 0, 0, 200, // max pages
					0, 0, 0, 150, // pages per sec
				},
				&ContinuousPagingOptions{
					MaxPages:       200,
					PagesPerSecond: 150,
				},
				nil,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				source := bytes.NewBuffer(tt.input)
				actual, err := DecodeContinuousPagingOptions(source, version)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
	version = primitive.ProtocolVersionDse2
	t.Run(version.String(), func(t *testing.T) {
		tests := []struct {
			name     string
			input    []byte
			expected *ContinuousPagingOptions
			err      error
		}{
			{
				"empty options",
				[]byte{
					0, 0, 0, 0, // max pages
					0, 0, 0, 0, // pages per sec
					0, 0, 0, 0, // next pages
				},
				&ContinuousPagingOptions{},
				nil,
			},
			{
				"simple options",
				[]byte{
					0, 0, 0, 200, // max pages
					0, 0, 0, 150, // pages per sec
					0, 0, 0, 100, // next pages
				},
				&ContinuousPagingOptions{
					MaxPages:       200,
					PagesPerSecond: 150,
					NextPages:      100,
				},
				nil,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				source := bytes.NewBuffer(tt.input)
				actual, err := DecodeContinuousPagingOptions(source, version)
				assert.Equal(t, tt.expected, actual)
				assert.Equal(t, tt.err, err)
			})
		}
	})
}
