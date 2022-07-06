// Copyright 2021 DataStax
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

package datacodec

import (
	"math"
	"math/big"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_int64ToInt(t *testing.T) {
	tests := []struct {
		name    string
		val     int64
		intSize int
		wantVal int64
		wantErr string
	}{
		{"32 bits out of range pos", math.MaxInt32 + 1, 32, 0, "value out of range: 2147483648"},
		{"32 bits out of range neg", math.MinInt32 - 1, 32, 0, "value out of range: -2147483649"},
		{"32 bits max", math.MaxInt32, 32, math.MaxInt32, ""},
		{"32 bits min", math.MinInt32, 32, math.MinInt32, ""},
	}
	if strconv.IntSize == 64 {
		tests = append(tests, []struct {
			name    string
			val     int64
			intSize int
			wantVal int64
			wantErr string
		}{
			{"64 bits max", math.MaxInt64, 64, math.MaxInt64, ""},
			{"64 bits min", math.MinInt64, 64, math.MinInt64, ""},
		}...)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := int64ToInt(tt.val, tt.intSize)
			assert.EqualValues(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_int64ToInt32(t *testing.T) {
	tests := []struct {
		name    string
		val     int64
		wantVal int32
		wantErr string
	}{
		{"out of range pos", math.MaxInt32 + 1, 0, "value out of range: 2147483648"},
		{"out of range neg", math.MinInt32 - 1, 0, "value out of range: -2147483649"},
		{"max", math.MaxInt32, math.MaxInt32, ""},
		{"min", math.MinInt32, math.MinInt32, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := int64ToInt32(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_int64ToInt16(t *testing.T) {
	tests := []struct {
		name    string
		val     int64
		wantVal int16
		wantErr string
	}{
		{"out of range pos", math.MaxInt16 + 1, 0, "value out of range: 32768"},
		{"out of range neg", math.MinInt16 - 1, 0, "value out of range: -32769"},
		{"max", math.MaxInt16, math.MaxInt16, ""},
		{"min", math.MinInt16, math.MinInt16, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := int64ToInt16(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_int64ToInt8(t *testing.T) {
	tests := []struct {
		name    string
		val     int64
		wantVal int8
		wantErr string
	}{
		{"out of range pos", math.MaxInt8 + 1, 0, "value out of range: 128"},
		{"out of range neg", math.MinInt8 - 1, 0, "value out of range: -129"},
		{"max", math.MaxInt8, math.MaxInt8, ""},
		{"min", math.MinInt8, math.MinInt8, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := int64ToInt8(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_int64ToUint64(t *testing.T) {
	tests := []struct {
		name    string
		val     int64
		wantVal uint64
		wantErr string
	}{
		{"out of range neg", -1, 0, "value out of range: -1"},
		{"max", math.MaxInt64, math.MaxInt64, ""},
		{"min", 0, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := int64ToUint64(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_int64ToUint32(t *testing.T) {
	tests := []struct {
		name    string
		val     int64
		wantVal uint32
		wantErr string
	}{
		{"out of range neg", -1, 0, "value out of range: -1"},
		{"out of range pos", math.MaxUint32 + 1, 0, "value out of range: 4294967296"},
		{"max", math.MaxUint32, math.MaxUint32, ""},
		{"min", 0, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := int64ToUint32(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_int64ToUint(t *testing.T) {
	tests := []struct {
		name    string
		val     int64
		intSize int
		wantVal uint64
		wantErr string
	}{
		{"32 bits out of range pos", math.MaxUint32 + 1, 32, 0, "value out of range: 4294967296"},
		{"32 bits out of range neg", -1, 32, 0, "value out of range: -1"},
		{"32 bits max", math.MaxUint32, 32, math.MaxUint32, ""},
		{"32 bits min", 0, 32, 0, ""},
	}
	if strconv.IntSize == 64 {
		tests = append(tests, []struct {
			name    string
			val     int64
			intSize int
			wantVal uint64
			wantErr string
		}{
			{"64 bits out of range neg", -1, 64, 0, "value out of range: -1"},
			{"64 bits max", math.MaxInt64, 64, math.MaxInt64, ""},
			{"64 bits min", 0, 64, 0, ""},
		}...)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := int64ToUint(tt.val, tt.intSize)
			assert.EqualValues(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_int64ToUint16(t *testing.T) {
	tests := []struct {
		name    string
		val     int64
		wantVal uint16
		wantErr string
	}{
		{"out of range neg", -1, 0, "value out of range: -1"},
		{"out of range pos", math.MaxUint16 + 1, 0, "value out of range: 65536"},
		{"max", math.MaxUint16, math.MaxUint16, ""},
		{"min", 0, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := int64ToUint16(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_int64ToUint8(t *testing.T) {
	tests := []struct {
		name    string
		val     int64
		wantVal uint8
		wantErr string
	}{
		{"out of range neg", -1, 0, "value out of range: -1"},
		{"out of range pos", math.MaxUint8 + 1, 0, "value out of range: 256"},
		{"max", math.MaxUint8, math.MaxUint8, ""},
		{"min", 0, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := int64ToUint8(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_intToInt32(t *testing.T) {
	tests := []struct {
		name    string
		val     int
		wantVal int32
		wantErr string
	}{
		{"out of range neg", math.MinInt32 - 1, 0, "value out of range: -2147483649"},
		{"out of range pos", math.MaxInt32 + 1, 0, "value out of range: 2147483648"},
		{"max", math.MaxInt32, math.MaxInt32, ""},
		{"min", math.MinInt32, math.MinInt32, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := intToInt32(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_intToInt16(t *testing.T) {
	tests := []struct {
		name    string
		val     int
		wantVal int16
		wantErr string
	}{
		{"out of range neg", math.MinInt16 - 1, 0, "value out of range: -32769"},
		{"out of range pos", math.MaxInt16 + 1, 0, "value out of range: 32768"},
		{"max", math.MaxInt16, math.MaxInt16, ""},
		{"min", math.MinInt16, math.MinInt16, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := intToInt16(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_intToInt8(t *testing.T) {
	tests := []struct {
		name    string
		val     int
		wantVal int8
		wantErr string
	}{
		{"out of range neg", math.MinInt8 - 1, 0, "value out of range: -1"},
		{"out of range pos", math.MaxInt8 + 1, 0, "value out of range: 128"},
		{"max", math.MaxInt8, math.MaxInt8, ""},
		{"min", math.MinInt8, math.MinInt8, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := intToInt8(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_int32ToInt16(t *testing.T) {
	tests := []struct {
		name    string
		val     int32
		wantVal int16
		wantErr string
	}{
		{"out of range pos", math.MaxInt16 + 1, 0, "value out of range: 32768"},
		{"out of range neg", math.MinInt16 - 1, 0, "value out of range: -32769"},
		{"max", math.MaxInt16, math.MaxInt16, ""},
		{"min", math.MinInt16, math.MinInt16, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := int32ToInt16(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_int32ToInt8(t *testing.T) {
	tests := []struct {
		name    string
		val     int32
		wantVal int8
		wantErr string
	}{
		{"out of range pos", math.MaxInt8 + 1, 0, "value out of range: 128"},
		{"out of range neg", math.MinInt8 - 1, 0, "value out of range: -129"},
		{"max", math.MaxInt8, math.MaxInt8, ""},
		{"min", math.MinInt8, math.MinInt8, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := int32ToInt8(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_int32ToUint64(t *testing.T) {
	tests := []struct {
		name    string
		val     int32
		wantVal uint64
		wantErr string
	}{
		{"out of range neg", -1, 0, "value out of range: -1"},
		{"max", math.MaxInt32, math.MaxInt32, ""},
		{"min", 0, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := int32ToUint64(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_int32ToUint(t *testing.T) {
	tests := []struct {
		name    string
		val     int32
		wantVal uint
		wantErr string
	}{
		{"out of range neg", -1, 0, "value out of range: -1"},
		{"max", math.MaxInt32, math.MaxInt32, ""},
		{"min", 0, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := int32ToUint(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_int32ToUint32(t *testing.T) {
	tests := []struct {
		name    string
		val     int32
		wantVal uint32
		wantErr string
	}{
		{"out of range neg", -1, 0, "value out of range: -1"},
		{"max", math.MaxInt32, math.MaxInt32, ""},
		{"min", 0, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := int32ToUint32(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_int32ToUint16(t *testing.T) {
	tests := []struct {
		name    string
		val     int32
		wantVal uint16
		wantErr string
	}{
		{"out of range neg", -1, 0, "value out of range: -1"},
		{"max", math.MaxInt16, math.MaxInt16, ""},
		{"min", 0, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := int32ToUint16(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_int32ToUint8(t *testing.T) {
	tests := []struct {
		name    string
		val     int32
		wantVal uint8
		wantErr string
	}{
		{"out of range neg", -1, 0, "value out of range: -1"},
		{"max", math.MaxInt8, math.MaxInt8, ""},
		{"min", 0, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := int32ToUint8(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_int16ToInt8(t *testing.T) {
	tests := []struct {
		name    string
		val     int16
		wantVal int8
		wantErr string
	}{
		{"out of range pos", math.MaxInt8 + 1, 0, "value out of range: 128"},
		{"out of range neg", math.MinInt8 - 1, 0, "value out of range: -129"},
		{"max", math.MaxInt8, math.MaxInt8, ""},
		{"min", math.MinInt8, math.MinInt8, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := int16ToInt8(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_int16ToUint64(t *testing.T) {
	tests := []struct {
		name    string
		val     int16
		wantVal uint64
		wantErr string
	}{
		{"out of range neg", -1, 0, "value out of range: -1"},
		{"max", math.MaxInt16, math.MaxInt16, ""},
		{"min", 0, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := int16ToUint64(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_int16ToUint(t *testing.T) {
	tests := []struct {
		name    string
		val     int16
		wantVal uint
		wantErr string
	}{
		{"out of range neg", -1, 0, "value out of range: -1"},
		{"max", math.MaxInt16, math.MaxInt16, ""},
		{"min", 0, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := int16ToUint(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_int16ToUint32(t *testing.T) {
	tests := []struct {
		name    string
		val     int16
		wantVal uint32
		wantErr string
	}{
		{"out of range neg", -1, 0, "value out of range: -1"},
		{"max", math.MaxInt16, math.MaxInt16, ""},
		{"min", 0, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := int16ToUint32(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_int16ToUint16(t *testing.T) {
	tests := []struct {
		name    string
		val     int16
		wantVal uint16
		wantErr string
	}{
		{"out of range neg", -1, 0, "value out of range: -1"},
		{"max", math.MaxInt16, math.MaxInt16, ""},
		{"min", 0, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := int16ToUint16(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_int16ToUint8(t *testing.T) {
	tests := []struct {
		name    string
		val     int16
		wantVal uint8
		wantErr string
	}{
		{"out of range neg", -1, 0, "value out of range: -1"},
		{"max", math.MaxInt8, math.MaxInt8, ""},
		{"min", 0, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := int16ToUint8(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_int8ToUint64(t *testing.T) {
	tests := []struct {
		name    string
		val     int8
		wantVal uint64
		wantErr string
	}{
		{"out of range neg", -1, 0, "value out of range: -1"},
		{"max", math.MaxInt8, math.MaxInt8, ""},
		{"min", 0, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := int8ToUint64(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_int8ToUint(t *testing.T) {
	tests := []struct {
		name    string
		val     int8
		wantVal uint
		wantErr string
	}{
		{"out of range neg", -1, 0, "value out of range: -1"},
		{"max", math.MaxInt8, math.MaxInt8, ""},
		{"min", 0, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := int8ToUint(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_int8ToUint32(t *testing.T) {
	tests := []struct {
		name    string
		val     int8
		wantVal uint32
		wantErr string
	}{
		{"out of range neg", -1, 0, "value out of range: -1"},
		{"max", math.MaxInt8, math.MaxInt8, ""},
		{"min", 0, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := int8ToUint32(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_int8ToUint16(t *testing.T) {
	tests := []struct {
		name    string
		val     int8
		wantVal uint16
		wantErr string
	}{
		{"out of range neg", -1, 0, "value out of range: -1"},
		{"max", math.MaxInt8, math.MaxInt8, ""},
		{"min", 0, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := int8ToUint16(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_int8ToUint8(t *testing.T) {
	tests := []struct {
		name    string
		val     int8
		wantVal uint8
		wantErr string
	}{
		{"out of range neg", -1, 0, "value out of range: -1"},
		{"max", math.MaxInt8, math.MaxInt8, ""},
		{"min", 0, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := int8ToUint8(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_uint64ToInt64(t *testing.T) {
	tests := []struct {
		name    string
		val     uint64
		wantVal int64
		wantErr string
	}{
		{"out of range pos", math.MaxInt64 + 1, 0, "value out of range: 9223372036854775808"},
		{"max", math.MaxInt64, math.MaxInt64, ""},
		{"min", 0, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := uint64ToInt64(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_uint64ToInt32(t *testing.T) {
	tests := []struct {
		name    string
		val     uint64
		wantVal int32
		wantErr string
	}{
		{"out of range pos", math.MaxInt32 + 1, 0, "value out of range: 2147483648"},
		{"max", math.MaxInt32, math.MaxInt32, ""},
		{"min", 0, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := uint64ToInt32(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_uint64ToInt16(t *testing.T) {
	tests := []struct {
		name    string
		val     uint64
		wantVal int16
		wantErr string
	}{
		{"out of range pos", math.MaxInt16 + 1, 0, "value out of range: 32768"},
		{"max", math.MaxInt16, math.MaxInt16, ""},
		{"min", 0, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := uint64ToInt16(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_uint64ToInt8(t *testing.T) {
	tests := []struct {
		name    string
		val     uint64
		wantVal int8
		wantErr string
	}{
		{"out of range pos", math.MaxInt8 + 1, 0, "value out of range: 128"},
		{"max", math.MaxInt8, math.MaxInt8, ""},
		{"min", 0, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := uint64ToInt8(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_uintToInt64(t *testing.T) {
	tests := []struct {
		name    string
		val     uint
		wantVal int64
		wantErr string
	}{
		{"out of range pos", math.MaxInt64 + 1, 0, "value out of range: 9223372036854775808"},
		{"max", math.MaxInt64, math.MaxInt64, ""},
		{"min", 0, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := uintToInt64(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_uintToInt32(t *testing.T) {
	tests := []struct {
		name    string
		val     uint
		wantVal int32
		wantErr string
	}{
		{"out of range pos", math.MaxInt32 + 1, 0, "value out of range: 2147483648"},
		{"max", math.MaxInt32, math.MaxInt32, ""},
		{"min", 0, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := uintToInt32(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_uintToInt16(t *testing.T) {
	tests := []struct {
		name    string
		val     uint
		wantVal int16
		wantErr string
	}{
		{"out of range pos", math.MaxInt16 + 1, 0, "value out of range: 32768"},
		{"max", math.MaxInt16, math.MaxInt16, ""},
		{"min", 0, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := uintToInt16(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_uintToInt8(t *testing.T) {
	tests := []struct {
		name    string
		val     uint
		wantVal int8
		wantErr string
	}{
		{"out of range pos", math.MaxInt8 + 1, 0, "value out of range: 128"},
		{"max", math.MaxInt8, math.MaxInt8, ""},
		{"min", 0, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := uintToInt8(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_uint32ToInt32(t *testing.T) {
	tests := []struct {
		name    string
		val     uint32
		wantVal int32
		wantErr string
	}{
		{"out of range pos", math.MaxInt32 + 1, 0, "value out of range: 2147483648"},
		{"max", math.MaxInt32, math.MaxInt32, ""},
		{"min", 0, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := uint32ToInt32(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_uint32ToInt16(t *testing.T) {
	tests := []struct {
		name    string
		val     uint32
		wantVal int16
		wantErr string
	}{
		{"out of range pos", math.MaxInt16 + 1, 0, "value out of range: 32768"},
		{"max", math.MaxInt16, math.MaxInt16, ""},
		{"min", 0, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := uint32ToInt16(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_uint32ToInt8(t *testing.T) {
	tests := []struct {
		name    string
		val     uint32
		wantVal int8
		wantErr string
	}{
		{"out of range pos", math.MaxInt8 + 1, 0, "value out of range: 128"},
		{"max", math.MaxInt8, math.MaxInt8, ""},
		{"min", 0, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := uint32ToInt8(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_uint16ToInt16(t *testing.T) {
	tests := []struct {
		name    string
		val     uint16
		wantVal int16
		wantErr string
	}{
		{"out of range pos", math.MaxInt16 + 1, 0, "value out of range: 32768"},
		{"max", math.MaxInt16, math.MaxInt16, ""},
		{"min", 0, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := uint16ToInt16(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_uint16ToInt8(t *testing.T) {
	tests := []struct {
		name    string
		val     uint16
		wantVal int8
		wantErr string
	}{
		{"out of range pos", math.MaxInt8 + 1, 0, "value out of range: 128"},
		{"max", math.MaxInt8, math.MaxInt8, ""},
		{"min", 0, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := uint16ToInt8(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_uint8ToInt8(t *testing.T) {
	tests := []struct {
		name    string
		val     uint8
		wantVal int8
		wantErr string
	}{
		{"out of range pos", math.MaxInt8 + 1, 0, "value out of range: 128"},
		{"max", math.MaxInt8, math.MaxInt8, ""},
		{"min", 0, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := uint8ToInt8(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_stringToInt64(t *testing.T) {
	tests := []struct {
		name    string
		val     string
		wantVal int64
		wantErr string
	}{
		{"invalid", "invalid", 0, "invalid syntax"},
		{"out of range pos", "9223372036854775808", 0, "value out of range"},
		{"max", "9223372036854775807", math.MaxInt64, ""},
		{"min", "-9223372036854775808", math.MinInt64, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := stringToInt64(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_stringToInt32(t *testing.T) {
	tests := []struct {
		name    string
		val     string
		wantVal int32
		wantErr string
	}{
		{"invalid", "invalid", 0, "invalid syntax"},
		{"out of range pos", "2147483648", 0, "value out of range"},
		{"max", "2147483647", math.MaxInt32, ""},
		{"min", "-2147483648", math.MinInt32, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := stringToInt32(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_stringToInt16(t *testing.T) {
	tests := []struct {
		name    string
		val     string
		wantVal int16
		wantErr string
	}{
		{"invalid", "invalid", 0, "invalid syntax"},
		{"out of range pos", "32768", 0, "value out of range"},
		{"max", "32767", math.MaxInt16, ""},
		{"min", "-32768", math.MinInt16, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := stringToInt16(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_stringToInt8(t *testing.T) {
	tests := []struct {
		name    string
		val     string
		wantVal int8
		wantErr string
	}{
		{"invalid", "invalid", 0, "invalid syntax"},
		{"out of range pos", "128", 0, "value out of range"},
		{"max", "127", math.MaxInt8, ""},
		{"min", "-128", math.MinInt8, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := stringToInt8(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_stringToBigInt(t *testing.T) {
	tests := []struct {
		name    string
		val     string
		wantVal *big.Int
		wantErr string
	}{
		{"invalid", "invalid", nil, "cannot parse 'invalid'"},
		{"pos", "18446744073709551615", new(big.Int).SetUint64(math.MaxUint64), ""},
		{"neg", "-9223372036854775808", new(big.Int).SetInt64(math.MinInt64), ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := stringToBigInt(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_stringToEpochMillis(t *testing.T) {
	tests := []struct {
		name     string
		val      string
		layout   string
		location *time.Location
		wantVal  int64
		wantErr  string
	}{
		{"invalid", "invalid", TimestampLayoutDefault, time.UTC, 0, "cannot parse \"invalid\" as \"2006\""},
		{"layout with TZ", "2021-10-12 00:00:00.999 +01:00", "2006-01-02 15:04:05 -07:00", paris, 1633993200999, ""},
		{"layout without TZ", "2021-10-12 01:00:00.999", "2006-01-02 15:04:05", paris, 1633993200999, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := stringToEpochMillis(tt.val, tt.layout, tt.location)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_stringToNanosOfDay(t *testing.T) {
	tests := []struct {
		name    string
		val     string
		layout  string
		wantVal int64
		wantErr string
	}{
		{"invalid", "invalid", TimeLayoutDefault, 0, "cannot parse \"invalid\" as \"15\""},
		{"ok", "12:34:56.123456789", "15:04:05.999999999", 45296123456789, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := stringToNanosOfDay(tt.val, tt.layout)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_stringToEpochDays(t *testing.T) {
	tests := []struct {
		name    string
		val     string
		layout  string
		wantVal int32
		wantErr string
	}{
		{"invalid", "invalid", DateLayoutDefault, 0, "cannot parse \"invalid\" as \"2006\""},
		{"ok", "2021-10-12", DateLayoutDefault, 18912, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := stringToEpochDays(tt.val, tt.layout)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_bigIntToInt64(t *testing.T) {
	tests := []struct {
		name    string
		val     *big.Int
		wantVal int64
		wantErr string
	}{
		{"not int64", new(big.Int).SetUint64(math.MaxInt64 + 1), 0, "value out of range: 9223372036854775808"},
		{"ok", new(big.Int).SetUint64(math.MaxInt64), math.MaxInt64, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := bigIntToInt64(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_bigIntToInt32(t *testing.T) {
	tests := []struct {
		name    string
		val     *big.Int
		wantVal int32
		wantErr string
	}{
		{"out of range", new(big.Int).SetUint64(math.MaxInt32 + 1), 0, "value out of range: 2147483648"},
		{"ok", new(big.Int).SetUint64(math.MaxInt32), math.MaxInt32, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := bigIntToInt32(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_bigIntToInt(t *testing.T) {
	tests := []struct {
		name    string
		val     *big.Int
		intSize int
		wantVal int64
		wantErr string
	}{
		{"32 bits out of range pos", big.NewInt(math.MaxInt32 + 1), 32, 0, "value out of range: 2147483648"},
		{"32 bits out of range neg", big.NewInt(math.MinInt32 - 1), 32, 0, "value out of range: -2147483649"},
		{"32 bits max", big.NewInt(math.MaxInt32), 32, math.MaxInt32, ""},
		{"32 bits min", big.NewInt(math.MinInt32), 32, math.MinInt32, ""},
	}
	if strconv.IntSize == 64 {
		tests = append(tests, []struct {
			name    string
			val     *big.Int
			intSize int
			wantVal int64
			wantErr string
		}{
			{"64 bits max", big.NewInt(math.MaxInt64), 64, math.MaxInt64, ""},
			{"64 bits min", big.NewInt(math.MinInt64), 64, math.MinInt64, ""},
			{"64 bits out of range pos", new(big.Int).Add(big.NewInt(math.MaxInt64), big.NewInt(1)), 64, 0, "value out of range: 9223372036854775808"},
			{"64 bits out of range neg", new(big.Int).Add(big.NewInt(math.MinInt64), big.NewInt(-1)), 64, 0, "value out of range: -9223372036854775809"},
		}...)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := bigIntToInt(tt.val, tt.intSize)
			assert.EqualValues(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_bigIntToInt16(t *testing.T) {
	tests := []struct {
		name    string
		val     *big.Int
		wantVal int16
		wantErr string
	}{
		{"out of range", new(big.Int).SetUint64(math.MaxInt16 + 1), 0, "value out of range: 32768"},
		{"ok", new(big.Int).SetUint64(math.MaxInt16), math.MaxInt16, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := bigIntToInt16(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_bigIntToInt8(t *testing.T) {
	tests := []struct {
		name    string
		val     *big.Int
		wantVal int8
		wantErr string
	}{
		{"out of range", new(big.Int).SetUint64(math.MaxInt8 + 1), 0, "value out of range: 128"},
		{"ok", new(big.Int).SetUint64(math.MaxInt8), math.MaxInt8, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := bigIntToInt8(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_bigIntToUint64(t *testing.T) {
	tests := []struct {
		name    string
		val     *big.Int
		wantVal uint64
		wantErr string
	}{
		{"out of range", new(big.Int).Add(new(big.Int).SetUint64(math.MaxUint64), big.NewInt(1)), 0, "value out of range: 18446744073709551616"},
		{"ok", new(big.Int).SetUint64(math.MaxUint64), math.MaxUint64, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := bigIntToUint64(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_bigIntToUint32(t *testing.T) {
	tests := []struct {
		name    string
		val     *big.Int
		wantVal uint32
		wantErr string
	}{
		{"out of range", new(big.Int).SetUint64(math.MaxUint32 + 1), 0, "value out of range: 4294967296"},
		{"ok", new(big.Int).SetUint64(math.MaxUint32), math.MaxUint32, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := bigIntToUint32(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_bigIntToUint(t *testing.T) {
	tests := []struct {
		name    string
		val     *big.Int
		intSize int
		wantVal uint64
		wantErr string
	}{
		{"32 bits out of range pos", big.NewInt(math.MaxUint32 + 1), 32, 0, "value out of range: 4294967296"},
		{"32 bits out of range neg", big.NewInt(-1), 32, 0, "value out of range: -1"},
		{"32 bits max", big.NewInt(math.MaxUint32), 32, math.MaxUint32, ""},
		{"32 bits min", big.NewInt(0), 32, 0, ""},
	}
	if strconv.IntSize == 64 {
		tests = append(tests, []struct {
			name    string
			val     *big.Int
			intSize int
			wantVal uint64
			wantErr string
		}{
			{"64 bits max", new(big.Int).SetUint64(math.MaxUint64), 64, math.MaxUint64, ""},
			{"64 bits min", big.NewInt(0), 64, 0, ""},
			{"64 bits out of range pos", new(big.Int).Add(new(big.Int).SetUint64(math.MaxUint64), big.NewInt(1)), 64, 0, "value out of range: 18446744073709551616"},
			{"64 bits out of range neg", big.NewInt(-1), 64, 0, "value out of range: -1"},
		}...)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := bigIntToUint(tt.val, tt.intSize)
			assert.EqualValues(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_bigIntToUint16(t *testing.T) {
	tests := []struct {
		name    string
		val     *big.Int
		wantVal uint16
		wantErr string
	}{
		{"out of range", new(big.Int).SetUint64(math.MaxUint16 + 1), 0, "value out of range: 65536"},
		{"ok", new(big.Int).SetUint64(math.MaxUint16), math.MaxUint16, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := bigIntToUint16(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_bigIntToUint8(t *testing.T) {
	tests := []struct {
		name    string
		val     *big.Int
		wantVal uint8
		wantErr string
	}{
		{"out of range", new(big.Int).SetUint64(math.MaxUint8 + 1), 0, "value out of range: 256"},
		{"ok", new(big.Int).SetUint64(math.MaxUint8), math.MaxUint8, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := bigIntToUint8(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_bigFloatToFloat64(t *testing.T) {
	tests := []struct {
		name    string
		val     *big.Float
		wantVal float64
		wantErr string
	}{
		{"exact", big.NewFloat(123.4), 123.4, ""},
		{"rounded", new(big.Float).Add(big.NewFloat(math.MaxFloat64), big.NewFloat(math.MaxFloat64)), 0, "value out of range: 3.5953862697246314e+308"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := bigFloatToFloat64(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_float64ToBigFloat(t *testing.T) {
	tests := []struct {
		name    string
		val     float64
		destVal *big.Float
		wantVal *big.Float
		wantErr string
	}{
		{"exact", 123.4, new(big.Float), big.NewFloat(123.4), ""},
		{"Nan", math.NaN(), new(big.Float), new(big.Float), "value out of range: NaN"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotErr := float64ToBigFloat(tt.val, tt.destVal)
			assert.Equal(t, tt.wantVal, tt.destVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}

func Test_float64ToFloat32(t *testing.T) {
	tests := []struct {
		name    string
		val     float64
		wantVal float32
		wantErr string
	}{
		{"exact", 123, float32(123), ""},
		{"max", math.MaxFloat64, 0, "value out of range: 1.7976931348623157e+308"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotErr := float64ToFloat32(tt.val)
			assert.Equal(t, tt.wantVal, gotVal)
			assertErrorMessage(t, tt.wantErr, gotErr)
		})
	}
}
