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

package primitive

import "testing"

func TestProtocolVersion_String(t *testing.T) {
	tests := []struct {
		name string
		v    ProtocolVersion
		want string
	}{
		{"v2", ProtocolVersion2, "ProtocolVersion OSS 2"},
		{"v3", ProtocolVersion3, "ProtocolVersion OSS 3"},
		{"v4", ProtocolVersion4, "ProtocolVersion OSS 4"},
		{"v5", ProtocolVersion5, "ProtocolVersion OSS 5"},
		{"DSE v1", ProtocolVersionDse1, "ProtocolVersion DSE 1"},
		{"DSE v2", ProtocolVersionDse2, "ProtocolVersion DSE 2"},
		{"unknown", ProtocolVersion(6), "ProtocolVersion ? [0X06]"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.v.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataTypeCode_IsValid(t *testing.T) {
	tests := []struct {
		name			string
		dtc 			DataTypeCode
		shouldBeValid	bool
	}{
		{"DataTypeCodeCustom", DataTypeCodeCustom, true},
		{"DataTypeCodeAscii", DataTypeCodeAscii, true},
		{"DataTypeCodeBigint", DataTypeCodeBigint, true},
		{"DataTypeCodeBlob", DataTypeCodeBlob, true},
		{"DataTypeCodeBoolean", DataTypeCodeBoolean, true},
		{"DataTypeCodeCounter", DataTypeCodeCounter, true},
		{"DataTypeCodeDecimal", DataTypeCodeDecimal, true},
		{"DataTypeCodeDouble", DataTypeCodeDouble, true},
		{"DataTypeCodeFloat", DataTypeCodeFloat, true},
		{"DataTypeCodeInt", DataTypeCodeInt, true},
		{"DataTypeCodeText", DataTypeCodeText, true},
		{"DataTypeCodeTimestamp", DataTypeCodeTimestamp, true},
		{"DataTypeCodeUuid", DataTypeCodeUuid, true},
		{"DataTypeCodeVarchar", DataTypeCodeVarchar, true},
		{"DataTypeCodeVarint", DataTypeCodeVarint, true},
		{"DataTypeCodeTimeuuid", DataTypeCodeTimeuuid, true},
		{"DataTypeCodeInet", DataTypeCodeInet, true},
		{"DataTypeCodeDate", DataTypeCodeDate, true},
		{"DataTypeCodeTime", DataTypeCodeTime, true},
		{"DataTypeCodeSmallint", DataTypeCodeSmallint, true},
		{"DataTypeCodeTinyint", DataTypeCodeTinyint, true},
		{"DataTypeCodeDuration", DataTypeCodeDuration, true},
		{"DataTypeCodeList", DataTypeCodeList, true},
		{"DataTypeCodeMap", DataTypeCodeMap, true},
		{"DataTypeCodeSet", DataTypeCodeSet, true},
		{"DataTypeCodeUdt", DataTypeCodeUdt, true},
		{"DataTypeCodeTuple", DataTypeCodeTuple, true},
		{"Nonsense", DataTypeCode(0x0023), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if isValid := tt.dtc.IsValid(); isValid != tt.shouldBeValid {
				t.Errorf("IsValid() = %v, shouldBeValid %v", isValid, tt.shouldBeValid)
			}
		})
	}
}