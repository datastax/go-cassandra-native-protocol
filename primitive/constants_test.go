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
		{"v5", ProtocolVersion5, "ProtocolVersion OSS 5 (beta)"},
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
