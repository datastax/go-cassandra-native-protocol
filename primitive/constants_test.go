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
