package cassandraprotocol

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReadByte(t *testing.T) {
	tests := []struct {
		name      string
		source    []byte
		expected  byte
		remaining []byte
		err       error
	}{
		{"simple byte", []byte{5}, byte(5), []byte{}, nil},
		{"zero byte", []byte{0}, byte(0), []byte{}, nil},
		{"byte with remaining", []byte{5, 1, 2, 3, 4}, byte(5), []byte{1, 2, 3, 4}, nil},
		{"cannot read byte", []byte{}, byte(0), []byte{}, errors.New("not enough bytes to read a protocol [byte]")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, remaining, err := ReadByte(tt.source)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.remaining, remaining)
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestWriteByte(t *testing.T) {
	tests := []struct {
		name      string
		input     byte
		dest      []byte
		expected  []byte
		remaining []byte
		err       error
	}{
		{"simple byte", byte(5), make([]byte, 1), []byte{5}, []byte{}, nil},
		{"byte with remaining", byte(5), make([]byte, 4), []byte{5, 0, 0, 0}, []byte{0, 0, 0}, nil},
		{"cannot write byte", byte(5), []byte{}, []byte{}, []byte{}, errors.New("not enough capacity to write a protocol [byte]")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			remaining, err := WriteByte(tt.input, tt.dest)
			assert.Equal(t, tt.expected, tt.dest)
			assert.Equal(t, tt.remaining, remaining)
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestReadShort(t *testing.T) {
	tests := []struct {
		name      string
		source    []byte
		expected  uint16
		remaining []byte
		err       error
	}{
		{"simple short", []byte{0, 5}, uint16(5), []byte{}, nil},
		{"zero short", []byte{0, 0}, uint16(0), []byte{}, nil},
		{"short with remaining", []byte{0, 5, 1, 2, 3, 4}, uint16(5), []byte{1, 2, 3, 4}, nil},
		{"cannot read short", []byte{0}, uint16(0), []byte{0}, errors.New("not enough bytes to read a protocol [short]")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, remaining, err := ReadShort(tt.source)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.remaining, remaining)
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestWriteShort(t *testing.T) {
	tests := []struct {
		name      string
		input     uint16
		dest      []byte
		expected  []byte
		remaining []byte
		err       error
	}{
		{"simple short", uint16(5), make([]byte, SizeOfShort), []byte{0, 5}, []byte{}, nil},
		{"short with remaining", uint16(5), make([]byte, SizeOfShort+1), []byte{0, 5, 0}, []byte{0}, nil},
		{"cannot write short", uint16(5), make([]byte, SizeOfShort-1), []byte{0}, []byte{0}, errors.New("not enough capacity to write a protocol [short]")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			remaining, err := WriteShort(tt.input, tt.dest)
			assert.Equal(t, tt.expected, tt.dest)
			assert.Equal(t, tt.remaining, remaining)
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestReadInt(t *testing.T) {
	tests := []struct {
		name      string
		source    []byte
		expected  int32
		remaining []byte
		err       error
	}{
		{"simple int", []byte{0, 0, 0, 5}, int32(5), []byte{}, nil},
		{"zero int", []byte{0, 0, 0, 0}, int32(0), []byte{}, nil},
		{"negative int", []byte{0xff, 0xff, 0xff, 0xff & -5}, int32(-5), []byte{}, nil},
		{"int with remaining", []byte{0, 0, 0, 5, 1, 2, 3, 4}, int32(5), []byte{1, 2, 3, 4}, nil},
		{"cannot read int", []byte{0, 0, 0}, int32(0), []byte{0, 0, 0}, errors.New("not enough bytes to read a protocol [int]")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, remaining, err := ReadInt(tt.source)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.remaining, remaining)
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestWriteInt(t *testing.T) {
	tests := []struct {
		name      string
		input     int32
		dest      []byte
		expected  []byte
		remaining []byte
		err       error
	}{
		{"simple int", int32(5), make([]byte, SizeOfInt), []byte{0, 0, 0, 5}, []byte{}, nil},
		{"negative int", int32(-5), make([]byte, SizeOfInt), []byte{0xff, 0xff, 0xff, 0xff & -5}, []byte{}, nil},
		{"int with remaining", int32(5), make([]byte, SizeOfInt+1), []byte{0, 0, 0, 5, 0}, []byte{0}, nil},
		{"cannot write int", int32(5), make([]byte, SizeOfInt-1), []byte{0, 0, 0}, []byte{0, 0, 0}, errors.New("not enough capacity to write a protocol [int]")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			remaining, err := WriteInt(tt.input, tt.dest)
			assert.Equal(t, tt.expected, tt.dest)
			assert.Equal(t, tt.remaining, remaining)
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestReadLong(t *testing.T) {
	tests := []struct {
		name      string
		source    []byte
		expected  int64
		remaining []byte
		err       error
	}{
		{"simple long", []byte{0, 0, 0, 0, 0, 0, 0, 5}, int64(5), []byte{}, nil},
		{"zero long", []byte{0, 0, 0, 0, 0, 0, 0, 0}, int64(0), []byte{}, nil},
		{"negative long", []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff & -5}, int64(-5), []byte{}, nil},
		{"long with remaining", []byte{0, 0, 0, 0, 0, 0, 0, 5, 1, 2, 3, 4}, int64(5), []byte{1, 2, 3, 4}, nil},
		{"cannot read long", []byte{0, 0, 0, 0, 0, 0, 0}, int64(0), []byte{0, 0, 0, 0, 0, 0, 0}, errors.New("not enough bytes to read a protocol [long]")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, remaining, err := ReadLong(tt.source)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.remaining, remaining)
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestWriteLong(t *testing.T) {
	tests := []struct {
		name      string
		input     int64
		dest      []byte
		expected  []byte
		remaining []byte
		err       error
	}{
		{"simple long", int64(5), make([]byte, SizeOfLong), []byte{0, 0, 0, 0, 0, 0, 0, 5}, []byte{}, nil},
		{"negative long", int64(-5), make([]byte, SizeOfLong), []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff & -5}, []byte{}, nil},
		{"long with remaining", int64(5), make([]byte, SizeOfLong+1), []byte{0, 0, 0, 0, 0, 0, 0, 5, 0}, []byte{0}, nil},
		{"cannot write long", int64(5), make([]byte, SizeOfLong-1), []byte{0, 0, 0, 0, 0, 0, 0}, []byte{0, 0, 0, 0, 0, 0, 0}, errors.New("not enough capacity to write a protocol [long]")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			remaining, err := WriteLong(tt.input, tt.dest)
			assert.Equal(t, tt.expected, tt.dest)
			assert.Equal(t, tt.remaining, remaining)
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestReadString(t *testing.T) {
	tests := []struct {
		name      string
		source    []byte
		expected  string
		remaining []byte
		err       error
	}{
		{"simple string", []byte{0, 5, 0x68, 0x65, 0x6c, 0x6c, 0x6f}, "hello", []byte{}, nil},
		{"string with remaining", []byte{0, 5, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 1, 2, 3, 4}, "hello", []byte{1, 2, 3, 4}, nil},
		{"empty string", []byte{0, 0}, "", []byte{}, nil},
		{"non-ASCII string", []byte{
			0, 15, // length
			0xce, 0xb3, 0xce, 0xb5, 0xce, 0xb9, 0xce, 0xac, //γειά
			0x20,                               // space
			0xcf, 0x83, 0xce, 0xbf, 0xcf, 0x85, // σου
		}, "γειά σου", []byte{}, nil},
		{"cannot read length", []byte{0}, "", []byte{0}, errors.New("not enough bytes to read a protocol [short]")},
		{"cannot read string", []byte{0, 5, 0x68, 0x65, 0x6c, 0x6c}, "", []byte{0x68, 0x65, 0x6c, 0x6c}, errors.New("not enough bytes to read a protocol [string]")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, remaining, err := ReadString(tt.source)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.remaining, remaining)
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestWriteString(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		dest      []byte
		expected  []byte
		remaining []byte
		err       error
	}{
		{
			"simple string",
			"hello",
			make([]byte, SizeOfString("hello")),
			[]byte{0, 5, 0x68, 0x65, 0x6c, 0x6c, 0x6f},
			[]byte{},
			nil,
		},
		{"empty string", "", make([]byte, SizeOfString("")), []byte{0, 0}, []byte{}, nil},
		{"non-ASCII string", "γειά σου", make([]byte, SizeOfString("γειά σου")), []byte{
			0, 15, // length
			0xce, 0xb3, 0xce, 0xb5, 0xce, 0xb9, 0xce, 0xac, //γειά
			0x20,                               // space
			0xcf, 0x83, 0xce, 0xbf, 0xcf, 0x85, // σου
		}, []byte{}, nil},
		{
			"string with remaining",
			"hello",
			make([]byte, SizeOfString("hello")+1),
			[]byte{0, 5, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0},
			[]byte{0},
			nil,
		},
		{
			"cannot write string size",
			"hello",
			make([]byte, SizeOfShort-1),
			[]byte{0},
			[]byte{0},
			errors.New("not enough capacity to write a protocol [short]"),
		},
		{
			"cannot write string",
			"hello",
			make([]byte, SizeOfString("hello")-1),
			[]byte{0, 5, 0, 0, 0, 0},
			[]byte{0, 0, 0, 0},
			errors.New("not enough capacity to write a protocol [string]"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			remaining, err := WriteString(tt.input, tt.dest)
			assert.Equal(t, tt.expected, tt.dest)
			assert.Equal(t, tt.remaining, remaining)
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestReadLongString(t *testing.T) {
	tests := []struct {
		name      string
		source    []byte
		expected  string
		remaining []byte
		err       error
	}{
		{"simple string", []byte{0, 0, 0, 5, 0x68, 0x65, 0x6c, 0x6c, 0x6f}, "hello", []byte{}, nil},
		{"string with remaining", []byte{0, 0, 0, 5, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 1, 2, 3, 4}, "hello", []byte{1, 2, 3, 4}, nil},
		{"empty string", []byte{0, 0, 0, 0}, "", []byte{}, nil},
		{"non-ASCII string", []byte{
			0, 0, 0, 15, // length
			0xce, 0xb3, 0xce, 0xb5, 0xce, 0xb9, 0xce, 0xac, //γειά
			0x20,                               // space
			0xcf, 0x83, 0xce, 0xbf, 0xcf, 0x85, // σου
		}, "γειά σου", []byte{}, nil},
		{
			"cannot read length",
			[]byte{0, 0, 0},
			"",
			[]byte{0, 0, 0},
			errors.New("not enough bytes to read a protocol [int]"),
		},
		{
			"cannot read string",
			[]byte{0, 0, 0, 5, 0x68, 0x65, 0x6c, 0x6c},
			"",
			[]byte{0x68, 0x65, 0x6c, 0x6c},
			errors.New("not enough bytes to read a protocol [long string]"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, remaining, err := ReadLongString(tt.source)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.remaining, remaining)
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestWriteLongString(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		dest      []byte
		expected  []byte
		remaining []byte
		err       error
	}{
		{"simple string", "hello", make([]byte, SizeOfLongString("hello")), []byte{0, 0, 0, 5, 0x68, 0x65, 0x6c, 0x6c, 0x6f}, []byte{}, nil},
		{"empty string", "", make([]byte, SizeOfLongString("")), []byte{0, 0, 0, 0}, []byte{}, nil},
		{"non-ASCII string", "γειά σου", make([]byte, SizeOfLongString("γειά σου")), []byte{
			0, 0, 0, 15, // length
			0xce, 0xb3, 0xce, 0xb5, 0xce, 0xb9, 0xce, 0xac, //γειά
			0x20,                               // space
			0xcf, 0x83, 0xce, 0xbf, 0xcf, 0x85, // σου
		}, []byte{}, nil},
		{
			"string with remaining",
			"hello",
			make([]byte, SizeOfLongString("hello")+1),
			[]byte{0, 0, 0, 5, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0},
			[]byte{0},
			nil,
		},
		{
			"cannot write string size",
			"hello",
			make([]byte, SizeOfInt-1),
			[]byte{0, 0, 0},
			[]byte{0, 0, 0},
			errors.New("not enough capacity to write a protocol [int]"),
		},
		{
			"cannot write string",
			"hello",
			make([]byte, SizeOfLongString("hello")-1),
			[]byte{0, 0, 0, 5, 0, 0, 0, 0},
			[]byte{0, 0, 0, 0},
			errors.New("not enough capacity to write a protocol [long string]"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			remaining, err := WriteLongString(tt.input, tt.dest)
			assert.Equal(t, tt.expected, tt.dest)
			assert.Equal(t, tt.remaining, remaining)
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestReadStringList(t *testing.T) {
	tests := []struct {
		name      string
		source    []byte
		expected  []string
		remaining []byte
		err       error
	}{
		{"empty string list", []byte{0, 0}, []string{}, []byte{}, nil},
		{"singleton string list", []byte{
			0, 1, // length
			0, 5, 0x68, 0x65, 0x6c, 0x6c, 0x6f, // hello
		}, []string{"hello"}, []byte{}, nil},
		{"simple string list", []byte{
			0, 2, // length
			0, 5, 0x68, 0x65, 0x6c, 0x6c, 0x6f, // hello
			0, 5, 0x77, 0x6f, 0x72, 0x6c, 0x64, // world
		}, []string{"hello", "world"}, []byte{}, nil},
		{"empty elements", []byte{
			0, 2, // length
			0, 0, // elt 1
			0, 0, // elt 2
		}, []string{"", ""}, []byte{}, nil},
		{
			"cannot read length",
			[]byte{0},
			nil,
			[]byte{0},
			errors.New("not enough bytes to read a protocol [short]"),
		},
		{
			"cannot read list",
			[]byte{0, 1, 0, 5, 0x68, 0x65, 0x6c, 0x6c},
			nil,
			[]byte{0x68, 0x65, 0x6c, 0x6c},
			errors.New("not enough bytes to read a protocol [string]"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, remaining, err := ReadStringList(tt.source)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.remaining, remaining)
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestWriteStringList(t *testing.T) {
	tests := []struct {
		name      string
		input     []string
		dest      []byte
		expected  []byte
		remaining []byte
		err       error
	}{
		{
			"empty string list",
			[]string{},
			make([]byte, SizeOfStringList([]string{})),
			[]byte{0, 0},
			[]byte{},
			nil,
		},
		{
			"singleton string list",
			[]string{"hello"},
			make([]byte, SizeOfStringList([]string{"hello"})),
			[]byte{
				0, 1, // length
				0, 5, 0x68, 0x65, 0x6c, 0x6c, 0x6f, // hello
			},
			[]byte{},
			nil,
		},
		{
			"simple string list",
			[]string{"hello", "world"},
			make([]byte, SizeOfStringList([]string{"hello", "world"})),
			[]byte{
				0, 2, // length
				0, 5, 0x68, 0x65, 0x6c, 0x6c, 0x6f, // hello
				0, 5, 0x77, 0x6f, 0x72, 0x6c, 0x64, // world
			},
			[]byte{},
			nil,
		},
		{
			"empty elements",
			[]string{"", ""},
			make([]byte, SizeOfStringList([]string{"", ""})),
			[]byte{
				0, 2, // length
				0, 0, // elt 1
				0, 0, // elt 2
			},
			[]byte{},
			nil,
		},
		{
			"cannot write list size",
			[]string{"hello"},
			make([]byte, SizeOfShort-1),
			[]byte{0},
			[]byte{0},
			errors.New("not enough capacity to write a protocol [short]"),
		},
		{
			"cannot write list element",
			[]string{"hello"},
			make([]byte, SizeOfStringList([]string{"hello"})-1),
			[]byte{0, 1, 0, 5, 0, 0, 0, 0},
			[]byte{0, 0, 0, 0},
			errors.New("not enough capacity to write a protocol [string]"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			remaining, err := WriteStringList(tt.input, tt.dest)
			assert.Equal(t, tt.expected, tt.dest)
			assert.Equal(t, tt.remaining, remaining)
			assert.Equal(t, tt.err, err)
		})
	}
}
