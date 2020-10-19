package primitives

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"go-cassandra-native-protocol/cassandraprotocol"
	"net"
	"testing"
)

func TestReadByte(t *testing.T) {
	tests := []struct {
		name     string
		source   []byte
		expected byte
		err      error
	}{
		{"simple byte", []byte{5}, byte(5), nil},
		{"zero byte", []byte{0}, byte(0), nil},
		{"byte with remaining", []byte{5, 1, 2, 3, 4}, byte(5), nil},
		{"cannot read byte", []byte{}, byte(0), fmt.Errorf("cannot read [byte]: %w", errors.New("EOF"))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(tt.source)
			actual, err := ReadByte(buf)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestWriteByte(t *testing.T) {
	tests := []struct {
		name     string
		input    byte
		expected []byte
		err      error
	}{
		{"simple byte", byte(5), []byte{5}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := WriteByte(tt.input, buf)
			assert.Equal(t, tt.expected, buf.Bytes())
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
		{"cannot read short", []byte{0}, uint16(0), []byte{}, fmt.Errorf("cannot read [short]: %w", errors.New("unexpected EOF"))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(tt.source)
			actual, err := ReadShort(buf)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.remaining, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestWriteShort(t *testing.T) {
	tests := []struct {
		name     string
		input    uint16
		expected []byte
		err      error
	}{
		{"simple short", uint16(5), []byte{0, 5}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := WriteShort(tt.input, buf)
			assert.Equal(t, tt.expected, buf.Bytes())
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
		{"cannot read int", []byte{0, 0, 0}, int32(0), []byte{}, fmt.Errorf("cannot read [int]: %w", errors.New("unexpected EOF"))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(tt.source)
			actual, err := ReadInt(buf)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.remaining, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestWriteInt(t *testing.T) {
	tests := []struct {
		name     string
		input    int32
		expected []byte
		err      error
	}{
		{"simple int", int32(5), []byte{0, 0, 0, 5}, nil},
		{"negative int", int32(-5), []byte{0xff, 0xff, 0xff, 0xff & -5}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := WriteInt(tt.input, buf)
			assert.Equal(t, tt.expected, buf.Bytes())
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
		{"cannot read long", []byte{0, 0, 0, 0, 0, 0, 0}, int64(0), []byte{}, fmt.Errorf("cannot read [long]: %w", errors.New("unexpected EOF"))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(tt.source)
			actual, err := ReadLong(buf)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.remaining, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestWriteLong(t *testing.T) {
	tests := []struct {
		name     string
		input    int64
		expected []byte
		err      error
	}{
		{"simple long", int64(5), []byte{0, 0, 0, 0, 0, 0, 0, 5}, nil},
		{"negative long", int64(-5), []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff & -5}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := WriteLong(tt.input, buf)
			assert.Equal(t, tt.expected, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

const (
	d = byte('d')
	e = byte('e')
	h = byte('h')
	k = byte('k')
	l = byte('l')
	m = byte('m')
	n = byte('n')
	o = byte('o')
	r = byte('r')
	u = byte('u')
	w = byte('w')
)

func TestReadString(t *testing.T) {
	tests := []struct {
		name      string
		source    []byte
		expected  string
		remaining []byte
		err       error
	}{
		{"simple string", []byte{0, 5, h, e, l, l, o}, "hello", []byte{}, nil},
		{"string with remaining", []byte{0, 5, h, e, l, l, o, 1, 2, 3, 4}, "hello", []byte{1, 2, 3, 4}, nil},
		{"empty string", []byte{0, 0}, "", []byte{}, nil},
		{"non-ASCII string", []byte{
			0, 15, // length
			0xce, 0xb3, 0xce, 0xb5, 0xce, 0xb9, 0xce, 0xac, //γειά
			0x20,                               // space
			0xcf, 0x83, 0xce, 0xbf, 0xcf, 0x85, // σου
		}, "γειά σου", []byte{}, nil},
		{
			"cannot read length",
			[]byte{0},
			"",
			[]byte{},
			fmt.Errorf("cannot read [string] length: %w", fmt.Errorf("cannot read [short]: %w", errors.New("unexpected EOF"))),
		},
		{
			"cannot read string",
			[]byte{0, 5, h, e, l, l},
			"",
			[]byte{},
			errors.New("not enough bytes to read [string] content"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(tt.source)
			actual, err := ReadString(buf)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.remaining, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestWriteString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []byte
		err      error
	}{
		{
			"simple string",
			"hello",
			[]byte{0, 5, h, e, l, l, o},
			nil,
		},
		{"empty string", "", []byte{0, 0}, nil},
		{"non-ASCII string", "γειά σου", []byte{
			0, 15, // length
			0xce, 0xb3, 0xce, 0xb5, 0xce, 0xb9, 0xce, 0xac, //γειά
			0x20,                               // space
			0xcf, 0x83, 0xce, 0xbf, 0xcf, 0x85, // σου
		}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := WriteString(tt.input, buf)
			assert.Equal(t, tt.expected, buf.Bytes())
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
		{"simple string", []byte{0, 0, 0, 5, h, e, l, l, o}, "hello", []byte{}, nil},
		{"string with remaining", []byte{0, 0, 0, 5, h, e, l, l, o, 1, 2, 3, 4}, "hello", []byte{1, 2, 3, 4}, nil},
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
			[]byte{},
			fmt.Errorf("cannot read [long string] length: %w", fmt.Errorf("cannot read [int]: %w", errors.New("unexpected EOF"))),
		},
		{
			"cannot read string",
			[]byte{0, 0, 0, 5, h, e, l, l},
			"",
			[]byte{},
			errors.New("not enough bytes to read [long string] content"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(tt.source)
			actual, err := ReadLongString(buf)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.remaining, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestWriteLongString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []byte
		err      error
	}{
		{
			"simple string",
			"hello",
			[]byte{0, 0, 0, 5, h, e, l, l, o},
			nil,
		},
		{
			"empty string",
			"",
			[]byte{0, 0, 0, 0},
			nil,
		},
		{
			"non-ASCII string",
			"γειά σου",
			[]byte{
				0, 0, 0, 15, // length
				0xce, 0xb3, 0xce, 0xb5, 0xce, 0xb9, 0xce, 0xac, //γειά
				0x20,                               // space
				0xcf, 0x83, 0xce, 0xbf, 0xcf, 0x85, // σου
			},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := WriteLongString(tt.input, buf)
			assert.Equal(t, tt.expected, buf.Bytes())
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
			0, 5, h, e, l, l, o, // hello
		}, []string{"hello"}, []byte{}, nil},
		{"simple string list", []byte{
			0, 2, // length
			0, 5, h, e, l, l, o, // hello
			0, 5, w, o, r, l, d, // world
		}, []string{"hello", "world"}, []byte{}, nil},
		{"empty elements", []byte{
			0, 2, // length
			0, 0, // elt 1
			0, 0, // elt 2
		}, []string{"", ""}, []byte{}, nil},
		{
			"cannot read list length",
			[]byte{0},
			nil,
			[]byte{},
			fmt.Errorf("cannot read [string list] length: %w", fmt.Errorf("cannot read [short]: %w", errors.New("unexpected EOF"))),
		},
		{
			"cannot read list element",
			[]byte{0, 1, 0, 5, h, e, l, l},
			nil,
			[]byte{},
			fmt.Errorf("cannot read [string list] element: %w", errors.New("not enough bytes to read [string] content")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(tt.source)
			actual, err := ReadStringList(buf)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.remaining, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestWriteStringList(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []byte
		err      error
	}{
		{
			"empty string list",
			[]string{},
			[]byte{0, 0},
			nil,
		},
		{
			"nil string list",
			nil,
			[]byte{0, 0},
			nil,
		},
		{
			"singleton string list",
			[]string{"hello"},
			[]byte{
				0, 1, // length
				0, 5, h, e, l, l, o, // hello
			},
			nil,
		},
		{
			"simple string list",
			[]string{"hello", "world"},
			[]byte{
				0, 2, // length
				0, 5, h, e, l, l, o, // hello
				0, 5, w, o, r, l, d, // world
			},
			nil,
		},
		{
			"empty elements",
			[]string{"", ""},
			[]byte{
				0, 2, // length
				0, 0, // elt 1
				0, 0, // elt 2
			},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := WriteStringList(tt.input, buf)
			assert.Equal(t, tt.expected, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestReadBytes(t *testing.T) {
	tests := []struct {
		name      string
		source    []byte
		expected  []byte
		remaining []byte
		err       error
	}{
		{"empty bytes", []byte{0, 0, 0, 0}, []byte{}, []byte{}, nil},
		{"nil bytes", []byte{0xff, 0xff, 0xff, 0xff}, nil, []byte{}, nil},
		{"singleton bytes", []byte{0, 0, 0, 1, 1}, []byte{1}, []byte{}, nil},
		{"simple bytes", []byte{0, 0, 0, 2, 1, 2}, []byte{1, 2}, []byte{}, nil},
		{
			"cannot read bytes length",
			[]byte{0, 0, 0},
			nil,
			[]byte{},
			fmt.Errorf("cannot read [bytes] length: %w", fmt.Errorf("cannot read [int]: %w", errors.New("unexpected EOF"))),
		},
		{
			"cannot read bytes content",
			[]byte{0, 0, 0, 2, 1},
			nil,
			[]byte{},
			fmt.Errorf("not enough bytes to read [bytes] content"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(tt.source)
			actual, err := ReadBytes(buf)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.remaining, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestWriteBytes(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected []byte
		err      error
	}{
		{
			"empty bytes",
			[]byte{},
			[]byte{0, 0, 0, 0},
			nil,
		},
		{
			"nil bytes",
			nil,
			[]byte{0xff, 0xff, 0xff, 0xff},
			nil,
		},
		{
			"singleton bytes",
			[]byte{1},
			[]byte{0, 0, 0, 1, 1},
			nil,
		},
		{
			"simple bytes",
			[]byte{1, 2},
			[]byte{0, 0, 0, 2, 1, 2},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := WriteBytes(tt.input, buf)
			assert.Equal(t, tt.expected, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestReadShortBytes(t *testing.T) {
	tests := []struct {
		name      string
		source    []byte
		expected  []byte
		remaining []byte
		err       error
	}{
		{"empty short bytes", []byte{0, 0}, []byte{}, []byte{}, nil},
		{"singleton short bytes", []byte{0, 1, 1}, []byte{1}, []byte{}, nil},
		{"simple short bytes", []byte{0, 2, 1, 2}, []byte{1, 2}, []byte{}, nil},
		{
			"cannot read short bytes length",
			[]byte{0},
			nil,
			[]byte{},
			fmt.Errorf("cannot read [short bytes] length: %w", fmt.Errorf("cannot read [short]: %w", errors.New("unexpected EOF"))),
		},
		{
			"cannot read short bytes content",
			[]byte{0, 2, 1},
			nil,
			[]byte{},
			fmt.Errorf("not enough bytes to read [short bytes] content"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(tt.source)
			actual, err := ReadShortBytes(buf)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.remaining, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestWriteShortBytes(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected []byte
		err      error
	}{
		{
			"empty short bytes",
			[]byte{},
			[]byte{0, 0},
			nil,
		},
		// not officially allowed by the specs, but better safe than sorry
		{
			"nil short bytes",
			nil,
			[]byte{0, 0},
			nil,
		},
		{
			"singleton short bytes",
			[]byte{1},
			[]byte{0, 1, 1},
			nil,
		},
		{
			"simple short bytes",
			[]byte{1, 2},
			[]byte{0, 2, 1, 2},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := WriteShortBytes(tt.input, buf)
			assert.Equal(t, tt.expected, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

var uuid = cassandraprotocol.UUID{0xC0, 0xD1, 0xD2, 0x1E, 0xBB, 0x01, 0x41, 0x96, 0x86, 0xDB, 0xBC, 0x31, 0x7B, 0xC1, 0x79, 0x6A}
var uuidBytes = [16]byte{0xC0, 0xD1, 0xD2, 0x1E, 0xBB, 0x01, 0x41, 0x96, 0x86, 0xDB, 0xBC, 0x31, 0x7B, 0xC1, 0x79, 0x6A}

func TestReadUuid(t *testing.T) {
	tests := []struct {
		name      string
		source    []byte
		expected  *cassandraprotocol.UUID
		remaining []byte
		err       error
	}{
		{"simple UUID", uuidBytes[:], &uuid, []byte{}, nil},
		{"UUID with remaining", append(uuidBytes[:], 1, 2, 3, 4), &uuid, []byte{1, 2, 3, 4}, nil},
		{
			"cannot read UUID",
			uuidBytes[:15],
			nil,
			[]byte{},
			errors.New("not enough bytes to read [uuid] content"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(tt.source)
			actual, err := ReadUuid(buf)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.remaining, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestWriteUuid(t *testing.T) {
	tests := []struct {
		name     string
		input    *cassandraprotocol.UUID
		expected []byte
		err      error
	}{
		{
			"simple UUID",
			&uuid,
			uuidBytes[:],
			nil,
		},
		{
			"UUID with remaining",
			&uuid,
			uuidBytes[:],
			nil,
		},
		{
			"nil UUID",
			nil,
			nil,
			errors.New("cannot write nil [uuid]"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := WriteUuid(tt.input, buf)
			assert.Equal(t, tt.expected, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

var inetAddr4 = net.IPv4(192, 168, 1, 1)

var inet4 = cassandraprotocol.Inet{
	Addr: inetAddr4,
	Port: 9042,
}

var inetAddr4Bytes = []byte{
	4,              // length of IP
	192, 168, 1, 1, // IP
}

var inet4Bytes = append(inetAddr4Bytes,
	0, 0, 0x23, 0x52, //port
)

// 2001:0db8:85a3:0000:0000:8a2e:0370:7334
var inetAddr6 = net.IP{0x20, 0x01, 0x0d, 0xb8, 0x85, 0xa3, 0x00, 0x00, 0x00, 0x00, 0x8a, 0x2e, 0x03, 0x70, 0x73, 0x34}

var inet6 = cassandraprotocol.Inet{
	Addr: inetAddr6,
	Port: 9042,
}

var inetAddr6Bytes = []byte{
	16,                                                                                             // length of IP
	0x20, 0x01, 0x0d, 0xb8, 0x85, 0xa3, 0x00, 0x00, 0x00, 0x00, 0x8a, 0x2e, 0x03, 0x70, 0x73, 0x34, // IP
}

var inet6Bytes = append(inetAddr6Bytes,
	0, 0, 0x23, 0x52, //port
)

func TestReadInetAddr(t *testing.T) {
	tests := []struct {
		name      string
		source    []byte
		expected  net.IP
		remaining []byte
		err       error
	}{
		{"IPv4 InetAddr", inetAddr4Bytes[:], inetAddr4, []byte{}, nil},
		{"IPv6 InetAddr", inetAddr6Bytes[:], inetAddr6, []byte{}, nil},
		{"InetAddr with remaining", append(inetAddr4Bytes[:], 1, 2, 3, 4), inetAddr4, []byte{1, 2, 3, 4}, nil},
		{
			"cannot read InetAddr length",
			[]byte{},
			nil,
			[]byte{},
			fmt.Errorf("cannot read [inetaddr] length: %w", fmt.Errorf("cannot read [byte]: %w", errors.New("EOF"))),
		},
		{
			"not enough bytes to read [inetaddr] IPv4 content",
			[]byte{4, 192, 168, 1},
			nil,
			[]byte{},
			errors.New("not enough bytes to read [inetaddr] IPv4 content"),
		},
		{
			"not enough bytes to read [inetaddr] IPv6 content",
			[]byte{16, 0x20, 0x01, 0x0d, 0xb8, 0x85, 0xa3, 0x00, 0x00, 0x00, 0x00, 0x8a, 0x2e, 0x03, 0x70, 0x73},
			nil,
			[]byte{},
			errors.New("not enough bytes to read [inetaddr] IPv6 content"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(tt.source)
			actual, err := ReadInetAddr(buf)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.remaining, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestWriteInetAddr(t *testing.T) {
	tests := []struct {
		name     string
		input    net.IP
		expected []byte
		err      error
	}{
		{
			"IPv4 InetAddr",
			inetAddr4,
			inetAddr4Bytes,
			nil,
		},
		{
			"IPv6 InetAddr",
			inetAddr6,
			inetAddr6Bytes,
			nil,
		},
		{
			"cannot write nil InetAddr",
			nil,
			nil,
			errors.New("cannot write nil [inetaddr]"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := WriteInetAddr(tt.input, buf)
			assert.Equal(t, tt.expected, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestLengthOfInetAddr(t *testing.T) {
	tests := []struct {
		name     string
		input    net.IP
		expected int
		err      error
	}{
		{
			"IPv4 InetAddr",
			inetAddr4,
			LengthOfByte + net.IPv4len,
			nil,
		},
		{
			"IPv6 InetAddr",
			inetAddr6,
			LengthOfByte + net.IPv6len,
			nil,
		},
		{
			"nil InetAddr",
			nil,
			-1,
			errors.New("cannot compute nil [inetaddr] length"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := LengthOfInetAddr(tt.input)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestReadInet(t *testing.T) {
	tests := []struct {
		name      string
		source    []byte
		expected  *cassandraprotocol.Inet
		remaining []byte
		err       error
	}{
		{"IPv4 INET", inet4Bytes[:], &inet4, []byte{}, nil},
		{"IPv6 INET", inet6Bytes[:], &inet6, []byte{}, nil},
		{"INET with remaining", append(inet4Bytes[:], 1, 2, 3, 4), &inet4, []byte{1, 2, 3, 4}, nil},
		{
			"cannot read INET length",
			[]byte{},
			nil,
			[]byte{},
			fmt.Errorf("cannot read [inet] address: %w", fmt.Errorf("cannot read [inetaddr] length: %w", fmt.Errorf("cannot read [byte]: %w", errors.New("EOF")))),
		},
		{
			"not enough bytes to read [inet] IPv4 content",
			[]byte{4, 192, 168, 1},
			nil,
			[]byte{},
			fmt.Errorf("cannot read [inet] address: %w", errors.New("not enough bytes to read [inetaddr] IPv4 content")),
		},
		{
			"not enough bytes to read [inet] IPv6 content",
			[]byte{16, 0x20, 0x01, 0x0d, 0xb8, 0x85, 0xa3, 0x00, 0x00, 0x00, 0x00, 0x8a, 0x2e, 0x03, 0x70, 0x73},
			nil,
			[]byte{},
			fmt.Errorf("cannot read [inet] address: %w", errors.New("not enough bytes to read [inetaddr] IPv6 content")),
		},
		{
			"cannot read [inet] port number",
			[]byte{4, 192, 168, 1, 1, 0, 0, 0},
			nil,
			[]byte{},
			fmt.Errorf("cannot read [inet] port number: %w", fmt.Errorf("cannot read [int]: %w", errors.New("unexpected EOF"))),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(tt.source)
			actual, err := ReadInet(buf)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.remaining, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestWriteInet(t *testing.T) {
	tests := []struct {
		name     string
		input    *cassandraprotocol.Inet
		expected []byte
		err      error
	}{
		{
			"IPv4 INET",
			&inet4,
			inet4Bytes,
			nil,
		},
		{
			"IPv6 INET",
			&inet6,
			inet6Bytes,
			nil,
		},
		{
			"INET with remaining",
			&inet4,
			inet4Bytes,
			nil,
		},
		{
			"cannot write nil INET",
			nil,
			nil,
			errors.New("cannot write nil [inet]"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := WriteInet(tt.input, buf)
			assert.Equal(t, tt.expected, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestLengthOfInet(t *testing.T) {
	tests := []struct {
		name     string
		input    *cassandraprotocol.Inet
		expected int
		err      error
	}{
		{
			"IPv4 INET",
			&inet4,
			LengthOfByte + net.IPv4len + LengthOfInt,
			nil,
		},
		{
			"IPv6 INET",
			&inet6,
			LengthOfByte + net.IPv6len + LengthOfInt,
			nil,
		},
		{
			"nil INET",
			nil,
			-1,
			errors.New("cannot compute nil [inet] length"),
		},
		{
			"nil INET addr",
			&cassandraprotocol.Inet{},
			-1,
			errors.New("cannot compute nil [inetaddr] length"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := LengthOfInet(tt.input)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestReadStringMap(t *testing.T) {
	tests := []struct {
		name      string
		source    []byte
		expected  map[string]string
		remaining []byte
		err       error
	}{
		{"empty string map", []byte{0, 0}, map[string]string{}, []byte{}, nil},
		{"map 1 key", []byte{
			0, 1, // map length
			0, 5, h, e, l, l, o, // key: hello
			0, 5, w, o, r, l, d, // value1: world
		}, map[string]string{"hello": "world"}, []byte{}, nil},
		{"map 2 keys", []byte{
			0, 2, // map length
			0, 5, h, e, l, l, o, // key1: hello
			0, 5, w, o, r, l, d, // value1: world
			0, 6, h, o, l, 0xc3, 0xa0, 0x21, // key2: holà!
			0, 5, m, u, n, d, o, // value2: mundo
		}, map[string]string{
			"hello": "world",
			"holà!": "mundo",
		}, []byte{}, nil},
		{
			"cannot read map length",
			[]byte{0},
			nil,
			[]byte{},
			fmt.Errorf("cannot read [string map] length: %w",
				fmt.Errorf("cannot read [short]: %w",
					errors.New("unexpected EOF"))),
		},
		{
			"cannot read key length",
			[]byte{0, 1, 0},
			nil,
			[]byte{},
			fmt.Errorf("cannot read [string map] key: %w",
				fmt.Errorf("cannot read [string] length: %w",
					fmt.Errorf("cannot read [short]: %w",
						errors.New("unexpected EOF")))),
		},
		{
			"cannot read key",
			[]byte{0, 1, 0, 2, 0},
			nil,
			[]byte{},
			fmt.Errorf("cannot read [string map] key: %w",
				errors.New("not enough bytes to read [string] content")),
		},
		{
			"cannot read value length",
			[]byte{0, 1, 0, 1, k, 0},
			nil,
			[]byte{},
			fmt.Errorf("cannot read [string map] value: %w",
				fmt.Errorf("cannot read [string] length: %w",
					fmt.Errorf("cannot read [short]: %w", errors.New("unexpected EOF")))),
		},
		{
			"cannot read value",
			[]byte{0, 1, 0, 1, k, 0, 2, 0},
			nil,
			[]byte{},
			fmt.Errorf(
				"cannot read [string map] value: %w",
				errors.New("not enough bytes to read [string] content"),
			),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(tt.source)
			actual, err := ReadStringMap(buf)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.remaining, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestWriteStringMap(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]string
		expected []byte
		err      error
	}{
		{
			"empty string map",
			map[string]string{},
			[]byte{0, 0},
			nil,
		},
		// not officially allowed by the specs, but better safe than sorry
		{
			"nil string map",
			nil,
			[]byte{0, 0},
			nil,
		},
		{
			"map 1 key",
			map[string]string{"hello": "world"},
			[]byte{
				0, 1, // map length
				0, 5, h, e, l, l, o, // key: hello
				0, 5, w, o, r, l, d, // value1: world
			},
			nil,
		},
		// Cannot test maps with > 1 key since map entry iteration order is not deterministic :-(
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := WriteStringMap(tt.input, buf)
			assert.Equal(t, tt.expected, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestReadStringMultiMap(t *testing.T) {
	tests := []struct {
		name      string
		source    []byte
		expected  map[string][]string
		remaining []byte
		err       error
	}{
		{"empty string multimap", []byte{0, 0}, map[string][]string{}, []byte{}, nil},
		{"multimap 1 key 1 value", []byte{
			0, 1, // map length
			0, 5, h, e, l, l, o, // key: hello
			0, 1, // list length
			0, 5, w, o, r, l, d, // value1: world
		}, map[string][]string{"hello": {"world"}}, []byte{}, nil},
		{"multimap 1 key 2 values", []byte{
			0, 1, // map length
			0, 5, h, e, l, l, o, // key: hello
			0, 2, // list length
			0, 5, w, o, r, l, d, // value1: world
			0, 5, m, u, n, d, o, // value2: mundo
		}, map[string][]string{"hello": {"world", "mundo"}}, []byte{}, nil},
		{"multimap 2 keys 2 values", []byte{
			0, 2, // map length
			0, 5, h, e, l, l, o, // key1: hello
			0, 2, // list length
			0, 5, w, o, r, l, d, // value1: world
			0, 5, m, u, n, d, o, // value2: mundo
			0, 6, h, o, l, 0xc3, 0xa0, 0x21, // key2: holà!
			0, 2, // list length
			0, 5, w, o, r, l, d, // value1: world
			0, 5, m, u, n, d, o, // value2: mundo
		}, map[string][]string{
			"hello": {"world", "mundo"},
			"holà!": {"world", "mundo"},
		}, []byte{}, nil},
		{
			"cannot read map length",
			[]byte{0},
			nil,
			[]byte{},
			fmt.Errorf(
				"cannot read [string multimap] length: %w",
				fmt.Errorf("cannot read [short]: %w", errors.New("unexpected EOF")),
			),
		},
		{
			"cannot read key length",
			[]byte{0, 1, 0},
			nil,
			[]byte{},
			fmt.Errorf(
				"cannot read [string multimap] key: %w",
				fmt.Errorf("cannot read [string] length: %w",
					fmt.Errorf("cannot read [short]: %w", errors.New("unexpected EOF"))),
			),
		},
		{
			"cannot read list length",
			[]byte{0, 1, 0, 1, k, 0},
			nil,
			[]byte{},
			fmt.Errorf(
				"cannot read [string multimap] value: %w",
				fmt.Errorf("cannot read [string list] length: %w",
					fmt.Errorf("cannot read [short]: %w", errors.New("unexpected EOF"))),
			),
		},
		{
			"cannot read element length",
			[]byte{0, 1, 0, 1, k, 0, 1, 0},
			nil,
			[]byte{},
			fmt.Errorf(
				"cannot read [string multimap] value: %w",
				fmt.Errorf("cannot read [string list] element: %w",
					fmt.Errorf("cannot read [string] length: %w",
						fmt.Errorf("cannot read [short]: %w", errors.New("unexpected EOF")))),
			),
		},
		{
			"cannot read list",
			[]byte{0, 1, 0, 1, k, 0, 1, 0, 5, h, e, l, l},
			nil,
			[]byte{},
			fmt.Errorf("cannot read [string multimap] value: %w",
				fmt.Errorf("cannot read [string list] element: %w",
					errors.New("not enough bytes to read [string] content"))),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(tt.source)
			actual, err := ReadStringMultiMap(buf)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.remaining, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestWriteStringMultiMap(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string][]string
		expected []byte
		err      error
	}{
		{
			"empty string multimap",
			map[string][]string{},
			[]byte{0, 0},
			nil,
		},
		// not officially allowed by the specs, but better safe than sorry
		{
			"nil string multimap",
			nil,
			[]byte{0, 0},
			nil,
		},
		{
			"multimap 1 key 1 value",
			map[string][]string{"hello": {"world"}},
			[]byte{
				0, 1, // map length
				0, 5, h, e, l, l, o, // key: hello
				0, 1, // list length
				0, 5, w, o, r, l, d, // value1: world
			},
			nil,
		},
		{
			"multimap 1 key 2 values",
			map[string][]string{"hello": {"world", "mundo"}},
			[]byte{
				0, 1, // map length
				0, 5, h, e, l, l, o, // key: hello
				0, 2, // list length
				0, 5, w, o, r, l, d, // value1: world
				0, 5, m, u, n, d, o, // value2: mundo
			},
			nil,
		},
		// Cannot test maps with > 1 key since map entry iteration order is not deterministic :-(
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := WriteStringMultiMap(tt.input, buf)
			assert.Equal(t, tt.expected, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestReadBytesMap(t *testing.T) {
	tests := []struct {
		name      string
		source    []byte
		expected  map[string][]byte
		remaining []byte
		err       error
	}{
		{"empty bytes map", []byte{0, 0}, map[string][]byte{}, []byte{}, nil},
		{"map 1 key", []byte{
			0, 1, // map length
			0, 5, h, e, l, l, o, // key: hello
			0, 0, 0, 5, w, o, r, l, d, // value1: world
		}, map[string][]byte{"hello": {w, o, r, l, d}}, []byte{}, nil},
		{"map 2 keys", []byte{
			0, 2, // map length
			0, 5, h, e, l, l, o, // key1: hello
			0, 0, 0, 5, w, o, r, l, d, // value1: world
			0, 6, h, o, l, 0xc3, 0xa0, 0x21, // key2: holà!
			0, 0, 0, 5, m, u, n, d, o, // value2: mundo
		}, map[string][]byte{
			"hello": {w, o, r, l, d},
			"holà!": {m, u, n, d, o},
		}, []byte{}, nil},
		{
			"cannot read map length",
			[]byte{0},
			nil,
			[]byte{},
			fmt.Errorf(
				"cannot read [bytes map] length: %w",
				fmt.Errorf("cannot read [short]: %w", errors.New("unexpected EOF")),
			),
		},
		{
			"cannot read key length",
			[]byte{0, 1, 0},
			nil,
			[]byte{},
			fmt.Errorf(
				"cannot read [bytes map] key: %w",
				fmt.Errorf("cannot read [string] length: %w",
					fmt.Errorf("cannot read [short]: %w", errors.New("unexpected EOF"))),
			),
		},
		{
			"cannot read key",
			[]byte{0, 1, 0, 2, 0},
			nil,
			[]byte{},
			fmt.Errorf(
				"cannot read [bytes map] key: %w",
				errors.New("not enough bytes to read [string] content"),
			),
		},
		{
			"cannot read value length",
			[]byte{0, 1, 0, 1, k, 0, 0, 0},
			nil,
			[]byte{},
			fmt.Errorf(
				"cannot read [bytes map] value: %w",
				fmt.Errorf("cannot read [bytes] length: %w",
					fmt.Errorf("cannot read [int]: %w", errors.New("unexpected EOF"))),
			),
		},
		{
			"cannot read value",
			[]byte{0, 1, 0, 1, k, 0, 0, 0, 2, 0},
			nil,
			[]byte{},
			fmt.Errorf(
				"cannot read [bytes map] value: %w",
				errors.New("not enough bytes to read [bytes] content"),
			),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(tt.source)
			actual, err := ReadBytesMap(buf)
			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.remaining, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestWriteBytesMap(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string][]byte
		expected []byte
		err      error
	}{
		{
			"empty bytes map",
			map[string][]byte{},
			[]byte{0, 0},
			nil,
		},
		// not officially allowed by the specs, but better safe than sorry
		{
			"nil bytes map",
			nil,
			[]byte{0, 0},
			nil,
		},
		{
			"map 1 key",
			map[string][]byte{"hello": {w, o, r, l, d}},
			[]byte{
				0, 1, // map length
				0, 5, h, e, l, l, o, // key: hello
				0, 0, 0, 5, w, o, r, l, d, // value1: world
			},
			nil,
		},
		// Cannot test maps with > 1 key since map entry iteration order is not deterministic :-(
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := WriteBytesMap(tt.input, buf)
			assert.Equal(t, tt.expected, buf.Bytes())
			assert.Equal(t, tt.err, err)
		})
	}
}
