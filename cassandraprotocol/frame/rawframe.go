package frame

import (
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitive"
)

// A low-level representation of a frame, where the body is not decoded.
type RawFrame struct {
	RawHeader *RawHeader
	RawBody   []byte
}

// A low-level representation of a frame header, as it is parsed from an encoded frame.
type RawHeader struct {
	IsResponse bool
	Version    primitive.ProtocolVersion
	Flags      primitive.HeaderFlag
	StreamId   int16
	OpCode     primitive.OpCode
	BodyLength int32
}

// IsCompressible returns true if the frame contains a body that can be compressed. Bodies containing STARTUP
// should never be compressed. Empty messages like OPTIONS and READY also should not be compressed,
// even if compression is in use.
func (f *RawFrame) IsCompressible() bool {
	return isCompressible(f.RawHeader.OpCode)
}

func (r *RawHeader) String() string {
	return fmt.Sprintf("{response: %v, version: %v, flags: %08b, stream id: %v, opcode: %v, body length: %v}",
		r.IsResponse, r.Version, r.Flags, r.StreamId, r.OpCode, r.BodyLength)
}

func (f *RawFrame) String() string {
	return fmt.Sprintf("{header: %v, body: %v}", f.RawHeader, f.RawBody)
}
