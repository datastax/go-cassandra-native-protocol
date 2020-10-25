package message

import (
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitives"
	"io"
)

type Prepare struct {
	Query    string
	Keyspace string
}

func (m *Prepare) IsResponse() bool {
	return false
}

func (m *Prepare) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodePrepare
}

func (m *Prepare) String() string {
	return fmt.Sprintf("PREPARE (%v, %v)", m.Query, m.Keyspace)
}

type PrepareCodec struct{}

func (c *PrepareCodec) Encode(msg Message, dest io.Writer, version cassandraprotocol.ProtocolVersion) (err error) {
	prepare, ok := msg.(*Prepare)
	if !ok {
		return errors.New(fmt.Sprintf("expected *message.Prepare, got %T", msg))
	}
	if version < cassandraprotocol.ProtocolVersion5 && prepare.Keyspace != "" {
		return fmt.Errorf("PREPARE cannot set keyspace with protocol version: %v", version)
	}
	if err = primitives.WriteLongString(prepare.Query, dest); err != nil {
		return fmt.Errorf("cannot write PREPARE query: %w", err)
	}
	if version >= cassandraprotocol.ProtocolVersion5 {
		var flags cassandraprotocol.PrepareFlag
		if prepare.Keyspace != "" {
			flags |= cassandraprotocol.PrepareFlagWithKeyspace
		}
		if err = primitives.WriteInt(flags, dest); err != nil {
			return fmt.Errorf("cannot write PREPARE flags: %w", err)
		}
		if flags&cassandraprotocol.PrepareFlagWithKeyspace > 0 {
			if err = primitives.WriteString(prepare.Keyspace, dest); err != nil {
				return fmt.Errorf("cannot write PREPARE keyspace: %w", err)
			}
		}
	}
	return
}

func (c *PrepareCodec) EncodedLength(msg Message, version cassandraprotocol.ProtocolVersion) (size int, err error) {
	prepare, ok := msg.(*Prepare)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected *message.Prepare, got %T", msg))
	}
	size += primitives.LengthOfLongString(prepare.Query)
	if version >= cassandraprotocol.ProtocolVersion5 {
		size += primitives.LengthOfInt // flags
		if prepare.Keyspace != "" {
			size += primitives.LengthOfString(prepare.Keyspace)
		}

	}
	return size, nil
}

func (c *PrepareCodec) Decode(source io.Reader, version cassandraprotocol.ProtocolVersion) (msg Message, err error) {
	prepare := &Prepare{}
	if prepare.Query, err = primitives.ReadLongString(source); err != nil {
		return nil, fmt.Errorf("cannot read PREPARE query: %w", err)
	}
	if version >= cassandraprotocol.ProtocolVersion5 {
		var flags cassandraprotocol.PrepareFlag
		if flags, err = primitives.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read PREPARE flags: %w", err)
		}
		if flags&cassandraprotocol.PrepareFlagWithKeyspace > 0 {
			if prepare.Keyspace, err = primitives.ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read PREPARE keyspace: %w", err)
			}
		}
	}
	return prepare, nil
}

func (c *PrepareCodec) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodePrepare
}
