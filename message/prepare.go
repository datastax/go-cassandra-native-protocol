package message

import (
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"io"
)

type Prepare struct {
	Query string
	// Introduced in Protocol Version 5, also present in DSE protocol v2.
	Keyspace string
}

func (m *Prepare) IsResponse() bool {
	return false
}

func (m *Prepare) GetOpCode() primitive.OpCode {
	return primitive.OpCodePrepare
}

func (m *Prepare) String() string {
	return fmt.Sprintf("PREPARE (%v, %v)", m.Query, m.Keyspace)
}

func (m *Prepare) Flags() primitive.PrepareFlag {
	var flags primitive.PrepareFlag
	if m.Keyspace != "" {
		flags = flags.Add(primitive.PrepareFlagWithKeyspace)
	}
	return flags
}

type prepareCodec struct{}

func (c *prepareCodec) Encode(msg Message, dest io.Writer, version primitive.ProtocolVersion) (err error) {
	prepare, ok := msg.(*Prepare)
	if !ok {
		return errors.New(fmt.Sprintf("expected *message.Prepare, got %T", msg))
	}
	if prepare.Query == "" {
		return errors.New("cannot write PREPARE empty query string")
	} else if err = primitive.WriteLongString(prepare.Query, dest); err != nil {
		return fmt.Errorf("cannot write PREPARE query string: %w", err)
	}
	if hasPrepareFlags(version) {
		flags := prepare.Flags()
		if err = primitive.WriteInt(int32(flags), dest); err != nil {
			return fmt.Errorf("cannot write PREPARE flags: %w", err)
		}
		if flags.Contains(primitive.PrepareFlagWithKeyspace) {
			if prepare.Keyspace == "" {
				return errors.New("cannot write empty keyspace")
			} else if err = primitive.WriteString(prepare.Keyspace, dest); err != nil {
				return fmt.Errorf("cannot write PREPARE keyspace: %w", err)
			}
		}
	}
	return
}

func (c *prepareCodec) EncodedLength(msg Message, version primitive.ProtocolVersion) (size int, err error) {
	prepare, ok := msg.(*Prepare)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected *message.Prepare, got %T", msg))
	}
	size += primitive.LengthOfLongString(prepare.Query)
	if hasPrepareFlags(version) {
		size += primitive.LengthOfInt // flags
		if prepare.Keyspace != "" {
			size += primitive.LengthOfString(prepare.Keyspace)
		}
	}
	return size, nil
}

func (c *prepareCodec) Decode(source io.Reader, version primitive.ProtocolVersion) (msg Message, err error) {
	prepare := &Prepare{}
	if prepare.Query, err = primitive.ReadLongString(source); err != nil {
		return nil, fmt.Errorf("cannot read PREPARE query: %w", err)
	}
	if hasPrepareFlags(version) {
		var flags primitive.PrepareFlag
		var f int32
		if f, err = primitive.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read PREPARE flags: %w", err)
		}
		flags = primitive.PrepareFlag(f)
		if flags.Contains(primitive.PrepareFlagWithKeyspace) {
			if prepare.Keyspace, err = primitive.ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read PREPARE keyspace: %w", err)
			}
		}
	}
	return prepare, nil
}

func (c *prepareCodec) GetOpCode() primitive.OpCode {
	return primitive.OpCodePrepare
}

func hasPrepareFlags(version primitive.ProtocolVersion) bool {
	return version >= primitive.ProtocolVersion5 &&
		version != primitive.ProtocolVersionDse1
}
