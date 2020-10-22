package message

import (
	"errors"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
	"io"
)

type Batch struct {
	Type              cassandraprotocol.BatchType
	Children          []*BatchChild
	Flags             cassandraprotocol.QueryFlag
	Consistency       cassandraprotocol.ConsistencyLevel
	SerialConsistency cassandraprotocol.ConsistencyLevel
	DefaultTimestamp  int64
	// Introduced in Protocol Version 5
	Keyspace string
	// Introduced in Protocol Version 5
	NowInSeconds int32
}

type BatchChild struct {
	QueryOrId interface{} // string or []byte
	// Note: named values are in theory possible, but their server-side implementation is
	// broken. see https://issues.apache.org/jira/browse/CASSANDRA-10246
	Values []*cassandraprotocol.Value
}

func (m *Batch) IsResponse() bool {
	return false
}

func (m *Batch) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeBatch
}

func (m *Batch) String() string {
	return fmt.Sprintf("BATCH (%d statements)", len(m.Children))
}

type BatchCodec struct{}

func (c *BatchCodec) Encode(msg Message, dest io.Writer, version cassandraprotocol.ProtocolVersion) (err error) {
	batch, ok := msg.(*Batch)
	if !ok {
		return errors.New(fmt.Sprintf("expected *message.Batch, got %T", msg))
	}
	if err = cassandraprotocol.CheckBatchType(batch.Type); err != nil {
		return err
	}
	if err = primitives.WriteByte(batch.Type, dest); err != nil {
		return fmt.Errorf("cannot write BATCH type: %w", err)
	}
	childrenCount := len(batch.Children)
	if childrenCount > 0xFFFF {
		return errors.New(fmt.Sprintf("BATCH messages can contain at most %d queries", 0xFFFF))
	}
	if err = primitives.WriteShort(uint16(childrenCount), dest); err != nil {
		return fmt.Errorf("cannot write BATCH query count: %w", err)
	}
	for i, child := range batch.Children {
		switch queryOrId := child.QueryOrId.(type) {
		case string:
			if err = primitives.WriteByte(0, dest); err != nil {
				return fmt.Errorf("cannot write BATCH query kind 0 for child #%d: %w", i, err)
			}
			if err = primitives.WriteLongString(queryOrId, dest); err != nil {
				return fmt.Errorf("cannot write BATCH query string for child #%d: %w", i, err)
			}
		case []byte:
			if err = primitives.WriteByte(1, dest); err != nil {
				return fmt.Errorf("cannot write BATCH query kind 1 for child #%d: %w", i, err)
			}
			if err = primitives.WriteShortBytes(queryOrId, dest); err != nil {
				return fmt.Errorf("cannot write BATCH query id for child #%d: %w", i, err)
			}
		default:
			return fmt.Errorf("unsupported BATCH child type for child #%d: %T", i, queryOrId)
		}
		if err = primitives.WritePositionalValues(child.Values, dest); err != nil {
			return fmt.Errorf("cannot write BATCH positional values for child #%d: %w", i, err)
		}
	}
	if err = primitives.WriteShort(batch.Consistency, dest); err != nil {
		return fmt.Errorf("cannot write BATCH consistency: %w", err)
	}
	if version >= cassandraprotocol.ProtocolVersion5 {
		err = primitives.WriteInt(batch.Flags, dest)
	} else {
		err = primitives.WriteByte(uint8(batch.Flags), dest)
	}
	if err != nil {
		return fmt.Errorf("cannot write BATCH query flags: %w", err)
	}
	if batch.Flags&cassandraprotocol.QueryFlagValueNames > 0 {
		return errors.New("cannot use BATCH with named values, see CASSANDRA-10246")
	}
	if batch.Flags&cassandraprotocol.QueryFlagSerialConsistency > 0 {
		if err = primitives.WriteShort(batch.SerialConsistency, dest); err != nil {
			return fmt.Errorf("cannot write BATCH serial consistency: %w", err)
		}
	}
	if batch.Flags&cassandraprotocol.QueryFlagDefaultTimestamp > 0 {
		if err = primitives.WriteLong(batch.DefaultTimestamp, dest); err != nil {
			return fmt.Errorf("cannot write BATCH default timestamp: %w", err)
		}
	}
	if batch.Flags&cassandraprotocol.QueryFlagWithKeyspace > 0 {
		if version < cassandraprotocol.ProtocolVersion5 {
			return errors.New(fmt.Sprintf("cannot set BATCH keyspace flag in protocol version %d", version))
		}
		if err = primitives.WriteString(batch.Keyspace, dest); err != nil {
			return fmt.Errorf("cannot write BATCH keyspace: %w", err)
		}
	}
	if batch.Flags&cassandraprotocol.QueryFlagNowInSeconds > 0 {
		if version < cassandraprotocol.ProtocolVersion5 {
			return errors.New(fmt.Sprintf("cannot set BATCH now-in-seconds flag in protocol version %d", version))
		}
		if err = primitives.WriteInt(batch.NowInSeconds, dest); err != nil {
			return fmt.Errorf("cannot write BATCH now-in-seconds: %w", err)
		}
	}
	return nil
}

func (c *BatchCodec) EncodedLength(msg Message, version cassandraprotocol.ProtocolVersion) (length int, err error) {
	batch, ok := msg.(*Batch)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected *message.Batch, got %T", msg))
	}
	childrenCount := len(batch.Children)
	if childrenCount > 0xFFFF {
		return -1, errors.New(fmt.Sprintf("BATCH messages can contain at most %d queries", 0xFFFF))
	}
	length += primitives.LengthOfByte  // type
	length += primitives.LengthOfShort // number of queries
	for i, child := range batch.Children {
		length += primitives.LengthOfByte // child type
		switch stringOrId := child.QueryOrId.(type) {
		case string:
			length += primitives.LengthOfLongString(stringOrId)
		case []byte:
			length += primitives.LengthOfShortBytes(stringOrId)
		default:
			return -1, fmt.Errorf("unsupported BATCH child type for child #%d: %T", i, stringOrId)
		}
		if valuesLength, err := primitives.LengthOfPositionalValues(child.Values); err != nil {
			return -1, fmt.Errorf("cannot compute length of BATCH positional values for child #%d: %w", i, err)
		} else {
			length += valuesLength
		}
	}
	length += primitives.LengthOfShort // consistency level
	// flags
	if version >= cassandraprotocol.ProtocolVersion5 {
		length += primitives.LengthOfInt
	} else {
		length += primitives.LengthOfByte
	}
	if batch.Flags&cassandraprotocol.QueryFlagSerialConsistency > 0 {
		length += primitives.LengthOfShort
	}
	if batch.Flags&cassandraprotocol.QueryFlagDefaultTimestamp > 0 {
		length += primitives.LengthOfLong
	}
	if batch.Flags&cassandraprotocol.QueryFlagWithKeyspace > 0 {
		length += primitives.LengthOfString(batch.Keyspace)
	}
	if batch.Flags&cassandraprotocol.QueryFlagNowInSeconds > 0 {
		length += primitives.LengthOfInt
	}
	return length, nil
}

func (c *BatchCodec) Decode(source io.Reader, version cassandraprotocol.ProtocolVersion) (msg Message, err error) {
	var batch = &Batch{}
	if batch.Type, err = primitives.ReadByte(source); err != nil {
		return nil, fmt.Errorf("cannot read BATCH type: %w", err)
	}
	if err = cassandraprotocol.CheckBatchType(batch.Type); err != nil {
		return nil, err
	}
	var childrenCount uint16
	if childrenCount, err = primitives.ReadShort(source); err != nil {
		return nil, fmt.Errorf("cannot read BATCH query count: %w", err)
	}
	batch.Children = make([]*BatchChild, childrenCount)
	for i := 0; i < int(childrenCount); i++ {
		var childType uint8
		if childType, err = primitives.ReadByte(source); err != nil {
			return nil, fmt.Errorf("cannot read BATCH child type for child #%d: %w", i, err)
		}
		var child = &BatchChild{}
		switch childType {
		case 0:
			if child.QueryOrId, err = primitives.ReadLongString(source); err != nil {
				return nil, fmt.Errorf("cannot read BATCH query string for child #%d: %w", i, err)
			}
		case 1:
			if child.QueryOrId, err = primitives.ReadShortBytes(source); err != nil {
				return nil, fmt.Errorf("cannot read BATCH query id for child #%d: %w", i, err)
			}
		default:
			return nil, fmt.Errorf("unsupported BATCH child type for child #%d: %v", i, childType)
		}
		if child.Values, err = primitives.ReadPositionalValues(source); err != nil {
			return nil, fmt.Errorf("cannot read BATCH positional values for child #%d: %w", i, err)
		}
		batch.Children[i] = child
	}
	if batch.Consistency, err = primitives.ReadShort(source); err != nil {
		return nil, fmt.Errorf("cannot read BATCH consistency: %w", err)
	}
	if version >= cassandraprotocol.ProtocolVersion5 {
		batch.Flags, err = primitives.ReadInt(source)
	} else {
		var flags uint8
		flags, err = primitives.ReadByte(source)
		batch.Flags = cassandraprotocol.QueryFlag(flags)
	}
	if err != nil {
		return nil, fmt.Errorf("cannot read BATCH query flags: %w", err)
	}
	if batch.Flags&cassandraprotocol.QueryFlagValueNames > 0 {
		return nil, errors.New("cannot use BATCH with named values, see CASSANDRA-10246")
	}
	if batch.Flags&cassandraprotocol.QueryFlagSerialConsistency > 0 {
		if batch.SerialConsistency, err = primitives.ReadShort(source); err != nil {
			return nil, fmt.Errorf("cannot read BATCH serial consistency: %w", err)
		}
	}
	if batch.Flags&cassandraprotocol.QueryFlagDefaultTimestamp > 0 {
		if batch.DefaultTimestamp, err = primitives.ReadLong(source); err != nil {
			return nil, fmt.Errorf("cannot read BATCH default timestamp: %w", err)
		}
	}
	if batch.Flags&cassandraprotocol.QueryFlagWithKeyspace > 0 {
		if batch.Keyspace, err = primitives.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read BATCH keyspace: %w", err)
		}
	}
	if batch.Flags&cassandraprotocol.QueryFlagNowInSeconds > 0 {
		if batch.NowInSeconds, err = primitives.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read BATCH now-in-seconds: %w", err)
		}
	}
	return batch, nil
}

func (c *BatchCodec) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeBatch
}
