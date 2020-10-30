package message

import (
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"io"
)

type Batch struct {
	Type              primitive.BatchType
	Children          []*BatchChild
	Consistency       primitive.ConsistencyLevel
	SerialConsistency *primitive.NillableConsistencyLevel
	DefaultTimestamp  *primitive.NillableInt64
	// Introduced in Protocol Version 5, also present in DSE protocol v2.
	Keyspace string
	// Introduced in Protocol Version 5, not present in DSE protocol versions.
	NowInSeconds *primitive.NillableInt32
}

func (m *Batch) IsResponse() bool {
	return false
}

func (m *Batch) GetOpCode() primitive.OpCode {
	return primitive.OpCodeBatch
}

func (m *Batch) String() string {
	return fmt.Sprintf("BATCH (%d statements)", len(m.Children))
}

func (m *Batch) Flags() primitive.QueryFlag {
	var flags primitive.QueryFlag
	if m.SerialConsistency != nil {
		flags |= primitive.QueryFlagSerialConsistency
	}
	if m.DefaultTimestamp != nil {
		flags |= primitive.QueryFlagDefaultTimestamp
	}
	if m.Keyspace != "" {
		flags |= primitive.QueryFlagWithKeyspace
	}
	if m.NowInSeconds != nil {
		flags |= primitive.QueryFlagNowInSeconds
	}
	return flags
}

type BatchChild struct {
	// Either a string or []byte; the former should be the CQL statement to execute, the latter the prepared id of
	// the statement to execute.
	QueryOrId interface{}
	// Note: named values are in theory possible, but their server-side implementation is
	// broken. see https://issues.apache.org/jira/browse/CASSANDRA-10246
	Values []*primitive.Value
}

type batchCodec struct{}

func (c *batchCodec) Encode(msg Message, dest io.Writer, version primitive.ProtocolVersion) (err error) {
	batch, ok := msg.(*Batch)
	if !ok {
		return errors.New(fmt.Sprintf("expected *message.Batch, got %T", msg))
	}
	if err = primitive.CheckBatchType(batch.Type); err != nil {
		return err
	} else if err = primitive.WriteByte(batch.Type, dest); err != nil {
		return fmt.Errorf("cannot write BATCH type: %w", err)
	}
	childrenCount := len(batch.Children)
	if childrenCount == 0 {
		return errors.New("BATCH messages must contain at least one child query")
	} else if childrenCount > 0xFFFF {
		return errors.New(fmt.Sprintf("BATCH messages can contain at most %d child queries", 0xFFFF))
	} else if err = primitive.WriteShort(uint16(childrenCount), dest); err != nil {
		return fmt.Errorf("cannot write BATCH query count: %w", err)
	}
	for i, child := range batch.Children {
		switch queryOrId := child.QueryOrId.(type) {
		case string:
			if err = primitive.WriteByte(primitive.BatchChildTypeQueryString, dest); err != nil {
				return fmt.Errorf("cannot write BATCH query kind 0 for child #%d: %w", i, err)
			} else if queryOrId == "" {
				return fmt.Errorf("cannot write empty BATCH query string for child #%d", i)
			} else if err = primitive.WriteLongString(queryOrId, dest); err != nil {
				return fmt.Errorf("cannot write BATCH query string for child #%d: %w", i, err)
			}
		case []byte:
			if err = primitive.WriteByte(primitive.BatchChildTypePreparedId, dest); err != nil {
				return fmt.Errorf("cannot write BATCH query kind 1 for child #%d: %w", i, err)
			} else if len(queryOrId) == 0 {
				return fmt.Errorf("cannot write empty BATCH query id for child #%d: %w", i, err)
			} else if err = primitive.WriteShortBytes(queryOrId, dest); err != nil {
				return fmt.Errorf("cannot write BATCH query id for child #%d: %w", i, err)
			}
		default:
			return fmt.Errorf("unsupported BATCH child type for child #%d: %T", i, queryOrId)
		}
		if err = primitive.WritePositionalValues(child.Values, dest, version); err != nil {
			return fmt.Errorf("cannot write BATCH positional values for child #%d: %w", i, err)
		}
	}
	if err = primitive.WriteShort(batch.Consistency, dest); err != nil {
		return fmt.Errorf("cannot write BATCH consistency: %w", err)
	}
	flags := batch.Flags()
	if version >= primitive.ProtocolVersion5 {
		err = primitive.WriteInt(int32(flags), dest)
	} else {
		err = primitive.WriteByte(uint8(flags), dest)
	}
	if err != nil {
		return fmt.Errorf("cannot write BATCH query flags: %w", err)
	}
	if flags&primitive.QueryFlagSerialConsistency > 0 {
		if err = primitive.WriteShort(batch.SerialConsistency.Value, dest); err != nil {
			return fmt.Errorf("cannot write BATCH serial consistency: %w", err)
		}
	}
	if flags&primitive.QueryFlagDefaultTimestamp > 0 {
		if err = primitive.WriteLong(batch.DefaultTimestamp.Value, dest); err != nil {
			return fmt.Errorf("cannot write BATCH default timestamp: %w", err)
		}
	}
	if flags&primitive.QueryFlagWithKeyspace > 0 {
		if batch.Keyspace == "" {
			return errors.New("cannot write BATCH empty keyspace")
		} else if err = primitive.WriteString(batch.Keyspace, dest); err != nil {
			return fmt.Errorf("cannot write BATCH keyspace: %w", err)
		}
	}
	if flags&primitive.QueryFlagNowInSeconds > 0 {
		if version != primitive.ProtocolVersion5 {
			return fmt.Errorf("cannot set BATCH now-in-seconds with protocol version %d", version)
		} else if err = primitive.WriteInt(batch.NowInSeconds.Value, dest); err != nil {
			return fmt.Errorf("cannot write BATCH now-in-seconds: %w", err)
		}
	}
	return nil
}

func (c *batchCodec) EncodedLength(msg Message, version primitive.ProtocolVersion) (length int, err error) {
	batch, ok := msg.(*Batch)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected *message.Batch, got %T", msg))
	}
	childrenCount := len(batch.Children)
	if childrenCount > 0xFFFF {
		return -1, errors.New(fmt.Sprintf("BATCH messages can contain at most %d queries", 0xFFFF))
	}
	length += primitive.LengthOfByte  // type
	length += primitive.LengthOfShort // number of queries
	for i, child := range batch.Children {
		length += primitive.LengthOfByte // child type
		switch stringOrId := child.QueryOrId.(type) {
		case string:
			length += primitive.LengthOfLongString(stringOrId)
		case []byte:
			length += primitive.LengthOfShortBytes(stringOrId)
		default:
			return -1, fmt.Errorf("unsupported BATCH child type for child #%d: %T", i, stringOrId)
		}
		if valuesLength, err := primitive.LengthOfPositionalValues(child.Values); err != nil {
			return -1, fmt.Errorf("cannot compute length of BATCH positional values for child #%d: %w", i, err)
		} else {
			length += valuesLength
		}
	}
	length += primitive.LengthOfShort // consistency level
	// flags
	if version >= primitive.ProtocolVersion5 {
		length += primitive.LengthOfInt
	} else {
		length += primitive.LengthOfByte
	}
	flags := batch.Flags()
	if flags&primitive.QueryFlagSerialConsistency > 0 {
		length += primitive.LengthOfShort
	}
	if flags&primitive.QueryFlagDefaultTimestamp > 0 {
		length += primitive.LengthOfLong
	}
	if flags&primitive.QueryFlagWithKeyspace > 0 {
		length += primitive.LengthOfString(batch.Keyspace)
	}
	if flags&primitive.QueryFlagNowInSeconds > 0 {
		length += primitive.LengthOfInt
	}
	return length, nil
}

func (c *batchCodec) Decode(source io.Reader, version primitive.ProtocolVersion) (msg Message, err error) {
	batch := &Batch{}
	if batch.Type, err = primitive.ReadByte(source); err != nil {
		return nil, fmt.Errorf("cannot read BATCH type: %w", err)
	} else if err = primitive.CheckBatchType(batch.Type); err != nil {
		return nil, err
	}
	var childrenCount uint16
	if childrenCount, err = primitive.ReadShort(source); err != nil {
		return nil, fmt.Errorf("cannot read BATCH query count: %w", err)
	}
	batch.Children = make([]*BatchChild, childrenCount)
	for i := 0; i < int(childrenCount); i++ {
		var childType uint8
		if childType, err = primitive.ReadByte(source); err != nil {
			return nil, fmt.Errorf("cannot read BATCH child type for child #%d: %w", i, err)
		}
		var child = &BatchChild{}
		switch childType {
		case primitive.BatchChildTypeQueryString:
			if child.QueryOrId, err = primitive.ReadLongString(source); err != nil {
				return nil, fmt.Errorf("cannot read BATCH query string for child #%d: %w", i, err)
			}
		case primitive.BatchChildTypePreparedId:
			if child.QueryOrId, err = primitive.ReadShortBytes(source); err != nil {
				return nil, fmt.Errorf("cannot read BATCH query id for child #%d: %w", i, err)
			}
		default:
			return nil, fmt.Errorf("unsupported BATCH child type for child #%d: %v", i, childType)
		}
		if child.Values, err = primitive.ReadPositionalValues(source, version); err != nil {
			return nil, fmt.Errorf("cannot read BATCH positional values for child #%d: %w", i, err)
		}
		batch.Children[i] = child
	}
	if batch.Consistency, err = primitive.ReadShort(source); err != nil {
		return nil, fmt.Errorf("cannot read BATCH consistency: %w", err)
	}
	var flags primitive.QueryFlag
	if version >= primitive.ProtocolVersion5 {
		var f int32
		f, err = primitive.ReadInt(source)
		flags = primitive.QueryFlag(f)
	} else {
		var f uint8
		f, err = primitive.ReadByte(source)
		flags = primitive.QueryFlag(f)
	}
	if err != nil {
		return nil, fmt.Errorf("cannot read BATCH query flags: %w", err)
	}
	if flags&primitive.QueryFlagValueNames > 0 {
		return nil, errors.New("cannot use BATCH with named values, see CASSANDRA-10246")
	}
	if flags&primitive.QueryFlagSerialConsistency > 0 {
		batch.SerialConsistency = &primitive.NillableConsistencyLevel{}
		if batch.SerialConsistency.Value, err = primitive.ReadShort(source); err != nil {
			return nil, fmt.Errorf("cannot read BATCH serial consistency: %w", err)
		}
	}
	if flags&primitive.QueryFlagDefaultTimestamp > 0 {
		batch.DefaultTimestamp = &primitive.NillableInt64{}
		if batch.DefaultTimestamp.Value, err = primitive.ReadLong(source); err != nil {
			return nil, fmt.Errorf("cannot read BATCH default timestamp: %w", err)
		}
	}
	if version >= primitive.ProtocolVersion5 {
		if flags&primitive.QueryFlagWithKeyspace > 0 {
			if batch.Keyspace, err = primitive.ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read BATCH keyspace: %w", err)
			}
		}
		if flags&primitive.QueryFlagNowInSeconds > 0 {
			batch.NowInSeconds = &primitive.NillableInt32{}
			if batch.NowInSeconds.Value, err = primitive.ReadInt(source); err != nil {
				return nil, fmt.Errorf("cannot read BATCH now-in-seconds: %w", err)
			}
		}
	}
	return batch, nil
}

func (c *batchCodec) GetOpCode() primitive.OpCode {
	return primitive.OpCodeBatch
}
