package message

import (
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitives"
	"io"
)

type Batch struct {
	Type              primitives.BatchType
	Children          []*BatchChild
	Consistency       primitives.ConsistencyLevel
	SerialConsistency primitives.ConsistencyLevel
	DefaultTimestamp  int64
	// Introduced in Protocol Version 5, also present in DSE protocol v2.
	Keyspace string
	// Introduced in Protocol Version 5, not present in DSE protocol versions.
	NowInSeconds int32
}

func NewBatch(customizers ...BatchCustomizer) *Batch {
	batch := &Batch{
		Type:              primitives.BatchTypeLogged,
		Consistency:       primitives.ConsistencyLevelOne,
		SerialConsistency: primitives.ConsistencyLevelSerial,
		DefaultTimestamp:  DefaultTimestampNone,
		NowInSeconds:      NowInSecondsNone,
	}
	for _, customizer := range customizers {
		customizer(batch)
	}
	return batch
}

type BatchCustomizer func(*Batch)

func WithBatchType(batchType primitives.BatchType) BatchCustomizer {
	return func(batch *Batch) {
		batch.Type = batchType
	}
}

func WithBatchChildren(children ...*BatchChild) BatchCustomizer {
	return func(batch *Batch) {
		batch.Children = children
	}
}

func WithBatchConsistencyLevel(consistency primitives.ConsistencyLevel) BatchCustomizer {
	return func(batch *Batch) {
		batch.Consistency = consistency
	}
}

func WithBatchSerialConsistencyLevel(consistency primitives.ConsistencyLevel) BatchCustomizer {
	return func(batch *Batch) {
		batch.SerialConsistency = consistency
	}
}

func WithBatchDefaultTimestamp(defaultTimestamp int64) BatchCustomizer {
	return func(batch *Batch) {
		batch.DefaultTimestamp = defaultTimestamp
	}
}

func WithBatchKeyspace(keyspace string) BatchCustomizer {
	return func(batch *Batch) {
		batch.Keyspace = keyspace
	}
}

func WithBatchNowInSeconds(nowInSeconds int32) BatchCustomizer {
	return func(batch *Batch) {
		batch.NowInSeconds = nowInSeconds
	}
}

func (m *Batch) IsResponse() bool {
	return false
}

func (m *Batch) GetOpCode() primitives.OpCode {
	return primitives.OpCodeBatch
}

func (m *Batch) String() string {
	return fmt.Sprintf("BATCH (%d statements)", len(m.Children))
}

func (m *Batch) Flags() primitives.QueryFlag {
	var flags primitives.QueryFlag
	if m.SerialConsistency != primitives.ConsistencyLevelSerial {
		flags |= primitives.QueryFlagSerialConsistency
	}
	if m.DefaultTimestamp != DefaultTimestampNone {
		flags |= primitives.QueryFlagDefaultTimestamp
	}
	if m.Keyspace != "" {
		flags |= primitives.QueryFlagWithKeyspace
	}
	if m.NowInSeconds != NowInSecondsNone {
		flags |= primitives.QueryFlagNowInSeconds
	}
	return flags
}

type BatchChild struct {
	QueryOrId interface{} // string or []byte
	// Note: named values are in theory possible, but their server-side implementation is
	// broken. see https://issues.apache.org/jira/browse/CASSANDRA-10246
	Values []*primitives.Value
}

func NewQueryBatchChild(query string, values ...*primitives.Value) *BatchChild {
	return &BatchChild{QueryOrId: query, Values: values}
}

func NewPreparedBatchChild(preparedId []byte, values ...*primitives.Value) *BatchChild {
	return &BatchChild{QueryOrId: preparedId, Values: values}
}

type BatchCodec struct{}

func (c *BatchCodec) Encode(msg Message, dest io.Writer, version primitives.ProtocolVersion) (err error) {
	batch, ok := msg.(*Batch)
	if !ok {
		return errors.New(fmt.Sprintf("expected *message.Batch, got %T", msg))
	}
	if err = primitives.CheckBatchType(batch.Type); err != nil {
		return err
	} else if err = primitives.WriteByte(batch.Type, dest); err != nil {
		return fmt.Errorf("cannot write BATCH type: %w", err)
	}
	childrenCount := len(batch.Children)
	if childrenCount == 0 {
		return errors.New("BATCH messages must contain at least one child query")
	} else if childrenCount > 0xFFFF {
		return errors.New(fmt.Sprintf("BATCH messages can contain at most %d child queries", 0xFFFF))
	} else if err = primitives.WriteShort(uint16(childrenCount), dest); err != nil {
		return fmt.Errorf("cannot write BATCH query count: %w", err)
	}
	for i, child := range batch.Children {
		switch queryOrId := child.QueryOrId.(type) {
		case string:
			if err = primitives.WriteByte(0, dest); err != nil {
				return fmt.Errorf("cannot write BATCH query kind 0 for child #%d: %w", i, err)
			} else if queryOrId == "" {
				return fmt.Errorf("cannot write empty BATCH query string for child #%d", i)
			} else if err = primitives.WriteLongString(queryOrId, dest); err != nil {
				return fmt.Errorf("cannot write BATCH query string for child #%d: %w", i, err)
			}
		case []byte:
			if err = primitives.WriteByte(1, dest); err != nil {
				return fmt.Errorf("cannot write BATCH query kind 1 for child #%d: %w", i, err)
			} else if len(queryOrId) == 0 {
				return fmt.Errorf("cannot write empty BATCH query id for child #%d: %w", i, err)
			} else if err = primitives.WriteShortBytes(queryOrId, dest); err != nil {
				return fmt.Errorf("cannot write BATCH query id for child #%d: %w", i, err)
			}
		default:
			return fmt.Errorf("unsupported BATCH child type for child #%d: %T", i, queryOrId)
		}
		if err = primitives.WritePositionalValues(child.Values, dest, version); err != nil {
			return fmt.Errorf("cannot write BATCH positional values for child #%d: %w", i, err)
		}
	}
	if err = primitives.WriteShort(batch.Consistency, dest); err != nil {
		return fmt.Errorf("cannot write BATCH consistency: %w", err)
	}
	flags := batch.Flags()
	if version >= primitives.ProtocolVersion5 {
		err = primitives.WriteInt(int32(flags), dest)
	} else {
		err = primitives.WriteByte(uint8(flags), dest)
	}
	if err != nil {
		return fmt.Errorf("cannot write BATCH query flags: %w", err)
	}
	if flags&primitives.QueryFlagSerialConsistency > 0 {
		if err = primitives.WriteShort(batch.SerialConsistency, dest); err != nil {
			return fmt.Errorf("cannot write BATCH serial consistency: %w", err)
		}
	}
	if flags&primitives.QueryFlagDefaultTimestamp > 0 {
		if err = primitives.WriteLong(batch.DefaultTimestamp, dest); err != nil {
			return fmt.Errorf("cannot write BATCH default timestamp: %w", err)
		}
	}
	if flags&primitives.QueryFlagWithKeyspace > 0 {
		if batch.Keyspace == "" {
			return errors.New("cannot write BATCH empty keyspace")
		} else if err = primitives.WriteString(batch.Keyspace, dest); err != nil {
			return fmt.Errorf("cannot write BATCH keyspace: %w", err)
		}
	}
	if flags&primitives.QueryFlagNowInSeconds > 0 {
		if version < primitives.ProtocolVersion5 {
			return fmt.Errorf("cannot set BATCH now-in-seconds with protocol version %d", version)
		} else if err = primitives.WriteInt(batch.NowInSeconds, dest); err != nil {
			return fmt.Errorf("cannot write BATCH now-in-seconds: %w", err)
		}
	}
	return nil
}

func (c *BatchCodec) EncodedLength(msg Message, version primitives.ProtocolVersion) (length int, err error) {
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
	if version >= primitives.ProtocolVersion5 {
		length += primitives.LengthOfInt
	} else {
		length += primitives.LengthOfByte
	}
	flags := batch.Flags()
	if flags&primitives.QueryFlagSerialConsistency > 0 {
		length += primitives.LengthOfShort
	}
	if flags&primitives.QueryFlagDefaultTimestamp > 0 {
		length += primitives.LengthOfLong
	}
	if flags&primitives.QueryFlagWithKeyspace > 0 {
		length += primitives.LengthOfString(batch.Keyspace)
	}
	if flags&primitives.QueryFlagNowInSeconds > 0 {
		length += primitives.LengthOfInt
	}
	return length, nil
}

func (c *BatchCodec) Decode(source io.Reader, version primitives.ProtocolVersion) (msg Message, err error) {
	batch := NewBatch()
	if batch.Type, err = primitives.ReadByte(source); err != nil {
		return nil, fmt.Errorf("cannot read BATCH type: %w", err)
	} else if err = primitives.CheckBatchType(batch.Type); err != nil {
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
		if child.Values, err = primitives.ReadPositionalValues(source, version); err != nil {
			return nil, fmt.Errorf("cannot read BATCH positional values for child #%d: %w", i, err)
		}
		batch.Children[i] = child
	}
	if batch.Consistency, err = primitives.ReadShort(source); err != nil {
		return nil, fmt.Errorf("cannot read BATCH consistency: %w", err)
	}
	var flags primitives.QueryFlag
	if version >= primitives.ProtocolVersion5 {
		var f int32
		f, err = primitives.ReadInt(source)
		flags = primitives.QueryFlag(f)
	} else {
		var f uint8
		f, err = primitives.ReadByte(source)
		flags = primitives.QueryFlag(f)
	}
	if err != nil {
		return nil, fmt.Errorf("cannot read BATCH query flags: %w", err)
	}
	if flags&primitives.QueryFlagValueNames > 0 {
		return nil, errors.New("cannot use BATCH with named values, see CASSANDRA-10246")
	}
	if flags&primitives.QueryFlagSerialConsistency > 0 {
		if batch.SerialConsistency, err = primitives.ReadShort(source); err != nil {
			return nil, fmt.Errorf("cannot read BATCH serial consistency: %w", err)
		}
	}
	if flags&primitives.QueryFlagDefaultTimestamp > 0 {
		if batch.DefaultTimestamp, err = primitives.ReadLong(source); err != nil {
			return nil, fmt.Errorf("cannot read BATCH default timestamp: %w", err)
		}
	}
	if version >= primitives.ProtocolVersion5 {
		if flags&primitives.QueryFlagWithKeyspace > 0 {
			if batch.Keyspace, err = primitives.ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read BATCH keyspace: %w", err)
			}
		}
		if flags&primitives.QueryFlagNowInSeconds > 0 {
			if batch.NowInSeconds, err = primitives.ReadInt(source); err != nil {
				return nil, fmt.Errorf("cannot read BATCH now-in-seconds: %w", err)
			}
		}
	}
	return batch, nil
}

func (c *BatchCodec) GetOpCode() primitives.OpCode {
	return primitives.OpCodeBatch
}
