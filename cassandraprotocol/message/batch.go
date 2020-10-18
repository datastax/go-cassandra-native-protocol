package message

import (
	"errors"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
)

type Batch struct {
	Type         cassandraprotocol.BatchType
	QueriesOrIds []interface{} // string or []byte
	// Note: named values are in theory possible, but their server-side implementation is
	// broken. see https://issues.apache.org/jira/browse/CASSANDRA-10246
	Values            [][]*cassandraprotocol.Value
	Flags             cassandraprotocol.QueryFlag
	Consistency       cassandraprotocol.ConsistencyLevel
	SerialConsistency cassandraprotocol.ConsistencyLevel
	DefaultTimestamp  int64
	Keyspace          string
	NowInSeconds      int32
}

func (m *Batch) IsResponse() bool {
	return false
}

func (m *Batch) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeBatch
}

func (m *Batch) String() string {
	return fmt.Sprintf("BATCH (%d statements)", len(m.QueriesOrIds))
}

type BatchCodec struct{}

func (c *BatchCodec) Encode(msg Message, dest []byte, version cassandraprotocol.ProtocolVersion) (err error) {
	batch := msg.(*Batch)
	if dest, err = primitives.WriteByte(batch.Type, dest); err != nil {
		return fmt.Errorf("cannot write BATCH type: %w", err)
	}
	if dest, err = primitives.WriteShort(uint16(len(batch.QueriesOrIds)), dest); err != nil {
		return fmt.Errorf("cannot write BATCH query count: %w", err)
	}
	for i, query := range batch.QueriesOrIds {
		switch stringOrId := query.(type) {
		case string:
			if dest, err = primitives.WriteByte(0, dest); err != nil {
				return fmt.Errorf("cannot write BATCH query kind 0: %w", err)
			}
			if dest, err = primitives.WriteLongString(stringOrId, dest); err != nil {
				return fmt.Errorf("cannot write BATCH query string: %w", err)
			}
		case []byte:
			if dest, err = primitives.WriteByte(1, dest); err != nil {
				return fmt.Errorf("cannot write BATCH query kind 1: %w", err)
			}
			if dest, err = primitives.WriteShortBytes(stringOrId, dest); err != nil {
				return fmt.Errorf("cannot write BATCH query id: %w", err)
			}
		default:
			return fmt.Errorf("unsupported BATCH child type: %T", stringOrId)
		}
		if dest, err = primitives.WritePositionalValues(batch.Values[i], dest); err != nil {
			return fmt.Errorf("cannot write BATCH positional values for child #%d: %w", i, err)
		}
	}
	if dest, err = primitives.WriteShort(batch.Consistency, dest); err != nil {
		return fmt.Errorf("cannot write BATCH consistency: %w", err)
	}
	if version >= cassandraprotocol.ProtocolVersion5 {
		dest, err = primitives.WriteInt(batch.Flags, dest)
	} else {
		dest, err = primitives.WriteByte(uint8(batch.Flags), dest)
	}
	if err != nil {
		return fmt.Errorf("cannot write BATCH query flags: %w", err)
	}
	if batch.Flags&cassandraprotocol.QueryFlagSerialConsistency > 0 {
		if dest, err = primitives.WriteShort(batch.SerialConsistency, dest); err != nil {
			return fmt.Errorf("cannot write BATCH serial consistency: %w", err)
		}
	}
	if batch.Flags&cassandraprotocol.QueryFlagDefaultTimestamp > 0 {
		if dest, err = primitives.WriteLong(batch.DefaultTimestamp, dest); err != nil {
			return fmt.Errorf("cannot write BATCH default timestamp: %w", err)
		}
	}
	if batch.Flags&cassandraprotocol.QueryFlagWithKeyspace > 0 {
		if dest, err = primitives.WriteString(batch.Keyspace, dest); err != nil {
			return fmt.Errorf("cannot write BATCH keyspace: %w", err)
		}
	}
	if batch.Flags&cassandraprotocol.QueryFlagNowInSeconds > 0 {
		if dest, err = primitives.WriteInt(batch.NowInSeconds, dest); err != nil {
			return fmt.Errorf("cannot write BATCH now-in-seconds: %w", err)
		}
	}
	return nil
}

func (c *BatchCodec) EncodedLength(msg Message, version cassandraprotocol.ProtocolVersion) (length int, err error) {
	batch := msg.(*Batch)
	if batch.Flags&cassandraprotocol.QueryFlagValueNames > 0 {
		return -1, errors.New("cannot read BATCH with named values, see CASSANDRA-10246")
	}
	queryCount := len(batch.QueriesOrIds)
	if queryCount > 0xFFFF {
		return -1, errors.New(fmt.Sprintf("batch messages can contain at most %d queries", 0xFFFF))
	}
	valuesCount := len(batch.Values)
	if queryCount != valuesCount {
		return -1, errors.New(fmt.Sprintf("batch contains %d queries but %d value lists", queryCount, valuesCount))
	}
	length += primitives.LengthOfByte  // type
	length += primitives.LengthOfShort // number of queries
	for i, query := range batch.QueriesOrIds {
		length += primitives.LengthOfByte
		switch stringOrId := query.(type) {
		case string:
			length += primitives.LengthOfLongString(stringOrId)
		case []byte:
			length += primitives.LengthOfShortBytes(stringOrId)
		default:
			return -1, fmt.Errorf("unsupported BATCH child type: %T", stringOrId)
		}
		if valuesLength, err := primitives.LengthOfPositionalValues(batch.Values[i]); err != nil {
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

func (c *BatchCodec) Decode(source []byte, version cassandraprotocol.ProtocolVersion) (msg Message, err error) {
	var batchType cassandraprotocol.BatchType
	if batchType, source, err = primitives.ReadByte(source); err != nil {
		return nil, fmt.Errorf("cannot read BATCH type: %w", err)
	}
	var queryCount uint16
	if queryCount, source, err = primitives.ReadShort(source); err != nil {
		return nil, fmt.Errorf("cannot read BATCH query count: %w", err)
	}
	queriesOrIds := make([]interface{}, queryCount)
	values := make([][]*cassandraprotocol.Value, queryCount)
	for i := 0; i < int(queryCount); i++ {
		var kind uint8
		if kind, source, err = primitives.ReadByte(source); err != nil {
			return nil, fmt.Errorf("cannot read BATCH query kind: %w", err)
		} else {
			switch kind {
			case 0:
				var query string
				if query, source, err = primitives.ReadLongString(source); err != nil {
					return nil, fmt.Errorf("cannot read BATCH query for child #%d: %w", i, err)
				} else {
					queriesOrIds = append(queriesOrIds, query)
				}
			case 1:
				var preparedId []byte
				if preparedId, source, err = primitives.ReadShortBytes(source); err != nil {
					return nil, fmt.Errorf("cannot read BATCH query for child #%d: %w", i, err)
				} else {
					queriesOrIds = append(queriesOrIds, preparedId)
				}
			default:
				return nil, fmt.Errorf("unsupported BATCH child type: %v", kind)
			}
			var vs []*cassandraprotocol.Value
			if vs, source, err = primitives.ReadPositionalValues(source); err != nil {
				return nil, fmt.Errorf("cannot read BATCH query for child #%d: %w", i, err)
			}
			values[i] = vs
		}
	}
	var consistency cassandraprotocol.ConsistencyLevel
	if consistency, source, err = primitives.ReadShort(source); err != nil {
		return nil, fmt.Errorf("cannot read BATCH consistency: %w", err)
	}
	var flags cassandraprotocol.QueryFlag
	if version >= cassandraprotocol.ProtocolVersion5 {
		flags, source, err = primitives.ReadInt(source)
	} else {
		var f uint8
		f, source, err = primitives.ReadByte(source)
		flags = cassandraprotocol.QueryFlag(f)
	}
	if err != nil {
		return nil, fmt.Errorf("cannot read BATCH query flags: %w", err)
	}
	if flags&cassandraprotocol.QueryFlagValueNames > 0 {
		return nil, errors.New("cannot read BATCH with named values, seevCASSANDRA-10246")
	}
	var serialConsistency cassandraprotocol.ConsistencyLevel
	if flags&cassandraprotocol.QueryFlagSerialConsistency > 0 {
		if serialConsistency, source, err = primitives.ReadShort(source); err != nil {
			return nil, fmt.Errorf("cannot read BATCH serial consistency: %w", err)
		}
	}
	var defaultTimestamp = DefaultTimestampNone
	if flags&cassandraprotocol.QueryFlagDefaultTimestamp > 0 {
		if defaultTimestamp, source, err = primitives.ReadLong(source); err != nil {
			return nil, fmt.Errorf("cannot read BATCH default timestamp: %w", err)
		}
	}
	var keyspace string
	if flags&cassandraprotocol.QueryFlagWithKeyspace > 0 {
		if keyspace, source, err = primitives.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read BATCH keyspace: %w", err)
		}
	}
	var nowInSeconds = NowInSecondsNone
	if flags&cassandraprotocol.QueryFlagNowInSeconds > 0 {
		if keyspace, source, err = primitives.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read BATCH now-in-seconds: %w", err)
		}
	}
	return &Batch{
		Type:              batchType,
		QueriesOrIds:      queriesOrIds,
		Values:            values,
		Flags:             flags,
		Consistency:       consistency,
		SerialConsistency: serialConsistency,
		DefaultTimestamp:  defaultTimestamp,
		Keyspace:          keyspace,
		NowInSeconds:      nowInSeconds,
	}, nil
}

func (c *BatchCodec) GetOpCode() cassandraprotocol.OpCode {
	return cassandraprotocol.OpCodeBatch
}
