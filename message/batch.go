// Copyright 2020 DataStax
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
		flags = flags.Add(primitive.QueryFlagSerialConsistency)
	}
	if m.DefaultTimestamp != nil {
		flags = flags.Add(primitive.QueryFlagDefaultTimestamp)
	}
	if m.Keyspace != "" {
		flags = flags.Add(primitive.QueryFlagWithKeyspace)
	}
	if m.NowInSeconds != nil {
		flags = flags.Add(primitive.QueryFlagNowInSeconds)
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
	if err = primitive.CheckValidBatchType(batch.Type); err != nil {
		return err
	} else if err = primitive.WriteByte(uint8(batch.Type), dest); err != nil {
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
			if err = primitive.WriteByte(uint8(primitive.BatchChildTypeQueryString), dest); err != nil {
				return fmt.Errorf("cannot write BATCH query kind 0 for child #%d: %w", i, err)
			} else if queryOrId == "" {
				return fmt.Errorf("cannot write empty BATCH query string for child #%d", i)
			} else if err = primitive.WriteLongString(queryOrId, dest); err != nil {
				return fmt.Errorf("cannot write BATCH query string for child #%d: %w", i, err)
			}
		case []byte:
			if err = primitive.WriteByte(uint8(primitive.BatchChildTypePreparedId), dest); err != nil {
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
	if err = primitive.WriteShort(uint16(batch.Consistency), dest); err != nil {
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
	if flags.Contains(primitive.QueryFlagSerialConsistency) {
		if err = primitive.WriteShort(uint16(batch.SerialConsistency.Value), dest); err != nil {
			return fmt.Errorf("cannot write BATCH serial consistency: %w", err)
		}
	}
	if flags.Contains(primitive.QueryFlagDefaultTimestamp) {
		if err = primitive.WriteLong(batch.DefaultTimestamp.Value, dest); err != nil {
			return fmt.Errorf("cannot write BATCH default timestamp: %w", err)
		}
	}
	if flags.Contains(primitive.QueryFlagWithKeyspace) {
		if batch.Keyspace == "" {
			return errors.New("cannot write BATCH empty keyspace")
		} else if err = primitive.WriteString(batch.Keyspace, dest); err != nil {
			return fmt.Errorf("cannot write BATCH keyspace: %w", err)
		}
	}
	if flags.Contains(primitive.QueryFlagNowInSeconds) {
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
	if flags.Contains(primitive.QueryFlagSerialConsistency) {
		length += primitive.LengthOfShort
	}
	if flags.Contains(primitive.QueryFlagDefaultTimestamp) {
		length += primitive.LengthOfLong
	}
	if flags.Contains(primitive.QueryFlagWithKeyspace) {
		length += primitive.LengthOfString(batch.Keyspace)
	}
	if flags.Contains(primitive.QueryFlagNowInSeconds) {
		length += primitive.LengthOfInt
	}
	return length, nil
}

func (c *batchCodec) Decode(source io.Reader, version primitive.ProtocolVersion) (msg Message, err error) {
	batch := &Batch{}
	var batchType uint8
	if batchType, err = primitive.ReadByte(source); err != nil {
		return nil, fmt.Errorf("cannot read BATCH type: %w", err)
	}
	batch.Type = primitive.BatchType(batchType)
	if err = primitive.CheckValidBatchType(batch.Type); err != nil {
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
		switch primitive.BatchChildType(childType) {
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
	var batchConsistency uint16
	if batchConsistency, err = primitive.ReadShort(source); err != nil {
		return nil, fmt.Errorf("cannot read BATCH consistency: %w", err)
	}
	batch.Consistency = primitive.ConsistencyLevel(batchConsistency)
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
	if flags.Contains(primitive.QueryFlagValueNames) {
		return nil, errors.New("cannot use BATCH with named values, see CASSANDRA-10246")
	}
	if flags.Contains(primitive.QueryFlagSerialConsistency) {
		batch.SerialConsistency = &primitive.NillableConsistencyLevel{}
		var batchSerialConsistency uint16
		if batchSerialConsistency, err = primitive.ReadShort(source); err != nil {
			return nil, fmt.Errorf("cannot read BATCH serial consistency: %w", err)
		}
		batch.SerialConsistency.Value = primitive.ConsistencyLevel(batchSerialConsistency)
	}
	if flags.Contains(primitive.QueryFlagDefaultTimestamp) {
		batch.DefaultTimestamp = &primitive.NillableInt64{}
		if batch.DefaultTimestamp.Value, err = primitive.ReadLong(source); err != nil {
			return nil, fmt.Errorf("cannot read BATCH default timestamp: %w", err)
		}
	}
	if version >= primitive.ProtocolVersion5 {
		if flags.Contains(primitive.QueryFlagWithKeyspace) {
			if batch.Keyspace, err = primitive.ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read BATCH keyspace: %w", err)
			}
		}
		if flags.Contains(primitive.QueryFlagNowInSeconds) {
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
