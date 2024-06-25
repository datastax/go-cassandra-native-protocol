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
	"io"

	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

// Batch is a BATCH request message. The zero value is NOT a valid message; at least one batch child must be provided.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/message.Message
type Batch struct {
	// The batch type: LOGGED, UNLOGGED or COUNTER. This field is mandatory; its default is LOGGED, as the zero value
	// of primitive.BatchType is primitive.BatchTypeLogged. The LOGGED type is equivalent to a regular CQL3 batch
	// statement CREATE BATCH ... APPLY BATCH.
	Type primitive.BatchType
	// This batch children statements. At least one batch child must be provided.
	Children []*BatchChild
	// The consistency level to use to execute the batch statements. This field is mandatory; its default is ANY, as the
	// zero value of primitive.ConsistencyLevel is primitive.ConsistencyLevelAny.
	Consistency primitive.ConsistencyLevel
	// The (optional) serial consistency level to use when executing the query. Serial consistency is available
	// starting with protocol version 3.
	SerialConsistency *primitive.ConsistencyLevel
	// The default timestamp for the query in microseconds (negative values are discouraged but supported for
	// backward compatibility reasons except for the smallest negative value (-2^63) that is forbidden). If provided,
	// this will replace the server-side assigned timestamp as default timestamp. Note that a timestamp in the query
	// itself (that is, if the query has a USING TIMESTAMP clause) will still override this timestamp.
	// Default timestamps are valid for protocol versions 3 and higher.
	DefaultTimestamp *int64
	// The keyspace in which to execute queries, when the target table name is not qualified. Optional. Introduced in
	// Protocol Version 5, also present in DSE protocol v2.
	Keyspace string
	// Introduced in Protocol Version 5, not present in DSE protocol versions.
	NowInSeconds *int32
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

// Flags are the flags of this BATCH message. BATCH messages have flags starting with protocol version 3.
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
	// Note: the named values flag is in theory possible, but server-side implementation is
	// broken. See https://issues.apache.org/jira/browse/CASSANDRA-10246
	return flags
}

// BatchChild represents a BATCH child statement.
// +k8s:deepcopy-gen=true
type BatchChild struct {
	// The CQL statement to execute. Exactly one of Query or Id must be present, never both.
	Query string
	// The prepared id of the statement to execute. Exactly one of Query or Id must be present, never both.
	Id []byte
	// Note: named values are in theory possible, but their server-side implementation is
	// broken. See https://issues.apache.org/jira/browse/CASSANDRA-10246
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
	if childrenCount > 0xFFFF {
		return errors.New(fmt.Sprintf("BATCH messages can contain at most %d child queries", 0xFFFF))
	} else if err = primitive.WriteShort(uint16(childrenCount), dest); err != nil {
		return fmt.Errorf("cannot write BATCH query count: %w", err)
	}
	for i, child := range batch.Children {
		if child.Query != "" {
			if err = primitive.WriteByte(uint8(primitive.BatchChildTypeQueryString), dest); err != nil {
				return fmt.Errorf("cannot write BATCH query kind 0 for child #%d: %w", i, err)
			} else if child.Query == "" {
				return fmt.Errorf("cannot write empty BATCH query string for child #%d", i)
			} else if err = primitive.WriteLongString(child.Query, dest); err != nil {
				return fmt.Errorf("cannot write BATCH query string for child #%d: %w", i, err)
			}
		} else {
			if err = primitive.WriteByte(uint8(primitive.BatchChildTypePreparedId), dest); err != nil {
				return fmt.Errorf("cannot write BATCH query kind 1 for child #%d: %w", i, err)
			} else if len(child.Id) == 0 {
				return fmt.Errorf("cannot write empty BATCH query id for child #%d: %w", i, err)
			} else if err = primitive.WriteShortBytes(child.Id, dest); err != nil {
				return fmt.Errorf("cannot write BATCH query id for child #%d: %w", i, err)
			}
		}
		if err = primitive.WritePositionalValues(child.Values, dest, version); err != nil {
			return fmt.Errorf("cannot write BATCH positional values for child #%d: %w", i, err)
		}
	}
	if err = primitive.WriteShort(uint16(batch.Consistency), dest); err != nil {
		return fmt.Errorf("cannot write BATCH consistency: %w", err)
	}
	if version.SupportsBatchQueryFlags() {
		flags := batch.Flags()
		if version.Uses4BytesQueryFlags() {
			err = primitive.WriteInt(int32(flags), dest)
		} else {
			err = primitive.WriteByte(uint8(flags), dest)
		}
		if err != nil {
			return fmt.Errorf("cannot write BATCH query flags: %w", err)
		}
		if version.SupportsQueryFlag(primitive.QueryFlagSerialConsistency) && flags.Contains(primitive.QueryFlagSerialConsistency) {
			if err = primitive.WriteShort(uint16(*batch.SerialConsistency), dest); err != nil {
				return fmt.Errorf("cannot write BATCH serial consistency: %w", err)
			}
		}
		if version.SupportsQueryFlag(primitive.QueryFlagDefaultTimestamp) && flags.Contains(primitive.QueryFlagDefaultTimestamp) {
			if err = primitive.WriteLong(*batch.DefaultTimestamp, dest); err != nil {
				return fmt.Errorf("cannot write BATCH default timestamp: %w", err)
			}
		}
		if version.SupportsQueryFlag(primitive.QueryFlagWithKeyspace) && flags.Contains(primitive.QueryFlagWithKeyspace) {
			if batch.Keyspace == "" {
				return errors.New("cannot write BATCH empty keyspace")
			} else if err = primitive.WriteString(batch.Keyspace, dest); err != nil {
				return fmt.Errorf("cannot write BATCH keyspace: %w", err)
			}
		}
		if version.SupportsQueryFlag(primitive.QueryFlagNowInSeconds) && flags.Contains(primitive.QueryFlagNowInSeconds) {
			if err = primitive.WriteInt(*batch.NowInSeconds, dest); err != nil {
				return fmt.Errorf("cannot write BATCH now-in-seconds: %w", err)
			}
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
		if child.Query != "" {
			length += primitive.LengthOfLongString(child.Query)
		} else {
			length += primitive.LengthOfShortBytes(child.Id)
		}
		if valuesLength, err := primitive.LengthOfPositionalValues(child.Values); err != nil {
			return -1, fmt.Errorf("cannot compute length of BATCH positional values for child #%d: %w", i, err)
		} else {
			length += valuesLength
		}
	}
	length += primitive.LengthOfShort // consistency level
	// flags
	if version.SupportsBatchQueryFlags() {
		if version.Uses4BytesQueryFlags() {
			length += primitive.LengthOfInt
		} else {
			length += primitive.LengthOfByte
		}
		flags := batch.Flags()
		if version.SupportsQueryFlag(primitive.QueryFlagSerialConsistency) && flags.Contains(primitive.QueryFlagSerialConsistency) {
			length += primitive.LengthOfShort
		}
		if version.SupportsQueryFlag(primitive.QueryFlagDefaultTimestamp) && flags.Contains(primitive.QueryFlagDefaultTimestamp) {
			length += primitive.LengthOfLong
		}
		if version.SupportsQueryFlag(primitive.QueryFlagWithKeyspace) && flags.Contains(primitive.QueryFlagWithKeyspace) {
			length += primitive.LengthOfString(batch.Keyspace)
		}
		if version.SupportsQueryFlag(primitive.QueryFlagNowInSeconds) && flags.Contains(primitive.QueryFlagNowInSeconds) {
			length += primitive.LengthOfInt
		}
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
			if child.Query, err = primitive.ReadLongString(source); err != nil {
				return nil, fmt.Errorf("cannot read BATCH query string for child #%d: %w", i, err)
			}
		case primitive.BatchChildTypePreparedId:
			if child.Id, err = primitive.ReadShortBytes(source); err != nil {
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
	if version.SupportsBatchQueryFlags() {
		var flags primitive.QueryFlag
		if version.Uses4BytesQueryFlags() {
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
			var batchSerialConsistencyUint uint16
			if batchSerialConsistencyUint, err = primitive.ReadShort(source); err != nil {
				return nil, fmt.Errorf("cannot read BATCH serial consistency: %w", err)
			}
			batchSerialConsistency := primitive.ConsistencyLevel(batchSerialConsistencyUint)
			batch.SerialConsistency = &batchSerialConsistency
		}
		if flags.Contains(primitive.QueryFlagDefaultTimestamp) {
			var batchDefaultTimestamp int64
			if batchDefaultTimestamp, err = primitive.ReadLong(source); err != nil {
				return nil, fmt.Errorf("cannot read BATCH default timestamp: %w", err)
			}
			batch.DefaultTimestamp = &batchDefaultTimestamp
		}
		if version.SupportsQueryFlag(primitive.QueryFlagWithKeyspace) && flags.Contains(primitive.QueryFlagWithKeyspace) {
			if batch.Keyspace, err = primitive.ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read BATCH keyspace: %w", err)
			}
		}
		if version.SupportsQueryFlag(primitive.QueryFlagNowInSeconds) && flags.Contains(primitive.QueryFlagNowInSeconds) {
			var batchNowInSeconds int32
			if batchNowInSeconds, err = primitive.ReadInt(source); err != nil {
				return nil, fmt.Errorf("cannot read BATCH now-in-seconds: %w", err)
			}
			batch.NowInSeconds = &batchNowInSeconds
		}
	}
	return batch, nil
}

func (c *batchCodec) GetOpCode() primitive.OpCode {
	return primitive.OpCodeBatch
}
