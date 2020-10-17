package message

import (
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
	"math"
)

var QueryOptionsDefault = NewQueryOptions(
	cassandraprotocol.ConsistencyLevelOne,
	cassandraprotocol.ConsistencyLevelSerial,
	nil,
	nil,
	false,
	-1,
	nil,
	DefaultTimestampNone,
	"",
	NowInSecondsNone,
)

const DefaultTimestampNone int64 = math.MinInt64

const NowInSecondsNone int32 = math.MinInt32

type QueryOptions struct {
	Flags cassandraprotocol.QueryFlag

	Consistency       cassandraprotocol.ConsistencyLevel
	SerialConsistency cassandraprotocol.ConsistencyLevel

	PositionalValues []*cassandraprotocol.Value
	NamedValues      map[string]*cassandraprotocol.Value

	SkipMetadata bool
	PageSize     int32
	PagingState  []byte

	DefaultTimestamp int64
	Keyspace         string

	NowInSeconds int32
}

func NewQueryOptions(
	consistency cassandraprotocol.ConsistencyLevel,
	serialConsistency cassandraprotocol.ConsistencyLevel,
	positionalValues []*cassandraprotocol.Value,
	namedValues map[string]*cassandraprotocol.Value,
	skipMetadata bool,
	pageSize int32,
	pagingState []byte,
	defaultTimestamp int64,
	keyspace string,
	nowInSeconds int32,
) *QueryOptions {
	return &QueryOptions{
		Flags: computeQueryFlags(
			positionalValues,
			namedValues,
			skipMetadata,
			pageSize,
			pagingState,
			serialConsistency,
			defaultTimestamp,
			keyspace,
			nowInSeconds),
		Consistency:       consistency,
		SerialConsistency: serialConsistency,
		PositionalValues:  positionalValues,
		NamedValues:       namedValues,
		SkipMetadata:      skipMetadata,
		PageSize:          pageSize,
		PagingState:       pagingState,
		DefaultTimestamp:  defaultTimestamp,
		Keyspace:          keyspace,
		NowInSeconds:      nowInSeconds,
	}
}

func (o *QueryOptions) String() string {
	return fmt.Sprintf(
		"[cl=%v, positionalVals=%v, namedVals=%v, skip=%v, psize=%v, state=%v, serialCl=%v]",
		o.Consistency,
		o.PositionalValues,
		o.NamedValues,
		o.SkipMetadata,
		o.PageSize,
		o.PagingState,
		o.SerialConsistency)
}

func computeQueryFlags(
	positionalValues []*cassandraprotocol.Value,
	namedValues map[string]*cassandraprotocol.Value,
	skipMetadata bool,
	pageSize int32,
	pagingState []byte,
	serialConsistency cassandraprotocol.ConsistencyLevel,
	defaultTimestamp int64,
	keyspace string,
	nowInSeconds int32) cassandraprotocol.QueryFlag {
	var flags cassandraprotocol.QueryFlag = 0
	if positionalValues != nil {
		flags |= cassandraprotocol.QueryFlagValues
	}
	if namedValues != nil {
		flags |= cassandraprotocol.QueryFlagValues
		flags |= cassandraprotocol.QueryFlagValueNames
	}
	if skipMetadata {
		flags |= cassandraprotocol.QueryFlagSkipMetadata
	}
	if pageSize > 0 {
		flags |= cassandraprotocol.QueryFlagPageSize
	}
	if pagingState != nil {
		flags |= cassandraprotocol.QueryFlagPagingState
	}
	if serialConsistency != cassandraprotocol.ConsistencyLevelSerial {
		flags |= cassandraprotocol.QueryFlagSerialConsistency
	}
	if defaultTimestamp != DefaultTimestampNone {
		flags |= cassandraprotocol.QueryFlagDefaultTimestamp
	}
	if keyspace != "" {
		flags |= cassandraprotocol.QueryFlagWithKeyspace
	}
	if nowInSeconds != NowInSecondsNone {
		flags |= cassandraprotocol.QueryFlagNowInSeconds
	}
	return flags
}

func EncodeQueryOptions(options *QueryOptions, dest []byte, version cassandraprotocol.ProtocolVersion) (remaining []byte, err error) {
	dest, err = primitives.WriteShort(options.Consistency, dest)
	if err != nil {
		return dest, fmt.Errorf("cannot write query consistency: %w", err)
	}
	if version >= cassandraprotocol.ProtocolVersion5 {
		dest, err = primitives.WriteInt(options.Flags, dest)
		if err != nil {
			return dest, fmt.Errorf("cannot write query flags: %w", err)
		}
	} else {
		dest, err = primitives.WriteByte(uint8(options.Flags), dest)
		if err != nil {
			return dest, fmt.Errorf("cannot write query flags: %w", err)
		}
	}
	if options.Flags&cassandraprotocol.QueryFlagValues != 0 {
		if options.Flags&cassandraprotocol.QueryFlagValueNames != 0 {
			dest, err = primitives.WriteNamedValues(options.NamedValues, dest)
			if err != nil {
				return dest, fmt.Errorf("cannot write named [value]s: %w", err)
			}
		} else {
			dest, err = primitives.WritePositionalValues(options.PositionalValues, dest)
			if err != nil {
				return dest, fmt.Errorf("cannot write positional [value]s: %w", err)
			}
		}
	}
	if options.Flags&cassandraprotocol.QueryFlagPageSize != 0 {
		dest, err = primitives.WriteInt(options.PageSize, dest)
		if err != nil {
			return dest, fmt.Errorf("cannot write query page size: %w", err)
		}
	}
	if options.Flags&cassandraprotocol.QueryFlagPagingState != 0 {
		dest, err = primitives.WriteBytes(options.PagingState, dest)
		if err != nil {
			return dest, fmt.Errorf("cannot write query paging state: %w", err)
		}
	}
	if options.Flags&cassandraprotocol.QueryFlagSerialConsistency != 0 {
		dest, err = primitives.WriteShort(options.SerialConsistency, dest)
		if err != nil {
			return dest, fmt.Errorf("cannot write query serial consistency: %w", err)
		}
	}
	if options.Flags&cassandraprotocol.QueryFlagDefaultTimestamp != 0 {
		dest, err = primitives.WriteLong(options.DefaultTimestamp, dest)
		if err != nil {
			return dest, fmt.Errorf("cannot write query default timestamp: %w", err)
		}
	}
	if options.Flags&cassandraprotocol.QueryFlagWithKeyspace != 0 {
		dest, err = primitives.WriteString(options.Keyspace, dest)
		if err != nil {
			return dest, fmt.Errorf("cannot write query keyspace: %w", err)
		}
	}
	if options.Flags&cassandraprotocol.QueryFlagNowInSeconds != 0 {
		dest, err = primitives.WriteInt(options.NowInSeconds, dest)
		if err != nil {
			return dest, fmt.Errorf("cannot write query now-in-seconds: %w", err)
		}
	}
	return dest, nil
}

func LengthOfQueryOptions(options *QueryOptions, version cassandraprotocol.ProtocolVersion) (size int, err error) {
	size += primitives.LengthOfShort // consistency level
	if version >= cassandraprotocol.ProtocolVersion5 {
		size += primitives.LengthOfInt
	} else {
		size += primitives.LengthOfByte
	}
	var s int
	if options.Flags&cassandraprotocol.QueryFlagValues != 0 {
		if options.Flags&cassandraprotocol.QueryFlagValueNames != 0 {
			s, err = primitives.LengthOfNamedValues(options.NamedValues)
		} else {
			s, err = primitives.LengthOfPositionalValues(options.PositionalValues)
		}
	}
	if err != nil {
		return -1, fmt.Errorf("cannot compute size of query options: %w", err)
	}
	size += s
	if options.Flags&cassandraprotocol.QueryFlagPageSize != 0 {
		size += primitives.LengthOfInt
	}
	if options.Flags&cassandraprotocol.QueryFlagPagingState != 0 {
		size += primitives.LengthOfBytes(options.PagingState)
	}
	if options.Flags&cassandraprotocol.QueryFlagSerialConsistency != 0 {
		size += primitives.LengthOfShort
	}
	if options.Flags&cassandraprotocol.QueryFlagDefaultTimestamp != 0 {
		size += primitives.LengthOfLong
	}
	if options.Flags&cassandraprotocol.QueryFlagWithKeyspace != 0 {
		size += primitives.LengthOfString(options.Keyspace)
	}
	if options.Flags&cassandraprotocol.QueryFlagNowInSeconds != 0 {
		size += primitives.LengthOfInt
	}
	return
}

func DecodeQueryOptions(source []byte, version cassandraprotocol.ProtocolVersion) (*QueryOptions, []byte, error) {
	var consistency cassandraprotocol.ConsistencyLevel
	var err error
	consistency, source, err = primitives.ReadShort(source)
	if err != nil {
		return nil, source, fmt.Errorf("cannot read query consistency: %w", err)
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
		return nil, source, fmt.Errorf("cannot read query flags: %w", err)
	}
	var positionalValues []*cassandraprotocol.Value
	var namedValues map[string]*cassandraprotocol.Value
	if flags&cassandraprotocol.QueryFlagValues != 0 {
		if flags&cassandraprotocol.QueryFlagValueNames != 0 {
			namedValues, source, err = primitives.ReadNamedValues(source)
		} else {
			positionalValues, source, err = primitives.ReadPositionalValues(source)
		}
	}
	if err != nil {
		return nil, source, fmt.Errorf("cannot read [value]s: %w", err)
	}
	skipMetadata := flags&cassandraprotocol.QueryFlagSkipMetadata != 0
	var pageSize int32 = -1
	if flags&cassandraprotocol.QueryFlagPageSize != 0 {
		flags, source, err = primitives.ReadInt(source)
		if err != nil {
			return nil, source, fmt.Errorf("cannot read query page size: %w", err)
		}
	}
	var pagingState []byte
	if flags&cassandraprotocol.QueryFlagPagingState != 0 {
		pagingState, source, err = primitives.ReadBytes(source)
		if err != nil {
			return nil, source, fmt.Errorf("cannot read query paging state: %w", err)
		}
	}
	var serialConsistency = cassandraprotocol.ConsistencyLevelSerial
	if flags&cassandraprotocol.QueryFlagSerialConsistency != 0 {
		serialConsistency, source, err = primitives.ReadShort(source)
		if err != nil {
			return nil, source, fmt.Errorf("cannot read query serial consistency: %w", err)
		}
	}
	var defaultTimestamp = DefaultTimestampNone
	if flags&cassandraprotocol.QueryFlagDefaultTimestamp != 0 {
		defaultTimestamp, source, err = primitives.ReadLong(source)
		if err != nil {
			return nil, source, fmt.Errorf("cannot read query default timestamp: %w", err)
		}
	}
	var keyspace = ""
	if flags&cassandraprotocol.QueryFlagWithKeyspace != 0 {
		keyspace, source, err = primitives.ReadString(source)
		if err != nil {
			return nil, source, fmt.Errorf("cannot read query keyspace: %w", err)
		}
	}
	var nowInSeconds = NowInSecondsNone
	if flags&cassandraprotocol.QueryFlagNowInSeconds != 0 {
		nowInSeconds, source, err = primitives.ReadInt(source)
		if err != nil {
			return nil, source, fmt.Errorf("cannot read query now-in-seconds: %w", err)
		}
	}
	return &QueryOptions{
		Flags:             flags,
		Consistency:       consistency,
		SerialConsistency: serialConsistency,
		PositionalValues:  positionalValues,
		NamedValues:       namedValues,
		SkipMetadata:      skipMetadata,
		PageSize:          pageSize,
		PagingState:       pagingState,
		DefaultTimestamp:  defaultTimestamp,
		Keyspace:          keyspace,
		NowInSeconds:      nowInSeconds}, source, nil
}
