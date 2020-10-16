package message

import (
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"math"
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
