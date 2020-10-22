package message

import (
	"errors"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
	"io"
	"math"
)

type QueryOptionsCustomizer func(*QueryOptions)

func NewQueryOptions(customizers ...QueryOptionsCustomizer) *QueryOptions {
	options := &QueryOptions{
		Consistency:       cassandraprotocol.ConsistencyLevelOne,
		PageSize:          pageSizeNone,
		SerialConsistency: cassandraprotocol.ConsistencyLevelSerial,
		DefaultTimestamp:  DefaultTimestampNone,
		NowInSeconds:      NowInSecondsNone,
	}
	for _, customizer := range customizers {
		customizer(options)
	}
	return options
}

func WithPositionalValues(values ...*primitives.Value) QueryOptionsCustomizer {
	return func(options *QueryOptions) {
		options.PositionalValues = values
	}
}

func WithNamedValues(values map[string]*primitives.Value) QueryOptionsCustomizer {
	return func(options *QueryOptions) {
		options.NamedValues = values
	}
}

func WithConsistencyLevel(consistency cassandraprotocol.ConsistencyLevel) QueryOptionsCustomizer {
	return func(options *QueryOptions) {
		options.Consistency = consistency
	}
}

func WithSerialConsistencyLevel(consistency cassandraprotocol.ConsistencyLevel) QueryOptionsCustomizer {
	return func(options *QueryOptions) {
		options.SerialConsistency = consistency
	}
}

func SkipMetadata() QueryOptionsCustomizer {
	return func(options *QueryOptions) {
		options.SkipMetadata = true
	}
}

func WithPageSize(pageSize int32) QueryOptionsCustomizer {
	return func(options *QueryOptions) {
		options.PageSize = pageSize
	}
}

func WithPagingState(pagingState []byte) QueryOptionsCustomizer {
	return func(options *QueryOptions) {
		options.PagingState = pagingState
	}
}

func WithDefaultTimestamp(defaultTimestamp int64) QueryOptionsCustomizer {
	return func(options *QueryOptions) {
		options.DefaultTimestamp = defaultTimestamp
	}
}

func WithKeyspace(keyspace string) QueryOptionsCustomizer {
	return func(options *QueryOptions) {
		options.Keyspace = keyspace
	}
}

func WithNowInSeconds(nowInSeconds int32) QueryOptionsCustomizer {
	return func(options *QueryOptions) {
		options.NowInSeconds = nowInSeconds
	}
}

const (
	pageSizeNone         int32 = -1
	DefaultTimestampNone int64 = math.MinInt64
	NowInSecondsNone     int32 = math.MinInt32
)

type QueryOptions struct {
	Consistency       cassandraprotocol.ConsistencyLevel
	PositionalValues  []*primitives.Value
	NamedValues       map[string]*primitives.Value
	SkipMetadata      bool
	PageSize          int32
	PagingState       []byte
	SerialConsistency cassandraprotocol.ConsistencyLevel
	DefaultTimestamp  int64
	Keyspace          string
	NowInSeconds      int32
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

func (o *QueryOptions) Flags() cassandraprotocol.QueryFlag {
	var flags cassandraprotocol.QueryFlag
	if o.PositionalValues != nil {
		flags |= cassandraprotocol.QueryFlagValues
	}
	if o.NamedValues != nil {
		flags |= cassandraprotocol.QueryFlagValues
		flags |= cassandraprotocol.QueryFlagValueNames
	}
	if o.SkipMetadata {
		flags |= cassandraprotocol.QueryFlagSkipMetadata
	}
	if o.PageSize > 0 {
		flags |= cassandraprotocol.QueryFlagPageSize
	}
	if o.PagingState != nil {
		flags |= cassandraprotocol.QueryFlagPagingState
	}
	if o.SerialConsistency != cassandraprotocol.ConsistencyLevelSerial {
		flags |= cassandraprotocol.QueryFlagSerialConsistency
	}
	if o.DefaultTimestamp != DefaultTimestampNone {
		flags |= cassandraprotocol.QueryFlagDefaultTimestamp
	}
	if o.Keyspace != "" {
		flags |= cassandraprotocol.QueryFlagWithKeyspace
	}
	if o.NowInSeconds != NowInSecondsNone {
		flags |= cassandraprotocol.QueryFlagNowInSeconds
	}
	return flags
}

func EncodeQueryOptions(options *QueryOptions, dest io.Writer, version cassandraprotocol.ProtocolVersion) (err error) {
	if err := cassandraprotocol.CheckNonSerialConsistencyLevel(options.Consistency); err != nil {
		return err
	} else if err = primitives.WriteShort(options.Consistency, dest); err != nil {
		return fmt.Errorf("cannot write consistency: %w", err)
	}
	flags := options.Flags()
	if version >= cassandraprotocol.ProtocolVersion5 {
		if err = primitives.WriteInt(flags, dest); err != nil {
			return fmt.Errorf("cannot write flags: %w", err)
		}
	} else {
		if err = primitives.WriteByte(uint8(flags), dest); err != nil {
			return fmt.Errorf("cannot write flags: %w", err)
		}
	}
	if flags&cassandraprotocol.QueryFlagValues != 0 {
		if flags&cassandraprotocol.QueryFlagValueNames != 0 {
			if err = primitives.WriteNamedValues(options.NamedValues, dest, version); err != nil {
				return fmt.Errorf("cannot write named [value]s: %w", err)
			}
		} else {
			if err = primitives.WritePositionalValues(options.PositionalValues, dest, version); err != nil {
				return fmt.Errorf("cannot write positional [value]s: %w", err)
			}
		}
	}
	if flags&cassandraprotocol.QueryFlagPageSize != 0 {
		if err = primitives.WriteInt(options.PageSize, dest); err != nil {
			return fmt.Errorf("cannot write page size: %w", err)
		}
	}
	if flags&cassandraprotocol.QueryFlagPagingState != 0 {
		if err = primitives.WriteBytes(options.PagingState, dest); err != nil {
			return fmt.Errorf("cannot write paging state: %w", err)
		}
	}
	if flags&cassandraprotocol.QueryFlagSerialConsistency != 0 {
		if err := cassandraprotocol.CheckSerialConsistencyLevel(options.SerialConsistency); err != nil {
			return err
		} else if err = primitives.WriteShort(options.SerialConsistency, dest); err != nil {
			return fmt.Errorf("cannot write serial consistency: %w", err)
		}
	}
	if flags&cassandraprotocol.QueryFlagDefaultTimestamp != 0 {
		if err = primitives.WriteLong(options.DefaultTimestamp, dest); err != nil {
			return fmt.Errorf("cannot write default timestamp: %w", err)
		}
	}
	if flags&cassandraprotocol.QueryFlagWithKeyspace != 0 {
		if version < cassandraprotocol.ProtocolVersion5 {
			return fmt.Errorf("cannot set keyspace with protocol version: %v", version)
		} else if options.Keyspace == "" {
			return errors.New("cannot write empty keyspace")
		} else if err = primitives.WriteString(options.Keyspace, dest); err != nil {
			return fmt.Errorf("cannot write keyspace: %w", err)
		}
	}
	if flags&cassandraprotocol.QueryFlagNowInSeconds != 0 {
		if version < cassandraprotocol.ProtocolVersion5 {
			return fmt.Errorf("cannot set now-in-seconds with protocol version: %v", version)
		} else if err = primitives.WriteInt(options.NowInSeconds, dest); err != nil {
			return fmt.Errorf("cannot write now-in-seconds: %w", err)
		}
	}
	return nil
}

func LengthOfQueryOptions(options *QueryOptions, version cassandraprotocol.ProtocolVersion) (size int, err error) {
	size += primitives.LengthOfShort // consistency level
	if version >= cassandraprotocol.ProtocolVersion5 {
		size += primitives.LengthOfInt
	} else {
		size += primitives.LengthOfByte
	}
	var s int
	flags := options.Flags()
	if flags&cassandraprotocol.QueryFlagValues != 0 {
		if flags&cassandraprotocol.QueryFlagValueNames != 0 {
			s, err = primitives.LengthOfNamedValues(options.NamedValues)
		} else {
			s, err = primitives.LengthOfPositionalValues(options.PositionalValues)
		}
	}
	if err != nil {
		return -1, fmt.Errorf("cannot compute size of options: %w", err)
	}
	size += s
	if flags&cassandraprotocol.QueryFlagPageSize != 0 {
		size += primitives.LengthOfInt
	}
	if flags&cassandraprotocol.QueryFlagPagingState != 0 {
		size += primitives.LengthOfBytes(options.PagingState)
	}
	if flags&cassandraprotocol.QueryFlagSerialConsistency != 0 {
		size += primitives.LengthOfShort
	}
	if flags&cassandraprotocol.QueryFlagDefaultTimestamp != 0 {
		size += primitives.LengthOfLong
	}
	if flags&cassandraprotocol.QueryFlagWithKeyspace != 0 {
		size += primitives.LengthOfString(options.Keyspace)
	}
	if flags&cassandraprotocol.QueryFlagNowInSeconds != 0 {
		size += primitives.LengthOfInt
	}
	return
}

func DecodeQueryOptions(source io.Reader, version cassandraprotocol.ProtocolVersion) (options *QueryOptions, err error) {
	options = NewQueryOptions()
	if options.Consistency, err = primitives.ReadShort(source); err != nil {
		return nil, fmt.Errorf("cannot read consistency: %w", err)
	} else if err = cassandraprotocol.CheckConsistencyLevel(options.Consistency); err != nil {
		return nil, err
	}
	var flags cassandraprotocol.QueryFlag
	if version >= cassandraprotocol.ProtocolVersion5 {
		flags, err = primitives.ReadInt(source)
	} else {
		var f uint8
		f, err = primitives.ReadByte(source)
		flags = cassandraprotocol.QueryFlag(f)
	}
	if err != nil {
		return nil, fmt.Errorf("cannot read flags: %w", err)
	}
	if flags&cassandraprotocol.QueryFlagValues != 0 {
		if flags&cassandraprotocol.QueryFlagValueNames != 0 {
			options.NamedValues, err = primitives.ReadNamedValues(source, version)
		} else {
			options.PositionalValues, err = primitives.ReadPositionalValues(source, version)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("cannot read [value]s: %w", err)
	}
	options.SkipMetadata = flags&cassandraprotocol.QueryFlagSkipMetadata != 0
	if flags&cassandraprotocol.QueryFlagPageSize != 0 {
		if options.PageSize, err = primitives.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read page size: %w", err)
		}
	}
	if flags&cassandraprotocol.QueryFlagPagingState != 0 {
		if options.PagingState, err = primitives.ReadBytes(source); err != nil {
			return nil, fmt.Errorf("cannot read paging state: %w", err)
		}
	}
	if flags&cassandraprotocol.QueryFlagSerialConsistency != 0 {
		if options.SerialConsistency, err = primitives.ReadShort(source); err != nil {
			return nil, fmt.Errorf("cannot read serial consistency: %w", err)
		} else if err = cassandraprotocol.CheckConsistencyLevel(options.SerialConsistency); err != nil {
			return nil, err
		}
	}
	if flags&cassandraprotocol.QueryFlagDefaultTimestamp != 0 {
		if options.DefaultTimestamp, err = primitives.ReadLong(source); err != nil {
			return nil, fmt.Errorf("cannot read default timestamp: %w", err)
		}
	}
	if flags&cassandraprotocol.QueryFlagWithKeyspace != 0 {
		if options.Keyspace, err = primitives.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read keyspace: %w", err)
		}
	}
	if flags&cassandraprotocol.QueryFlagNowInSeconds != 0 {
		if options.NowInSeconds, err = primitives.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read now-in-seconds: %w", err)
		}
	}
	return options, nil
}
