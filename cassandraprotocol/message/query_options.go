package message

import (
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitives"
	"io"
	"math"
)

type QueryOptionsCustomizer func(*QueryOptions)

func NewQueryOptions(customizers ...QueryOptionsCustomizer) *QueryOptions {
	options := &QueryOptions{
		Consistency:       primitives.ConsistencyLevelOne,
		PageSize:          pageSizeNone,
		SerialConsistency: primitives.ConsistencyLevelSerial,
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

func WithConsistencyLevel(consistency primitives.ConsistencyLevel) QueryOptionsCustomizer {
	return func(options *QueryOptions) {
		options.Consistency = consistency
	}
}

func WithSerialConsistencyLevel(consistency primitives.ConsistencyLevel) QueryOptionsCustomizer {
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

func WithPageSizeInBytes() QueryOptionsCustomizer {
	return func(options *QueryOptions) {
		options.PageSizeInBytes = true
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

func WithContinuousPagingOptions(continuousPagingOptions *ContinuousPagingOptions) QueryOptionsCustomizer {
	return func(options *QueryOptions) {
		options.ContinuousPagingOptions = continuousPagingOptions
	}
}

const (
	pageSizeNone         int32 = -1
	DefaultTimestampNone int64 = math.MinInt64
	NowInSecondsNone     int32 = math.MinInt32
)

type QueryOptions struct {
	Consistency      primitives.ConsistencyLevel
	PositionalValues []*primitives.Value
	NamedValues      map[string]*primitives.Value
	SkipMetadata     bool
	PageSize         int32
	// Whether the page size is expressed in number of rows or number of bytes. Valid for DSE protocol versions only.
	PageSizeInBytes   bool
	PagingState       []byte
	SerialConsistency primitives.ConsistencyLevel
	DefaultTimestamp  int64
	// Valid for protocol version 5 and DSE protocol version 2 only.
	Keyspace     string
	NowInSeconds int32
	// Valid only for DSE protocol versions
	ContinuousPagingOptions *ContinuousPagingOptions
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

func (o *QueryOptions) Flags() primitives.QueryFlag {
	var flags primitives.QueryFlag
	if o.PositionalValues != nil {
		flags |= primitives.QueryFlagValues
	}
	if o.NamedValues != nil {
		flags |= primitives.QueryFlagValues
		flags |= primitives.QueryFlagValueNames
	}
	if o.SkipMetadata {
		flags |= primitives.QueryFlagSkipMetadata
	}
	if o.PageSize > 0 {
		flags |= primitives.QueryFlagPageSize
		if o.PageSizeInBytes {
			flags |= primitives.QueryFlagDsePageSizeBytes
		}
	}
	if o.PagingState != nil {
		flags |= primitives.QueryFlagPagingState
	}
	if o.SerialConsistency != primitives.ConsistencyLevelSerial {
		flags |= primitives.QueryFlagSerialConsistency
	}
	if o.DefaultTimestamp != DefaultTimestampNone {
		flags |= primitives.QueryFlagDefaultTimestamp
	}
	if o.Keyspace != "" {
		flags |= primitives.QueryFlagWithKeyspace
	}
	if o.NowInSeconds != NowInSecondsNone {
		flags |= primitives.QueryFlagNowInSeconds
	}
	if o.ContinuousPagingOptions != nil {
		flags |= primitives.QueryFlagDseWithContinuousPagingOptions
	}
	return flags
}

func EncodeQueryOptions(options *QueryOptions, dest io.Writer, version primitives.ProtocolVersion) (err error) {
	if err := primitives.CheckNonSerialConsistencyLevel(options.Consistency); err != nil {
		return err
	} else if err = primitives.WriteShort(options.Consistency, dest); err != nil {
		return fmt.Errorf("cannot write consistency: %w", err)
	}
	flags := options.Flags()
	if version >= primitives.ProtocolVersion5 {
		if err = primitives.WriteInt(int32(flags), dest); err != nil {
			return fmt.Errorf("cannot write flags: %w", err)
		}
	} else {
		if err = primitives.WriteByte(uint8(flags), dest); err != nil {
			return fmt.Errorf("cannot write flags: %w", err)
		}
	}
	if flags&primitives.QueryFlagValues != 0 {
		if flags&primitives.QueryFlagValueNames != 0 {
			if err = primitives.WriteNamedValues(options.NamedValues, dest, version); err != nil {
				return fmt.Errorf("cannot write named [value]s: %w", err)
			}
		} else {
			if err = primitives.WritePositionalValues(options.PositionalValues, dest, version); err != nil {
				return fmt.Errorf("cannot write positional [value]s: %w", err)
			}
		}
	}
	if flags&primitives.QueryFlagPageSize != 0 {
		if err = primitives.WriteInt(options.PageSize, dest); err != nil {
			return fmt.Errorf("cannot write page size: %w", err)
		}
	}
	if flags&primitives.QueryFlagPagingState != 0 {
		if err = primitives.WriteBytes(options.PagingState, dest); err != nil {
			return fmt.Errorf("cannot write paging state: %w", err)
		}
	}
	if flags&primitives.QueryFlagSerialConsistency != 0 {
		if err := primitives.CheckSerialConsistencyLevel(options.SerialConsistency); err != nil {
			return err
		} else if err = primitives.WriteShort(options.SerialConsistency, dest); err != nil {
			return fmt.Errorf("cannot write serial consistency: %w", err)
		}
	}
	if flags&primitives.QueryFlagDefaultTimestamp != 0 {
		if err = primitives.WriteLong(options.DefaultTimestamp, dest); err != nil {
			return fmt.Errorf("cannot write default timestamp: %w", err)
		}
	}
	if flags&primitives.QueryFlagWithKeyspace != 0 {
		if options.Keyspace == "" {
			return errors.New("cannot write empty keyspace")
		} else if err = primitives.WriteString(options.Keyspace, dest); err != nil {
			return fmt.Errorf("cannot write keyspace: %w", err)
		}
	}
	if flags&primitives.QueryFlagNowInSeconds != 0 {
		if err = primitives.WriteInt(options.NowInSeconds, dest); err != nil {
			return fmt.Errorf("cannot write now-in-seconds: %w", err)
		}
	}
	if flags&primitives.QueryFlagDseWithContinuousPagingOptions != 0 {
		if err = EncodeContinuousPagingOptions(options.ContinuousPagingOptions, dest, version); err != nil {
			return fmt.Errorf("cannot encode continuous paging options: %w", err)
		}
	}
	return nil
}

func LengthOfQueryOptions(options *QueryOptions, version primitives.ProtocolVersion) (length int, err error) {
	length += primitives.LengthOfShort // consistency level
	if version >= primitives.ProtocolVersion5 {
		length += primitives.LengthOfInt
	} else {
		length += primitives.LengthOfByte
	}
	var s int
	flags := options.Flags()
	if flags&primitives.QueryFlagValues != 0 {
		if flags&primitives.QueryFlagValueNames != 0 {
			s, err = primitives.LengthOfNamedValues(options.NamedValues)
		} else {
			s, err = primitives.LengthOfPositionalValues(options.PositionalValues)
		}
	}
	if err != nil {
		return -1, fmt.Errorf("cannot compute length of query options values: %w", err)
	}
	length += s
	if flags&primitives.QueryFlagPageSize != 0 {
		length += primitives.LengthOfInt
	}
	if flags&primitives.QueryFlagPagingState != 0 {
		length += primitives.LengthOfBytes(options.PagingState)
	}
	if flags&primitives.QueryFlagSerialConsistency != 0 {
		length += primitives.LengthOfShort
	}
	if flags&primitives.QueryFlagDefaultTimestamp != 0 {
		length += primitives.LengthOfLong
	}
	if flags&primitives.QueryFlagWithKeyspace != 0 {
		length += primitives.LengthOfString(options.Keyspace)
	}
	if flags&primitives.QueryFlagNowInSeconds != 0 {
		length += primitives.LengthOfInt
	}
	if flags&primitives.QueryFlagDseWithContinuousPagingOptions != 0 {
		if lengthOfContinuousPagingOptions, err := LengthOfContinuousPagingOptions(options.ContinuousPagingOptions, version); err != nil {
			return -1, fmt.Errorf("cannot compute length of continuous paging options: %w", err)
		} else {
			length += lengthOfContinuousPagingOptions
		}
	}
	return
}

func DecodeQueryOptions(source io.Reader, version primitives.ProtocolVersion) (options *QueryOptions, err error) {
	options = NewQueryOptions()
	if options.Consistency, err = primitives.ReadShort(source); err != nil {
		return nil, fmt.Errorf("cannot read consistency: %w", err)
	} else if err = primitives.CheckConsistencyLevel(options.Consistency); err != nil {
		return nil, err
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
		return nil, fmt.Errorf("cannot read flags: %w", err)
	}
	if flags&primitives.QueryFlagValues != 0 {
		if flags&primitives.QueryFlagValueNames != 0 {
			options.NamedValues, err = primitives.ReadNamedValues(source, version)
		} else {
			options.PositionalValues, err = primitives.ReadPositionalValues(source, version)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("cannot read [value]s: %w", err)
	}
	options.SkipMetadata = flags&primitives.QueryFlagSkipMetadata != 0
	if flags&primitives.QueryFlagPageSize != 0 {
		if options.PageSize, err = primitives.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read page size: %w", err)
		}
		if flags&primitives.QueryFlagDsePageSizeBytes != 0 {
			options.PageSizeInBytes = true
		}
	}
	if flags&primitives.QueryFlagPagingState != 0 {
		if options.PagingState, err = primitives.ReadBytes(source); err != nil {
			return nil, fmt.Errorf("cannot read paging state: %w", err)
		}
	}
	if flags&primitives.QueryFlagSerialConsistency != 0 {
		if options.SerialConsistency, err = primitives.ReadShort(source); err != nil {
			return nil, fmt.Errorf("cannot read serial consistency: %w", err)
		} else if err = primitives.CheckConsistencyLevel(options.SerialConsistency); err != nil {
			return nil, err
		}
	}
	if flags&primitives.QueryFlagDefaultTimestamp != 0 {
		if options.DefaultTimestamp, err = primitives.ReadLong(source); err != nil {
			return nil, fmt.Errorf("cannot read default timestamp: %w", err)
		}
	}
	if flags&primitives.QueryFlagWithKeyspace != 0 {
		if options.Keyspace, err = primitives.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read keyspace: %w", err)
		}
	}
	if flags&primitives.QueryFlagNowInSeconds != 0 {
		if options.NowInSeconds, err = primitives.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read now-in-seconds: %w", err)
		}
	}
	if flags&primitives.QueryFlagDseWithContinuousPagingOptions != 0 {
		if options.ContinuousPagingOptions, err = DecodeContinuousPagingOptions(source, version); err != nil {
			return nil, fmt.Errorf("cannot read continuous paging options: %w", err)
		}
	}
	return options, nil
}
