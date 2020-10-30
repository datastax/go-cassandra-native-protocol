package message

import (
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitive"
	"io"
	"math"
)

type QueryOptionsCustomizer func(*QueryOptions)

func NewQueryOptions(customizers ...QueryOptionsCustomizer) *QueryOptions {
	options := &QueryOptions{
		Consistency:       primitive.ConsistencyLevelOne,
		PageSize:          pageSizeNone,
		SerialConsistency: primitive.ConsistencyLevelSerial,
		DefaultTimestamp:  DefaultTimestampNone,
		NowInSeconds:      NowInSecondsNone,
	}
	for _, customizer := range customizers {
		customizer(options)
	}
	return options
}

func WithPositionalValues(values ...*primitive.Value) QueryOptionsCustomizer {
	return func(options *QueryOptions) {
		options.PositionalValues = values
	}
}

func WithNamedValues(values map[string]*primitive.Value) QueryOptionsCustomizer {
	return func(options *QueryOptions) {
		options.NamedValues = values
	}
}

func WithConsistencyLevel(consistency primitive.ConsistencyLevel) QueryOptionsCustomizer {
	return func(options *QueryOptions) {
		options.Consistency = consistency
	}
}

func WithSerialConsistencyLevel(consistency primitive.ConsistencyLevel) QueryOptionsCustomizer {
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
	Consistency      primitive.ConsistencyLevel
	PositionalValues []*primitive.Value
	NamedValues      map[string]*primitive.Value
	SkipMetadata     bool
	PageSize         int32
	// Whether the page size is expressed in number of rows or number of bytes. Valid for DSE protocol versions only.
	PageSizeInBytes   bool
	PagingState       []byte
	SerialConsistency primitive.ConsistencyLevel
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

func (o *QueryOptions) Flags() primitive.QueryFlag {
	var flags primitive.QueryFlag
	if o.PositionalValues != nil {
		flags |= primitive.QueryFlagValues
	}
	if o.NamedValues != nil {
		flags |= primitive.QueryFlagValues
		flags |= primitive.QueryFlagValueNames
	}
	if o.SkipMetadata {
		flags |= primitive.QueryFlagSkipMetadata
	}
	if o.PageSize > 0 {
		flags |= primitive.QueryFlagPageSize
		if o.PageSizeInBytes {
			flags |= primitive.QueryFlagDsePageSizeBytes
		}
	}
	if o.PagingState != nil {
		flags |= primitive.QueryFlagPagingState
	}
	if o.SerialConsistency != primitive.ConsistencyLevelSerial {
		flags |= primitive.QueryFlagSerialConsistency
	}
	if o.DefaultTimestamp != DefaultTimestampNone {
		flags |= primitive.QueryFlagDefaultTimestamp
	}
	if o.Keyspace != "" {
		flags |= primitive.QueryFlagWithKeyspace
	}
	if o.NowInSeconds != NowInSecondsNone {
		flags |= primitive.QueryFlagNowInSeconds
	}
	if o.ContinuousPagingOptions != nil {
		flags |= primitive.QueryFlagDseWithContinuousPagingOptions
	}
	return flags
}

func EncodeQueryOptions(options *QueryOptions, dest io.Writer, version primitive.ProtocolVersion) (err error) {
	if err := primitive.CheckNonSerialConsistencyLevel(options.Consistency); err != nil {
		return err
	} else if err = primitive.WriteShort(options.Consistency, dest); err != nil {
		return fmt.Errorf("cannot write consistency: %w", err)
	}
	flags := options.Flags()
	if version >= primitive.ProtocolVersion5 {
		if err = primitive.WriteInt(int32(flags), dest); err != nil {
			return fmt.Errorf("cannot write flags: %w", err)
		}
	} else {
		if err = primitive.WriteByte(uint8(flags), dest); err != nil {
			return fmt.Errorf("cannot write flags: %w", err)
		}
	}
	if flags&primitive.QueryFlagValues != 0 {
		if flags&primitive.QueryFlagValueNames != 0 {
			if err = primitive.WriteNamedValues(options.NamedValues, dest, version); err != nil {
				return fmt.Errorf("cannot write named [value]s: %w", err)
			}
		} else {
			if err = primitive.WritePositionalValues(options.PositionalValues, dest, version); err != nil {
				return fmt.Errorf("cannot write positional [value]s: %w", err)
			}
		}
	}
	if flags&primitive.QueryFlagPageSize != 0 {
		if err = primitive.WriteInt(options.PageSize, dest); err != nil {
			return fmt.Errorf("cannot write page size: %w", err)
		}
	}
	if flags&primitive.QueryFlagPagingState != 0 {
		if err = primitive.WriteBytes(options.PagingState, dest); err != nil {
			return fmt.Errorf("cannot write paging state: %w", err)
		}
	}
	if flags&primitive.QueryFlagSerialConsistency != 0 {
		if err := primitive.CheckSerialConsistencyLevel(options.SerialConsistency); err != nil {
			return err
		} else if err = primitive.WriteShort(options.SerialConsistency, dest); err != nil {
			return fmt.Errorf("cannot write serial consistency: %w", err)
		}
	}
	if flags&primitive.QueryFlagDefaultTimestamp != 0 {
		if err = primitive.WriteLong(options.DefaultTimestamp, dest); err != nil {
			return fmt.Errorf("cannot write default timestamp: %w", err)
		}
	}
	if flags&primitive.QueryFlagWithKeyspace != 0 {
		if options.Keyspace == "" {
			return errors.New("cannot write empty keyspace")
		} else if err = primitive.WriteString(options.Keyspace, dest); err != nil {
			return fmt.Errorf("cannot write keyspace: %w", err)
		}
	}
	if flags&primitive.QueryFlagNowInSeconds != 0 {
		if err = primitive.WriteInt(options.NowInSeconds, dest); err != nil {
			return fmt.Errorf("cannot write now-in-seconds: %w", err)
		}
	}
	if flags&primitive.QueryFlagDseWithContinuousPagingOptions != 0 {
		if err = EncodeContinuousPagingOptions(options.ContinuousPagingOptions, dest, version); err != nil {
			return fmt.Errorf("cannot encode continuous paging options: %w", err)
		}
	}
	return nil
}

func LengthOfQueryOptions(options *QueryOptions, version primitive.ProtocolVersion) (length int, err error) {
	length += primitive.LengthOfShort // consistency level
	if version >= primitive.ProtocolVersion5 {
		length += primitive.LengthOfInt
	} else {
		length += primitive.LengthOfByte
	}
	var s int
	flags := options.Flags()
	if flags&primitive.QueryFlagValues != 0 {
		if flags&primitive.QueryFlagValueNames != 0 {
			s, err = primitive.LengthOfNamedValues(options.NamedValues)
		} else {
			s, err = primitive.LengthOfPositionalValues(options.PositionalValues)
		}
	}
	if err != nil {
		return -1, fmt.Errorf("cannot compute length of query options values: %w", err)
	}
	length += s
	if flags&primitive.QueryFlagPageSize != 0 {
		length += primitive.LengthOfInt
	}
	if flags&primitive.QueryFlagPagingState != 0 {
		length += primitive.LengthOfBytes(options.PagingState)
	}
	if flags&primitive.QueryFlagSerialConsistency != 0 {
		length += primitive.LengthOfShort
	}
	if flags&primitive.QueryFlagDefaultTimestamp != 0 {
		length += primitive.LengthOfLong
	}
	if flags&primitive.QueryFlagWithKeyspace != 0 {
		length += primitive.LengthOfString(options.Keyspace)
	}
	if flags&primitive.QueryFlagNowInSeconds != 0 {
		length += primitive.LengthOfInt
	}
	if flags&primitive.QueryFlagDseWithContinuousPagingOptions != 0 {
		if lengthOfContinuousPagingOptions, err := LengthOfContinuousPagingOptions(options.ContinuousPagingOptions, version); err != nil {
			return -1, fmt.Errorf("cannot compute length of continuous paging options: %w", err)
		} else {
			length += lengthOfContinuousPagingOptions
		}
	}
	return
}

func DecodeQueryOptions(source io.Reader, version primitive.ProtocolVersion) (options *QueryOptions, err error) {
	options = NewQueryOptions()
	if options.Consistency, err = primitive.ReadShort(source); err != nil {
		return nil, fmt.Errorf("cannot read consistency: %w", err)
	} else if err = primitive.CheckConsistencyLevel(options.Consistency); err != nil {
		return nil, err
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
		return nil, fmt.Errorf("cannot read flags: %w", err)
	}
	if flags&primitive.QueryFlagValues != 0 {
		if flags&primitive.QueryFlagValueNames != 0 {
			options.NamedValues, err = primitive.ReadNamedValues(source, version)
		} else {
			options.PositionalValues, err = primitive.ReadPositionalValues(source, version)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("cannot read [value]s: %w", err)
	}
	options.SkipMetadata = flags&primitive.QueryFlagSkipMetadata != 0
	if flags&primitive.QueryFlagPageSize != 0 {
		if options.PageSize, err = primitive.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read page size: %w", err)
		}
		if flags&primitive.QueryFlagDsePageSizeBytes != 0 {
			options.PageSizeInBytes = true
		}
	}
	if flags&primitive.QueryFlagPagingState != 0 {
		if options.PagingState, err = primitive.ReadBytes(source); err != nil {
			return nil, fmt.Errorf("cannot read paging state: %w", err)
		}
	}
	if flags&primitive.QueryFlagSerialConsistency != 0 {
		if options.SerialConsistency, err = primitive.ReadShort(source); err != nil {
			return nil, fmt.Errorf("cannot read serial consistency: %w", err)
		} else if err = primitive.CheckConsistencyLevel(options.SerialConsistency); err != nil {
			return nil, err
		}
	}
	if flags&primitive.QueryFlagDefaultTimestamp != 0 {
		if options.DefaultTimestamp, err = primitive.ReadLong(source); err != nil {
			return nil, fmt.Errorf("cannot read default timestamp: %w", err)
		}
	}
	if flags&primitive.QueryFlagWithKeyspace != 0 {
		if options.Keyspace, err = primitive.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read keyspace: %w", err)
		}
	}
	if flags&primitive.QueryFlagNowInSeconds != 0 {
		if options.NowInSeconds, err = primitive.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read now-in-seconds: %w", err)
		}
	}
	if flags&primitive.QueryFlagDseWithContinuousPagingOptions != 0 {
		if options.ContinuousPagingOptions, err = DecodeContinuousPagingOptions(source, version); err != nil {
			return nil, fmt.Errorf("cannot read continuous paging options: %w", err)
		}
	}
	return options, nil
}
