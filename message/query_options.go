package message

import (
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"io"
)

type QueryOptions struct {
	Consistency      primitive.ConsistencyLevel
	PositionalValues []*primitive.Value
	NamedValues      map[string]*primitive.Value
	SkipMetadata     bool
	// The desired page size. A value of zero or negative is usually interpreted as "no pagination" and can result
	// in the entire result set being returned in one single page.
	PageSize int32
	// Whether the page size is expressed in number of rows or number of bytes. Valid for DSE protocol versions only.
	PageSizeInBytes   bool
	PagingState       []byte
	SerialConsistency *primitive.NillableConsistencyLevel
	DefaultTimestamp  *primitive.NillableInt64
	// Valid for protocol version 5 and DSE protocol version 2 only.
	Keyspace     string
	NowInSeconds *primitive.NillableInt32
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
		flags = flags.Add(primitive.QueryFlagValues)
	}
	if o.NamedValues != nil {
		flags = flags.Add(primitive.QueryFlagValues)
		flags = flags.Add(primitive.QueryFlagValueNames)
	}
	if o.SkipMetadata {
		flags = flags.Add(primitive.QueryFlagSkipMetadata)
	}
	if o.PageSize > 0 {
		flags = flags.Add(primitive.QueryFlagPageSize)
		if o.PageSizeInBytes {
			flags = flags.Add(primitive.QueryFlagDsePageSizeBytes)
		}
	}
	if o.PagingState != nil {
		flags = flags.Add(primitive.QueryFlagPagingState)
	}
	if o.SerialConsistency != nil {
		flags = flags.Add(primitive.QueryFlagSerialConsistency)
	}
	if o.DefaultTimestamp != nil {
		flags = flags.Add(primitive.QueryFlagDefaultTimestamp)
	}
	if o.Keyspace != "" {
		flags = flags.Add(primitive.QueryFlagWithKeyspace)
	}
	if o.NowInSeconds != nil {
		flags = flags.Add(primitive.QueryFlagNowInSeconds)
	}
	if o.ContinuousPagingOptions != nil {
		flags = flags.Add(primitive.QueryFlagDseWithContinuousPagingOptions)
	}
	return flags
}

func EncodeQueryOptions(options *QueryOptions, dest io.Writer, version primitive.ProtocolVersion) (err error) {
	if options == nil {
		options = &QueryOptions{} // use defaults if nil provided
	}
	if err := primitive.CheckValidNonSerialConsistencyLevel(options.Consistency); err != nil {
		return err
	} else if err = primitive.WriteShort(uint16(options.Consistency), dest); err != nil {
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
	if flags.Contains(primitive.QueryFlagValues) {
		if flags.Contains(primitive.QueryFlagValueNames) {
			if err = primitive.WriteNamedValues(options.NamedValues, dest, version); err != nil {
				return fmt.Errorf("cannot write named [value]s: %w", err)
			}
		} else {
			if err = primitive.WritePositionalValues(options.PositionalValues, dest, version); err != nil {
				return fmt.Errorf("cannot write positional [value]s: %w", err)
			}
		}
	}
	if flags.Contains(primitive.QueryFlagPageSize) {
		if err = primitive.WriteInt(options.PageSize, dest); err != nil {
			return fmt.Errorf("cannot write page size: %w", err)
		}
	}
	if flags.Contains(primitive.QueryFlagPagingState) {
		if err = primitive.WriteBytes(options.PagingState, dest); err != nil {
			return fmt.Errorf("cannot write paging state: %w", err)
		}
	}
	if flags.Contains(primitive.QueryFlagSerialConsistency) {
		if err := primitive.CheckValidSerialConsistencyLevel(options.SerialConsistency.Value); err != nil {
			return err
		} else if err = primitive.WriteShort(uint16(options.SerialConsistency.Value), dest); err != nil {
			return fmt.Errorf("cannot write serial consistency: %w", err)
		}
	}
	if flags.Contains(primitive.QueryFlagDefaultTimestamp) {
		if err = primitive.WriteLong(options.DefaultTimestamp.Value, dest); err != nil {
			return fmt.Errorf("cannot write default timestamp: %w", err)
		}
	}
	if flags.Contains(primitive.QueryFlagWithKeyspace) {
		if options.Keyspace == "" {
			return errors.New("cannot write empty keyspace")
		} else if err = primitive.WriteString(options.Keyspace, dest); err != nil {
			return fmt.Errorf("cannot write keyspace: %w", err)
		}
	}
	if flags.Contains(primitive.QueryFlagNowInSeconds) {
		if err = primitive.WriteInt(options.NowInSeconds.Value, dest); err != nil {
			return fmt.Errorf("cannot write now-in-seconds: %w", err)
		}
	}
	if flags.Contains(primitive.QueryFlagDseWithContinuousPagingOptions) {
		if err = EncodeContinuousPagingOptions(options.ContinuousPagingOptions, dest, version); err != nil {
			return fmt.Errorf("cannot encode continuous paging options: %w", err)
		}
	}
	return nil
}

func LengthOfQueryOptions(options *QueryOptions, version primitive.ProtocolVersion) (length int, err error) {
	if options == nil {
		options = &QueryOptions{} // use defaults if nil provided
	}
	length += primitive.LengthOfShort // consistency level
	if version >= primitive.ProtocolVersion5 {
		length += primitive.LengthOfInt
	} else {
		length += primitive.LengthOfByte
	}
	var s int
	flags := options.Flags()
	if flags.Contains(primitive.QueryFlagValues) {
		if flags.Contains(primitive.QueryFlagValueNames) {
			s, err = primitive.LengthOfNamedValues(options.NamedValues)
		} else {
			s, err = primitive.LengthOfPositionalValues(options.PositionalValues)
		}
	}
	if err != nil {
		return -1, fmt.Errorf("cannot compute length of query options values: %w", err)
	}
	length += s
	if flags.Contains(primitive.QueryFlagPageSize) {
		length += primitive.LengthOfInt
	}
	if flags.Contains(primitive.QueryFlagPagingState) {
		length += primitive.LengthOfBytes(options.PagingState)
	}
	if flags.Contains(primitive.QueryFlagSerialConsistency) {
		length += primitive.LengthOfShort
	}
	if flags.Contains(primitive.QueryFlagDefaultTimestamp) {
		length += primitive.LengthOfLong
	}
	if flags.Contains(primitive.QueryFlagWithKeyspace) {
		length += primitive.LengthOfString(options.Keyspace)
	}
	if flags.Contains(primitive.QueryFlagNowInSeconds) {
		length += primitive.LengthOfInt
	}
	if flags.Contains(primitive.QueryFlagDseWithContinuousPagingOptions) {
		if lengthOfContinuousPagingOptions, err := LengthOfContinuousPagingOptions(options.ContinuousPagingOptions, version); err != nil {
			return -1, fmt.Errorf("cannot compute length of continuous paging options: %w", err)
		} else {
			length += lengthOfContinuousPagingOptions
		}
	}
	return
}

func DecodeQueryOptions(source io.Reader, version primitive.ProtocolVersion) (options *QueryOptions, err error) {
	options = &QueryOptions{}
	var consistency uint16
	if consistency, err = primitive.ReadShort(source); err != nil {
		return nil, fmt.Errorf("cannot read consistency: %w", err)
	}
	options.Consistency = primitive.ConsistencyLevel(consistency)
	if err = primitive.CheckValidConsistencyLevel(options.Consistency); err != nil {
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
	if flags.Contains(primitive.QueryFlagValues) {
		if flags.Contains(primitive.QueryFlagValueNames) {
			options.NamedValues, err = primitive.ReadNamedValues(source, version)
		} else {
			options.PositionalValues, err = primitive.ReadPositionalValues(source, version)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("cannot read [value]s: %w", err)
	}
	options.SkipMetadata = flags.Contains(primitive.QueryFlagSkipMetadata)
	if flags.Contains(primitive.QueryFlagPageSize) {
		if options.PageSize, err = primitive.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read page size: %w", err)
		}
		if flags.Contains(primitive.QueryFlagDsePageSizeBytes) {
			options.PageSizeInBytes = true
		}
	}
	if flags.Contains(primitive.QueryFlagPagingState) {
		if options.PagingState, err = primitive.ReadBytes(source); err != nil {
			return nil, fmt.Errorf("cannot read paging state: %w", err)
		}
	}
	if flags.Contains(primitive.QueryFlagSerialConsistency) {
		options.SerialConsistency = &primitive.NillableConsistencyLevel{}
		var serialConsistency uint16
		if serialConsistency, err = primitive.ReadShort(source); err != nil {
			return nil, fmt.Errorf("cannot read serial consistency: %w", err)
		}
		options.SerialConsistency.Value = primitive.ConsistencyLevel(serialConsistency)
		if err = primitive.CheckValidConsistencyLevel(options.SerialConsistency.Value); err != nil {
			return nil, err
		}
	}
	if flags.Contains(primitive.QueryFlagDefaultTimestamp) {
		options.DefaultTimestamp = &primitive.NillableInt64{}
		if options.DefaultTimestamp.Value, err = primitive.ReadLong(source); err != nil {
			return nil, fmt.Errorf("cannot read default timestamp: %w", err)
		}
	}
	if flags.Contains(primitive.QueryFlagWithKeyspace) {
		if options.Keyspace, err = primitive.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read keyspace: %w", err)
		}
	}
	if flags.Contains(primitive.QueryFlagNowInSeconds) {
		options.NowInSeconds = &primitive.NillableInt32{}
		if options.NowInSeconds.Value, err = primitive.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read now-in-seconds: %w", err)
		}
	}
	if flags.Contains(primitive.QueryFlagDseWithContinuousPagingOptions) {
		if options.ContinuousPagingOptions, err = DecodeContinuousPagingOptions(source, version); err != nil {
			return nil, fmt.Errorf("cannot read continuous paging options: %w", err)
		}
	}
	return options, nil
}
