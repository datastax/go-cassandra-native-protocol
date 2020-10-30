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
	if o.SerialConsistency != nil {
		flags |= primitive.QueryFlagSerialConsistency
	}
	if o.DefaultTimestamp != nil {
		flags |= primitive.QueryFlagDefaultTimestamp
	}
	if o.Keyspace != "" {
		flags |= primitive.QueryFlagWithKeyspace
	}
	if o.NowInSeconds != nil {
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
		if err := primitive.CheckSerialConsistencyLevel(options.SerialConsistency.Value); err != nil {
			return err
		} else if err = primitive.WriteShort(options.SerialConsistency.Value, dest); err != nil {
			return fmt.Errorf("cannot write serial consistency: %w", err)
		}
	}
	if flags&primitive.QueryFlagDefaultTimestamp != 0 {
		if err = primitive.WriteLong(options.DefaultTimestamp.Value, dest); err != nil {
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
		if err = primitive.WriteInt(options.NowInSeconds.Value, dest); err != nil {
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
	options = &QueryOptions{}
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
		options.SerialConsistency = &primitive.NillableConsistencyLevel{}
		if options.SerialConsistency.Value, err = primitive.ReadShort(source); err != nil {
			return nil, fmt.Errorf("cannot read serial consistency: %w", err)
		} else if err = primitive.CheckConsistencyLevel(options.SerialConsistency.Value); err != nil {
			return nil, err
		}
	}
	if flags&primitive.QueryFlagDefaultTimestamp != 0 {
		options.DefaultTimestamp = &primitive.NillableInt64{}
		if options.DefaultTimestamp.Value, err = primitive.ReadLong(source); err != nil {
			return nil, fmt.Errorf("cannot read default timestamp: %w", err)
		}
	}
	if flags&primitive.QueryFlagWithKeyspace != 0 {
		if options.Keyspace, err = primitive.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read keyspace: %w", err)
		}
	}
	if flags&primitive.QueryFlagNowInSeconds != 0 {
		options.NowInSeconds = &primitive.NillableInt32{}
		if options.NowInSeconds.Value, err = primitive.ReadInt(source); err != nil {
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
