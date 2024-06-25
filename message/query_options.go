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

// QueryOptions is the set of options common to Query and Execute messages.
// +k8s:deepcopy-gen=true
type QueryOptions struct {
	// The consistency level to use when executing the query. This field is mandatory; its default is ANY, as the
	// zero value of primitive.ConsistencyLevel is primitive.ConsistencyLevelAny. Note that ANY is NOT suitable for
	// read queries.
	Consistency primitive.ConsistencyLevel

	// The positional values of the associated query. Positional values are designated in query strings with a
	// question mark bind marker ('?'). Positional values are valid for all protocol versions.
	// It is illegal to use both positional and named values at the same time. If this happens, positional values will
	// be used and named values will be silently ignored.
	PositionalValues []*primitive.Value

	// The named values of the associated query. Named values are designated in query strings with a
	// a bind marker starting with a colon (e.g. ':var1'). Named values can only be used with protocol version 3 or
	// higher.
	// It is illegal to use both positional and named values at the same time. If this happens, positional values will
	// be used and named values will be silently ignored.
	NamedValues map[string]*primitive.Value

	// If true, asks the server to skip result set metadata when sending results. In such case, the RowsResult message
	// returned as a response to the query (if any) will contain no result set metadata, and will have the NO_METADATA
	// flag set; in other words, RowsResult.Metadata will be present but RowsResult.Metadata.Columns will be nil.
	SkipMetadata bool

	// The desired page size. A value of zero or negative is usually interpreted as "no pagination" and can result
	// in the entire result set being returned in one single page.
	PageSize int32

	// Whether the page size is expressed in number of rows or number of bytes. Valid for DSE protocol versions only.
	PageSizeInBytes bool

	// PagingState is a [bytes] value coming from a previously-received RowsResult.Metadata object. If provided, the
	// query will be executed but starting from a given paging state.
	PagingState []byte

	// The (optional) serial consistency level to use when executing the query. Valid for protocol versions 2 and
	// higher.
	SerialConsistency *primitive.ConsistencyLevel

	// The default timestamp for the query in microseconds (negative values are discouraged but supported for
	// backward compatibility reasons except for the smallest negative value (-2^63) that is forbidden). If provided,
	// this will replace the server-side assigned timestamp as default timestamp. Note that a timestamp in the query
	// itself (that is, if the query has a USING TIMESTAMP clause) will still override this timestamp.
	// Default timestamps are valid for protocol versions 3 and higher.
	DefaultTimestamp *int64

	// Valid for protocol version 5 and DSE protocol version 2 only.
	Keyspace string

	// Introduced in Protocol Version 5, not present in DSE protocol versions.
	NowInSeconds *int32

	// Valid only for DSE protocol versions.
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
	// prefer positional values, if provided, and ignore named ones.
	if o.PositionalValues != nil {
		flags = flags.Add(primitive.QueryFlagValues)
	} else if o.NamedValues != nil {
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
	if err := primitive.CheckValidConsistencyLevel(options.Consistency); err != nil {
		return err
	} else if err = primitive.WriteShort(uint16(options.Consistency), dest); err != nil {
		return fmt.Errorf("cannot write consistency: %w", err)
	}
	flags := options.Flags()
	if version.Uses4BytesQueryFlags() {
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
		if err := primitive.CheckSerialConsistencyLevel(*options.SerialConsistency); err != nil {
			return err
		} else if err = primitive.WriteShort(uint16(*options.SerialConsistency), dest); err != nil {
			return fmt.Errorf("cannot write serial consistency: %w", err)
		}
	}
	if flags.Contains(primitive.QueryFlagDefaultTimestamp) {
		if err = primitive.WriteLong(*options.DefaultTimestamp, dest); err != nil {
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
		if err = primitive.WriteInt(*options.NowInSeconds, dest); err != nil {
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
	if version.Uses4BytesQueryFlags() {
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
		var optionsSerialConsistencyUint uint16
		if optionsSerialConsistencyUint, err = primitive.ReadShort(source); err != nil {
			return nil, fmt.Errorf("cannot read serial consistency: %w", err)
		}
		optionsSerialConsistency := primitive.ConsistencyLevel(optionsSerialConsistencyUint)
		if err = primitive.CheckValidConsistencyLevel(optionsSerialConsistency); err != nil {
			return nil, err
		}
		options.SerialConsistency = &optionsSerialConsistency
	}
	if flags.Contains(primitive.QueryFlagDefaultTimestamp) {
		var optionsDefaultTimestamp int64
		if optionsDefaultTimestamp, err = primitive.ReadLong(source); err != nil {
			return nil, fmt.Errorf("cannot read default timestamp: %w", err)
		}
		options.DefaultTimestamp = &optionsDefaultTimestamp
	}
	if flags.Contains(primitive.QueryFlagWithKeyspace) {
		if options.Keyspace, err = primitive.ReadString(source); err != nil {
			return nil, fmt.Errorf("cannot read keyspace: %w", err)
		}
	}
	if flags.Contains(primitive.QueryFlagNowInSeconds) {
		var optionsNowInSeconds int32
		if optionsNowInSeconds, err = primitive.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read now-in-seconds: %w", err)
		}
		options.NowInSeconds = &optionsNowInSeconds
	}
	if flags.Contains(primitive.QueryFlagDseWithContinuousPagingOptions) {
		if options.ContinuousPagingOptions, err = DecodeContinuousPagingOptions(source, version); err != nil {
			return nil, fmt.Errorf("cannot read continuous paging options: %w", err)
		}
	}
	return options, nil
}
