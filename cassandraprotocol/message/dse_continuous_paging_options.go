package message

import (
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitives"
	"io"
)

// ContinuousPagingOptions holds options for DSE continuous paging feature. Valid for QUERY and EXECUTE messages.
// See QueryOptions.
type ContinuousPagingOptions struct {
	// The maximum number of pages that the server will send to the client in total, or zero to indicate no limit.
	// Valid for both DSE v1 and v2.
	MaxPages int32
	// The maximum number of pages per second, or zero to indicate no limit.
	// Valid for both DSE v1 and v2.
	PagesPerSecond int32
	// The number of pages that the client is ready to receive, or zero to indicate no limit.
	// Valid for DSE v2 only.
	// See Revise.
	NextPages int32
}

func EncodeContinuousPagingOptions(options *ContinuousPagingOptions, dest io.Writer, version cassandraprotocol.ProtocolVersion) (err error) {
	if err = cassandraprotocol.CheckDseProtocolVersion(version); err != nil {
		return err
	} else if err = primitives.WriteInt(options.MaxPages, dest); err != nil {
		return fmt.Errorf("cannot write max num pages: %w", err)
	} else if err = primitives.WriteInt(options.PagesPerSecond, dest); err != nil {
		return fmt.Errorf("cannot write pages per second: %w", err)
	} else if version >= cassandraprotocol.ProtocolVersionDse2 {
		if err = primitives.WriteInt(options.NextPages, dest); err != nil {
			return fmt.Errorf("cannot write next pages: %w", err)
		}
	}
	return nil
}

func LengthOfContinuousPagingOptions(_ *ContinuousPagingOptions, version cassandraprotocol.ProtocolVersion) (length int, err error) {
	if err = cassandraprotocol.CheckDseProtocolVersion(version); err != nil {
		return -1, err
	}
	length += primitives.LengthOfInt // max num pages
	length += primitives.LengthOfInt // pages per second
	if version >= cassandraprotocol.ProtocolVersionDse2 {
		length += primitives.LengthOfInt // next pages
	}
	return length, nil
}

func DecodeContinuousPagingOptions(source io.Reader, version cassandraprotocol.ProtocolVersion) (options *ContinuousPagingOptions, err error) {
	if err = cassandraprotocol.CheckDseProtocolVersion(version); err != nil {
		return nil, err
	}
	options = &ContinuousPagingOptions{}
	if options.MaxPages, err = primitives.ReadInt(source); err != nil {
		return nil, fmt.Errorf("cannot read max num pages: %w", err)
	} else if options.PagesPerSecond, err = primitives.ReadInt(source); err != nil {
		return nil, fmt.Errorf("cannot read pages per second: %w", err)
	} else if version >= cassandraprotocol.ProtocolVersionDse2 {
		if options.NextPages, err = primitives.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read next pages: %w", err)
		}
	}
	return options, nil
}
