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
	"fmt"
	"io"

	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

// ContinuousPagingOptions holds options for DSE continuous paging feature. Valid for QUERY and EXECUTE messages.
// See QueryOptions.
// +k8s:deepcopy-gen=true
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

func EncodeContinuousPagingOptions(options *ContinuousPagingOptions, dest io.Writer, version primitive.ProtocolVersion) (err error) {
	if err = primitive.CheckDseProtocolVersion(version); err != nil {
		return err
	} else if err = primitive.WriteInt(options.MaxPages, dest); err != nil {
		return fmt.Errorf("cannot write max num pages: %w", err)
	} else if err = primitive.WriteInt(options.PagesPerSecond, dest); err != nil {
		return fmt.Errorf("cannot write pages per second: %w", err)
	} else if version >= primitive.ProtocolVersionDse2 {
		if err = primitive.WriteInt(options.NextPages, dest); err != nil {
			return fmt.Errorf("cannot write next pages: %w", err)
		}
	}
	return nil
}

func LengthOfContinuousPagingOptions(_ *ContinuousPagingOptions, version primitive.ProtocolVersion) (length int, err error) {
	if err = primitive.CheckDseProtocolVersion(version); err != nil {
		return -1, err
	}
	length += primitive.LengthOfInt // max num pages
	length += primitive.LengthOfInt // pages per second
	if version >= primitive.ProtocolVersionDse2 {
		length += primitive.LengthOfInt // next pages
	}
	return length, nil
}

func DecodeContinuousPagingOptions(source io.Reader, version primitive.ProtocolVersion) (options *ContinuousPagingOptions, err error) {
	if err = primitive.CheckDseProtocolVersion(version); err != nil {
		return nil, err
	}
	options = &ContinuousPagingOptions{}
	if options.MaxPages, err = primitive.ReadInt(source); err != nil {
		return nil, fmt.Errorf("cannot read max num pages: %w", err)
	} else if options.PagesPerSecond, err = primitive.ReadInt(source); err != nil {
		return nil, fmt.Errorf("cannot read pages per second: %w", err)
	} else if version >= primitive.ProtocolVersionDse2 {
		if options.NextPages, err = primitive.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read next pages: %w", err)
		}
	}
	return options, nil
}
