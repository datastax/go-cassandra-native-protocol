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
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

// Execute is a request message that executes a prepared statement, identified by its prepared query id.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/message.Message
type Execute struct {
	// QueryId is the prepared query id to execute.
	QueryId []byte
	// the ID of the result set metadata that was sent along with response to PREPARE message.
	// Valid in protocol version 5 and DSE protocol version 2. See PreparedResult.
	ResultMetadataId []byte
	Options          *QueryOptions
}

func (m *Execute) IsResponse() bool {
	return false
}

func (m *Execute) GetOpCode() primitive.OpCode {
	return primitive.OpCodeExecute
}

func (m *Execute) String() string {
	return "EXECUTE " + hex.EncodeToString(m.QueryId)
}

type executeCodec struct{}

func (c *executeCodec) Encode(msg Message, dest io.Writer, version primitive.ProtocolVersion) error {
	execute, ok := msg.(*Execute)
	if !ok {
		return errors.New(fmt.Sprintf("expected *message.Execute, got %T", msg))
	}
	if len(execute.QueryId) == 0 {
		return errors.New("EXECUTE missing query id")
	} else if err := primitive.WriteShortBytes(execute.QueryId, dest); err != nil {
		return fmt.Errorf("cannot write EXECUTE query id: %w", err)
	}
	if version.SupportsResultMetadataId() {
		if len(execute.ResultMetadataId) == 0 {
			return errors.New("EXECUTE missing result metadata id")
		} else if err := primitive.WriteShortBytes(execute.ResultMetadataId, dest); err != nil {
			return fmt.Errorf("cannot write EXECUTE result metadata id: %w", err)
		}
	}
	if err := EncodeQueryOptions(execute.Options, dest, version); err != nil {
		return fmt.Errorf("cannot write EXECUTE options: %w", err)
	}
	return nil
}

func (c *executeCodec) EncodedLength(msg Message, version primitive.ProtocolVersion) (size int, err error) {
	execute, ok := msg.(*Execute)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected *message.Execute, got %T", msg))
	}
	size += primitive.LengthOfShortBytes(execute.QueryId)
	if version.SupportsResultMetadataId() {
		size += primitive.LengthOfShortBytes(execute.ResultMetadataId)
	}
	if lengthOfQueryOptions, err := LengthOfQueryOptions(execute.Options, version); err == nil {
		return size + lengthOfQueryOptions, nil
	} else {
		return -1, fmt.Errorf("cannot compute size EXECUTE query options: %w", err)
	}
}

func (c *executeCodec) Decode(source io.Reader, version primitive.ProtocolVersion) (msg Message, err error) {
	var execute = &Execute{
		Options: nil,
	}
	if execute.QueryId, err = primitive.ReadShortBytes(source); err != nil {
		return nil, fmt.Errorf("cannot read EXECUTE query id: %w", err)
	} else if len(execute.QueryId) == 0 {
		return nil, errors.New("EXECUTE missing query id")
	}
	if version.SupportsResultMetadataId() {
		if execute.ResultMetadataId, err = primitive.ReadShortBytes(source); err != nil {
			return nil, fmt.Errorf("cannot read EXECUTE result metadata id: %w", err)
		} else if len(execute.ResultMetadataId) == 0 {
			return nil, errors.New("EXECUTE missing result metadata id")
		}
	}
	if execute.Options, err = DecodeQueryOptions(source, version); err != nil {
		return nil, fmt.Errorf("cannot read EXECUTE query options: %w", err)
	}
	return execute, nil
}

func (c *executeCodec) GetOpCode() primitive.OpCode {
	return primitive.OpCodeExecute
}
