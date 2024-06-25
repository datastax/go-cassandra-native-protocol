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

// Prepare is a request to prepare a CQL statement.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/message.Message
type Prepare struct {
	// The CQL query to prepare.
	Query string
	// The keyspace that the query should be executed in.
	// Introduced in Protocol Version 5, also present in DSE protocol v2.
	Keyspace string
}

func (m *Prepare) IsResponse() bool {
	return false
}

func (m *Prepare) GetOpCode() primitive.OpCode {
	return primitive.OpCodePrepare
}

func (m *Prepare) String() string {
	return fmt.Sprintf("PREPARE (%v, %v)", m.Query, m.Keyspace)
}

func (m *Prepare) Flags() primitive.PrepareFlag {
	var flags primitive.PrepareFlag
	if m.Keyspace != "" {
		flags = flags.Add(primitive.PrepareFlagWithKeyspace)
	}
	return flags
}

type prepareCodec struct{}

func (c *prepareCodec) Encode(msg Message, dest io.Writer, version primitive.ProtocolVersion) (err error) {
	prepare, ok := msg.(*Prepare)
	if !ok {
		return errors.New(fmt.Sprintf("expected *message.Prepare, got %T", msg))
	}
	if prepare.Query == "" {
		return errors.New("cannot write PREPARE empty query string")
	} else if err = primitive.WriteLongString(prepare.Query, dest); err != nil {
		return fmt.Errorf("cannot write PREPARE query string: %w", err)
	}
	if version.SupportsPrepareFlags() {
		flags := prepare.Flags()
		if err = primitive.WriteInt(int32(flags), dest); err != nil {
			return fmt.Errorf("cannot write PREPARE flags: %w", err)
		}
		if flags.Contains(primitive.PrepareFlagWithKeyspace) {
			if prepare.Keyspace == "" {
				return errors.New("cannot write empty keyspace")
			} else if err = primitive.WriteString(prepare.Keyspace, dest); err != nil {
				return fmt.Errorf("cannot write PREPARE keyspace: %w", err)
			}
		}
	}
	return
}

func (c *prepareCodec) EncodedLength(msg Message, version primitive.ProtocolVersion) (size int, err error) {
	prepare, ok := msg.(*Prepare)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected *message.Prepare, got %T", msg))
	}
	size += primitive.LengthOfLongString(prepare.Query)
	if version.SupportsPrepareFlags() {
		size += primitive.LengthOfInt // flags
		if prepare.Keyspace != "" {
			size += primitive.LengthOfString(prepare.Keyspace)
		}
	}
	return size, nil
}

func (c *prepareCodec) Decode(source io.Reader, version primitive.ProtocolVersion) (msg Message, err error) {
	prepare := &Prepare{}
	if prepare.Query, err = primitive.ReadLongString(source); err != nil {
		return nil, fmt.Errorf("cannot read PREPARE query: %w", err)
	}
	if version.SupportsPrepareFlags() {
		var flags primitive.PrepareFlag
		var f int32
		if f, err = primitive.ReadInt(source); err != nil {
			return nil, fmt.Errorf("cannot read PREPARE flags: %w", err)
		}
		flags = primitive.PrepareFlag(f)
		if flags.Contains(primitive.PrepareFlagWithKeyspace) {
			if prepare.Keyspace, err = primitive.ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read PREPARE keyspace: %w", err)
			}
		}
	}
	return prepare, nil
}

func (c *prepareCodec) GetOpCode() primitive.OpCode {
	return primitive.OpCodePrepare
}
