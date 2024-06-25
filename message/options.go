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

// Options is a request message to obtain supported features from the server. The response to such a request is
// Supported.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/message.Message
type Options struct {
}

func (m *Options) IsResponse() bool {
	return false
}

func (m *Options) GetOpCode() primitive.OpCode {
	return primitive.OpCodeOptions
}

func (m *Options) String() string {
	return "OPTIONS"
}

type optionsCodec struct{}

func (c *optionsCodec) Encode(msg Message, _ io.Writer, _ primitive.ProtocolVersion) error {
	_, ok := msg.(*Options)
	if !ok {
		return errors.New(fmt.Sprintf("expected *message.Options, got %T", msg))
	}
	return nil
}

func (c *optionsCodec) EncodedLength(msg Message, _ primitive.ProtocolVersion) (int, error) {
	_, ok := msg.(*Options)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected *message.Options, got %T", msg))
	}
	return 0, nil
}

func (c *optionsCodec) Decode(_ io.Reader, _ primitive.ProtocolVersion) (Message, error) {
	return &Options{}, nil
}

func (c *optionsCodec) GetOpCode() primitive.OpCode {
	return primitive.OpCodeOptions
}
