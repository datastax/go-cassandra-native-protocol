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

const (
	// SupportedProtocolVersions is a Supported.Options multimap key returned by Cassandra from protocol v5 onwards.
	// It holds the list of native protocol versions that are supported, encoded as the version number followed by a
	// slash and the version description. For example: 3/v3, 4/v4, 5/v5-beta. If a version is in beta, it will have the
	// word "beta" in its description.
	SupportedProtocolVersions = "PROTOCOL_VERSIONS"
)

// Supported is a response message sent in reply to an Options request.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/message.Message
type Supported struct {
	// This multimap gives for each of the supported Startup options, the list of supported values.
	// See Startup.Options for details about supported option keys.
	Options map[string][]string
}

func (m *Supported) IsResponse() bool {
	return true
}

func (m *Supported) GetOpCode() primitive.OpCode {
	return primitive.OpCodeSupported
}

func (m *Supported) String() string {
	return fmt.Sprintf("SUPPORTED %v", m.Options)
}

type supportedCodec struct{}

func (c *supportedCodec) Encode(msg Message, dest io.Writer, _ primitive.ProtocolVersion) error {
	supported, ok := msg.(*Supported)
	if !ok {
		return errors.New(fmt.Sprintf("expected *message.Supported, got %T", msg))
	}
	if err := primitive.WriteStringMultiMap(supported.Options, dest); err != nil {
		return err
	}
	return nil
}

func (c *supportedCodec) EncodedLength(msg Message, _ primitive.ProtocolVersion) (int, error) {
	supported, ok := msg.(*Supported)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected *message.Supported, got %T", msg))
	}
	return primitive.LengthOfStringMultiMap(supported.Options), nil
}

func (c *supportedCodec) Decode(source io.Reader, _ primitive.ProtocolVersion) (Message, error) {
	if options, err := primitive.ReadStringMultiMap(source); err != nil {
		return nil, err
	} else {
		return &Supported{Options: options}, nil
	}
}

func (c *supportedCodec) GetOpCode() primitive.OpCode {
	return primitive.OpCodeSupported
}
