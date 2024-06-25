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

// Register is a request to register the client as a listener for the specified event types.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/message.Message
type Register struct {
	EventTypes []primitive.EventType
}

func (m *Register) IsResponse() bool {
	return false
}

func (m *Register) GetOpCode() primitive.OpCode {
	return primitive.OpCodeRegister
}

func (m *Register) String() string {
	return fmt.Sprint("REGISTER ", m.EventTypes)
}

type registerCodec struct{}

func (c *registerCodec) Encode(msg Message, dest io.Writer, _ primitive.ProtocolVersion) error {
	register, ok := msg.(*Register)
	if !ok {
		return errors.New(fmt.Sprintf("expected *message.Register, got %T", msg))
	}
	if len(register.EventTypes) == 0 {
		return errors.New("REGISTER messages must have at least one event type")
	}
	for _, eventType := range register.EventTypes {
		if err := primitive.CheckValidEventType(eventType); err != nil {
			return err
		}
	}
	return primitive.WriteStringList(asStringList(register.EventTypes), dest)
}

func (c *registerCodec) EncodedLength(msg Message, _ primitive.ProtocolVersion) (int, error) {
	register, ok := msg.(*Register)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected *message.Register, got %T", msg))
	}
	return primitive.LengthOfStringList(asStringList(register.EventTypes)), nil
}

func (c *registerCodec) Decode(source io.Reader, _ primitive.ProtocolVersion) (Message, error) {
	if eventTypes, err := primitive.ReadStringList(source); err != nil {
		return nil, err
	} else {
		for _, eventType := range eventTypes {
			if err := primitive.CheckValidEventType(primitive.EventType(eventType)); err != nil {
				return nil, err
			}
		}
		return &Register{EventTypes: fromStringList(eventTypes)}, nil
	}
}

func (c *registerCodec) GetOpCode() primitive.OpCode {
	return primitive.OpCodeRegister
}

func asStringList(eventTypes []primitive.EventType) []string {
	strings := make([]string, len(eventTypes))
	for i, eventType := range eventTypes {
		strings[i] = string(eventType)
	}
	return strings
}

func fromStringList(strings []string) []primitive.EventType {
	eventTypes := make([]primitive.EventType, len(strings))
	for i, eventType := range strings {
		eventTypes[i] = primitive.EventType(eventType)
	}
	return eventTypes
}
