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

package datatype

import (
	"fmt"
	"io"

	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

// Set is a data type that represents a CQL set type.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/datatype.DataType
type Set struct {
	ElementType DataType
}

func (t *Set) Code() primitive.DataTypeCode {
	return primitive.DataTypeCodeSet
}

func (t *Set) String() string {
	return t.AsCql()
}

func (t *Set) AsCql() string {
	return fmt.Sprintf("set<%v>", t.ElementType.AsCql())
}

func NewSet(elementType DataType) *Set {
	return &Set{ElementType: elementType}
}

func writeSetType(t DataType, dest io.Writer, version primitive.ProtocolVersion) (err error) {
	if setType, ok := t.(*Set); !ok {
		return fmt.Errorf("expected *Set, got %T", t)
	} else if err = WriteDataType(setType.ElementType, dest, version); err != nil {
		return fmt.Errorf("cannot write set element type: %w", err)
	}
	return nil
}

func lengthOfSetType(t DataType, version primitive.ProtocolVersion) (length int, err error) {
	if setType, ok := t.(*Set); !ok {
		return -1, fmt.Errorf("expected *Set, got %T", t)
	} else if elementLength, err := LengthOfDataType(setType.ElementType, version); err != nil {
		return -1, fmt.Errorf("cannot compute length of set element type: %w", err)
	} else {
		length += elementLength
	}
	return length, nil
}

func readSetType(source io.Reader, version primitive.ProtocolVersion) (decoded DataType, err error) {
	setType := &Set{}
	if setType.ElementType, err = ReadDataType(source, version); err != nil {
		return nil, fmt.Errorf("cannot read set element type: %w", err)
	}
	return setType, nil
}
