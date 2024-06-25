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

// Map is a data type that represents a CQL map type.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/datatype.DataType
type Map struct {
	KeyType   DataType
	ValueType DataType
}

func (t *Map) Code() primitive.DataTypeCode {
	return primitive.DataTypeCodeMap
}

func (t *Map) String() string {
	return t.AsCql()
}

func (t *Map) AsCql() string {
	return fmt.Sprintf("map<%v,%v>", t.KeyType.AsCql(), t.ValueType.AsCql())
}

func NewMap(keyType DataType, valueType DataType) *Map {
	return &Map{KeyType: keyType, ValueType: valueType}
}

func writeMapType(t DataType, dest io.Writer, version primitive.ProtocolVersion) (err error) {
	mapType, ok := t.(*Map)
	if !ok {
		return fmt.Errorf("expected *Map, got %T", t)
	} else if err = WriteDataType(mapType.KeyType, dest, version); err != nil {
		return fmt.Errorf("cannot write map key type: %w", err)
	} else if err = WriteDataType(mapType.ValueType, dest, version); err != nil {
		return fmt.Errorf("cannot write map value type: %w", err)
	}
	return nil
}

func lengthOfMapType(t DataType, version primitive.ProtocolVersion) (length int, err error) {
	mapType, ok := t.(*Map)
	if !ok {
		return -1, fmt.Errorf("expected *Map, got %T", t)
	}
	if keyLength, err := LengthOfDataType(mapType.KeyType, version); err != nil {
		return -1, fmt.Errorf("cannot compute length of map key type: %w", err)
	} else {
		length += keyLength
	}
	if valueLength, err := LengthOfDataType(mapType.ValueType, version); err != nil {
		return -1, fmt.Errorf("cannot compute length of map value type: %w", err)
	} else {
		length += valueLength
	}
	return length, nil
}

func readMapType(source io.Reader, version primitive.ProtocolVersion) (decoded DataType, err error) {
	mapType := &Map{}
	if mapType.KeyType, err = ReadDataType(source, version); err != nil {
		return nil, fmt.Errorf("cannot read map key type: %w", err)
	} else if mapType.ValueType, err = ReadDataType(source, version); err != nil {
		return nil, fmt.Errorf("cannot read map value type: %w", err)
	}
	return mapType, nil
}
