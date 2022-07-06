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
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"io"
)

// ListType is a data type that represents a CQL list type.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/datatype.DataType
type ListType struct {
	ElementType DataType
}

func NewListType(elementType DataType) *ListType {
	return &ListType{ElementType: elementType}
}

func (t *ListType) GetDataTypeCode() primitive.DataTypeCode {
	return primitive.DataTypeCodeList
}

func (t *ListType) String() string {
	return fmt.Sprintf("list<%v>", t.ElementType)
}

func (t *ListType) MarshalJSON() ([]byte, error) {
	return []byte("\"" + t.String() + "\""), nil
}

func writeListType(t DataType, dest io.Writer, version primitive.ProtocolVersion) (err error) {
	if listType, ok := t.(*ListType); !ok {
		return fmt.Errorf("expected *ListType, got %T", t)
	} else if err = WriteDataType(listType.ElementType, dest, version); err != nil {
		return fmt.Errorf("cannot write list element type: %w", err)
	}
	return nil
}

func lengthOfListType(t DataType, version primitive.ProtocolVersion) (length int, err error) {
	if listType, ok := t.(*ListType); !ok {
		return -1, fmt.Errorf("expected *ListType, got %T", t)
	} else if elementLength, err := LengthOfDataType(listType.ElementType, version); err != nil {
		return -1, fmt.Errorf("cannot compute length of list element type: %w", err)
	} else {
		length += elementLength
	}
	return length, nil
}

func readListType(source io.Reader, version primitive.ProtocolVersion) (decoded DataType, err error) {
	listType := &ListType{}
	if listType.ElementType, err = ReadDataType(source, version); err != nil {
		return nil, fmt.Errorf("cannot read list element type: %w", err)
	}
	return listType, nil
}
