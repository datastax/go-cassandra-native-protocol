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
	"bytes"
	"fmt"
	"io"

	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

// Tuple is a data type that represents a CQL tuple type.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/datatype.DataType
type Tuple struct {
	FieldTypes []DataType
}

func NewTuple(fieldTypes ...DataType) *Tuple {
	return &Tuple{FieldTypes: fieldTypes}
}

func (t *Tuple) Code() primitive.DataTypeCode {
	return primitive.DataTypeCodeTuple
}

func (t *Tuple) String() string {
	return t.AsCql()
}

func (t *Tuple) AsCql() string {
	buf := &bytes.Buffer{}
	buf.WriteString("tuple<")
	for i, elementType := range t.FieldTypes {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(elementType.AsCql())
	}
	buf.WriteString(">")
	return buf.String()
}

func writeTupleType(t DataType, dest io.Writer, version primitive.ProtocolVersion) (err error) {
	tupleType, ok := t.(*Tuple)
	if !ok {
		return fmt.Errorf("expected *Tuple, got %T", t)
	} else if err = primitive.WriteShort(uint16(len(tupleType.FieldTypes)), dest); err != nil {
		return fmt.Errorf("cannot write tuple type field count: %w", err)
	}
	for i, fieldType := range tupleType.FieldTypes {
		if err = WriteDataType(fieldType, dest, version); err != nil {
			return fmt.Errorf("cannot write tuple field %d: %w", i, err)
		}
	}
	return nil
}

func lengthOfTupleType(t DataType, version primitive.ProtocolVersion) (int, error) {
	if tupleType, ok := t.(*Tuple); !ok {
		return -1, fmt.Errorf("expected *Tuple, got %T", t)
	} else {
		length := primitive.LengthOfShort // field count
		for i, fieldType := range tupleType.FieldTypes {
			if fieldLength, err := LengthOfDataType(fieldType, version); err != nil {
				return -1, fmt.Errorf("cannot compute length of tuple field %d: %w", i, err)
			} else {
				length += fieldLength
			}
		}
		return length, nil
	}
}

func readTupleType(source io.Reader, version primitive.ProtocolVersion) (DataType, error) {
	if fieldCount, err := primitive.ReadShort(source); err != nil {
		return nil, fmt.Errorf("cannot read tuple field count: %w", err)
	} else {
		tupleType := &Tuple{}
		tupleType.FieldTypes = make([]DataType, fieldCount)
		for i := 0; i < int(fieldCount); i++ {
			if tupleType.FieldTypes[i], err = ReadDataType(source, version); err != nil {
				return nil, fmt.Errorf("cannot read tuple field %d: %w", i, err)
			}
		}
		return tupleType, nil
	}
}
