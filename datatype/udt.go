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

// UserDefined is a data type that represents a CQL user-defined type.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/datatype.DataType
type UserDefined struct {
	Keyspace   string
	Name       string
	FieldNames []string
	FieldTypes []DataType
	// Note: field names and field types are not modeled as a map because iteration order matters.
}

func NewUserDefined(keyspace string, name string, fieldNames []string, fieldTypes []DataType) (*UserDefined, error) {
	fieldNamesLength := len(fieldNames)
	fieldTypesLength := len(fieldTypes)
	if fieldNamesLength != fieldTypesLength {
		return nil, fmt.Errorf("field names and field types length mismatch: %d != %d", fieldNamesLength, fieldTypesLength)
	}
	return &UserDefined{Keyspace: keyspace, Name: name, FieldNames: fieldNames, FieldTypes: fieldTypes}, nil
}

func (t *UserDefined) Code() primitive.DataTypeCode {
	return primitive.DataTypeCodeUdt
}

func (t *UserDefined) String() string {
	return t.AsCql()
}

func (t *UserDefined) AsCql() string {
	buf := &bytes.Buffer{}
	buf.WriteString(t.Keyspace)
	buf.WriteString(".")
	buf.WriteString(t.Name)
	buf.WriteString("<")
	for i, fieldType := range t.FieldTypes {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(t.FieldNames[i])
		buf.WriteString(":")
		buf.WriteString(fieldType.AsCql())
	}
	buf.WriteString(">")
	return buf.String()
}

func writeUserDefinedType(t DataType, dest io.Writer, version primitive.ProtocolVersion) (err error) {
	userDefinedType, ok := t.(*UserDefined)
	if !ok {
		return fmt.Errorf("expected *UserDefined, got %T", t)
	} else if err = primitive.WriteString(userDefinedType.Keyspace, dest); err != nil {
		return fmt.Errorf("cannot write udt keyspace: %w", err)
	} else if err = primitive.WriteString(userDefinedType.Name, dest); err != nil {
		return fmt.Errorf("cannot write udt name: %w", err)
	} else if err = primitive.WriteShort(uint16(len(userDefinedType.FieldTypes)), dest); err != nil {
		return fmt.Errorf("cannot write udt field count: %w", err)
	}
	if len(userDefinedType.FieldNames) != len(userDefinedType.FieldTypes) {
		return fmt.Errorf("invalid user-defined type: length of field names is not equal to length of field types")
	}
	for i, fieldName := range userDefinedType.FieldNames {
		fieldType := userDefinedType.FieldTypes[i]
		if err = primitive.WriteString(fieldName, dest); err != nil {
			return fmt.Errorf("cannot write udt field %v name: %w", fieldName, err)
		} else if err = WriteDataType(fieldType, dest, version); err != nil {
			return fmt.Errorf("cannot write udt field %v: %w", fieldName, err)
		}
	}
	return nil
}

func lengthOfUserDefinedType(t DataType, version primitive.ProtocolVersion) (length int, err error) {
	userDefinedType, ok := t.(*UserDefined)
	if !ok {
		return -1, fmt.Errorf("expected *UserDefined, got %T", t)
	}
	length += primitive.LengthOfString(userDefinedType.Keyspace)
	length += primitive.LengthOfString(userDefinedType.Name)
	length += primitive.LengthOfShort // field count
	if len(userDefinedType.FieldNames) != len(userDefinedType.FieldTypes) {
		return -1, fmt.Errorf("invalid user-defined type: length of field names is not equal to length of field types")
	}
	for i, fieldName := range userDefinedType.FieldNames {
		fieldType := userDefinedType.FieldTypes[i]
		length += primitive.LengthOfString(fieldName)
		if fieldLength, err := LengthOfDataType(fieldType, version); err != nil {
			return -1, fmt.Errorf("cannot compute length of udt field %v: %w", fieldName, err)
		} else {
			length += fieldLength
		}
	}
	return length, nil
}

func readUserDefinedType(source io.Reader, version primitive.ProtocolVersion) (decoded DataType, err error) {
	userDefinedType := &UserDefined{}
	if userDefinedType.Keyspace, err = primitive.ReadString(source); err != nil {
		return nil, fmt.Errorf("cannot read udt keyspace: %w", err)
	} else if userDefinedType.Name, err = primitive.ReadString(source); err != nil {
		return nil, fmt.Errorf("cannot read udt name: %w", err)
	} else if fieldCount, err := primitive.ReadShort(source); err != nil {
		return nil, fmt.Errorf("cannot read udt field count: %w", err)
	} else {
		userDefinedType.FieldNames = make([]string, fieldCount)
		userDefinedType.FieldTypes = make([]DataType, fieldCount)
		for i := 0; i < int(fieldCount); i++ {
			if userDefinedType.FieldNames[i], err = primitive.ReadString(source); err != nil {
				return nil, fmt.Errorf("cannot read udt field %d name: %w", i, err)
			} else if userDefinedType.FieldTypes[i], err = ReadDataType(source, version); err != nil {
				return nil, fmt.Errorf("cannot read udt field %d: %w", i, err)
			}
		}
		return userDefinedType, nil
	}
}
