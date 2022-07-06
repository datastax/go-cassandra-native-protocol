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

// CustomType is a data type that represents a CQL custom type.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/datatype.DataType
type CustomType struct {
	ClassName string
}

func NewCustomType(className string) *CustomType {
	return &CustomType{ClassName: className}
}

func (t *CustomType) GetDataTypeCode() primitive.DataTypeCode {
	return primitive.DataTypeCodeCustom
}

func (t *CustomType) String() string {
	return fmt.Sprintf("custom(%v)", t.ClassName)
}

func (t *CustomType) MarshalJSON() ([]byte, error) {
	return []byte("\"" + t.String() + "\""), nil
}

func writeCustomType(t DataType, dest io.Writer, _ primitive.ProtocolVersion) (err error) {
	if customType, ok := t.(*CustomType); !ok {
		return fmt.Errorf("expected *CustomType, got %T", t)
	} else if err = primitive.WriteString(customType.ClassName, dest); err != nil {
		return fmt.Errorf("cannot write custom type class name: %w", err)
	}
	return nil
}

func lengthOfCustomType(t DataType, _ primitive.ProtocolVersion) (length int, err error) {
	if customType, ok := t.(*CustomType); !ok {
		return -1, fmt.Errorf("expected *CustomType, got %T", t)
	} else {
		length += primitive.LengthOfString(customType.ClassName)
	}
	return length, nil
}

func readCustomType(source io.Reader, _ primitive.ProtocolVersion) (t DataType, err error) {
	customType := &CustomType{}
	if customType.ClassName, err = primitive.ReadString(source); err != nil {
		return nil, fmt.Errorf("cannot read custom type class name: %w", err)
	}
	return customType, nil
}
