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

// Custom is a data type that represents a CQL custom type.
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=github.com/datastax/go-cassandra-native-protocol/datatype.DataType
type Custom struct {
	ClassName string
}

func NewCustom(className string) *Custom {
	return &Custom{ClassName: className}
}

func (t *Custom) Code() primitive.DataTypeCode {
	return primitive.DataTypeCodeCustom
}

func (t *Custom) String() string {
	return t.AsCql()
}

func (t *Custom) AsCql() string {
	return fmt.Sprintf("'%v'", t.ClassName)
}

func writeCustomType(t DataType, dest io.Writer, _ primitive.ProtocolVersion) (err error) {
	if customType, ok := t.(*Custom); !ok {
		return fmt.Errorf("expected *Custom, got %T", t)
	} else if err = primitive.WriteString(customType.ClassName, dest); err != nil {
		return fmt.Errorf("cannot write custom type class name: %w", err)
	}
	return nil
}

func lengthOfCustomType(t DataType, _ primitive.ProtocolVersion) (length int, err error) {
	if customType, ok := t.(*Custom); !ok {
		return -1, fmt.Errorf("expected *Custom, got %T", t)
	} else {
		length += primitive.LengthOfString(customType.ClassName)
	}
	return length, nil
}

func readCustomType(source io.Reader, _ primitive.ProtocolVersion) (t DataType, err error) {
	customType := &Custom{}
	if customType.ClassName, err = primitive.ReadString(source); err != nil {
		return nil, fmt.Errorf("cannot read custom type class name: %w", err)
	}
	return customType, nil
}
