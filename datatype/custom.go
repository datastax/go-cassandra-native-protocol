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

type CustomType interface {
	DataType
	GetClassName() string
}

type customType struct {
	className string
}

func (t *customType) GetClassName() string {
	return t.className
}

func NewCustomType(className string) CustomType {
	return &customType{className: className}
}

func (t *customType) GetDataTypeCode() primitive.DataTypeCode {
	return primitive.DataTypeCodeCustom
}

func (t *customType) Clone() DataType {
	return &customType{
		className: t.className,
	}
}

func (t *customType) String() string {
	return fmt.Sprintf("custom(%v)", t.className)
}

func (t *customType) MarshalJSON() ([]byte, error) {
	return []byte("\"" + t.String() + "\""), nil
}

func writeCustomType(t DataType, dest io.Writer, _ primitive.ProtocolVersion) (err error) {
	if customType, ok := t.(CustomType); !ok {
		return fmt.Errorf("expected CustomType, got %T", t)
	} else if err = primitive.WriteString(customType.GetClassName(), dest); err != nil {
		return fmt.Errorf("cannot write custom type class name: %w", err)
	}
	return nil
}

func lengthOfCustomType(t DataType, _ primitive.ProtocolVersion) (length int, err error) {
	if customType, ok := t.(CustomType); !ok {
		return -1, fmt.Errorf("expected CustomType, got %T", t)
	} else {
		length += primitive.LengthOfString(customType.GetClassName())
	}
	return length, nil
}

func readCustomType(source io.Reader, _ primitive.ProtocolVersion) (t DataType, err error) {
	customType := &customType{}
	if customType.className, err = primitive.ReadString(source); err != nil {
		return nil, fmt.Errorf("cannot read custom type class name: %w", err)
	}
	return customType, nil
}
