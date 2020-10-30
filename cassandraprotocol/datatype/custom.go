package datatype

import (
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitives"
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

func (t *customType) GetDataTypeCode() primitives.DataTypeCode {
	return primitives.DataTypeCodeCustom
}

func (t *customType) String() string {
	return fmt.Sprintf("custom(%v)", t.className)
}

func (t *customType) MarshalJSON() ([]byte, error) {
	return []byte("\"" + t.String() + "\""), nil
}

type customTypeCodec struct{}

func (c *customTypeCodec) encode(t DataType, dest io.Writer, _ primitives.ProtocolVersion) (err error) {
	if customType, ok := t.(CustomType); !ok {
		return errors.New(fmt.Sprintf("expected CustomType, got %T", t))
	} else if err = primitives.WriteString(customType.GetClassName(), dest); err != nil {
		return fmt.Errorf("cannot write custom type class name: %w", err)
	}
	return nil
}

func (c *customTypeCodec) encodedLength(t DataType, _ primitives.ProtocolVersion) (length int, err error) {
	if customType, ok := t.(CustomType); !ok {
		return -1, errors.New(fmt.Sprintf("expected CustomType, got %T", t))
	} else {
		length += primitives.LengthOfString(customType.GetClassName())
	}
	return length, nil
}

func (c *customTypeCodec) decode(source io.Reader, _ primitives.ProtocolVersion) (t DataType, err error) {
	customType := &customType{}
	if customType.className, err = primitives.ReadString(source); err != nil {
		return nil, fmt.Errorf("cannot read custom type class name: %w", err)
	}
	return customType, nil
}
