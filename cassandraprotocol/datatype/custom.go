package datatype

import (
	"errors"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
)

type customType struct {
	className string
}

func (t *customType) GetDataTypeCode() cassandraprotocol.DataTypeCode {
	return cassandraprotocol.DataTypeCodeCustom
}

func (t *customType) String() string {
	return fmt.Sprintf("custom(%v)", t.className)
}

type customTypeCodec struct{}

func (c *customTypeCodec) Encode(t DataType, dest []byte, _ cassandraprotocol.ProtocolVersion) (remaining []byte, err error) {
	customType, ok := t.(*customType)
	if !ok {
		return dest, errors.New(fmt.Sprintf("expected customType struct, got %T", t))
	} else if dest, err = primitives.WriteString(customType.className, dest); err != nil {
		return dest, fmt.Errorf("cannot write custom type class name: %w", err)
	}
	return dest, nil
}

func (c *customTypeCodec) EncodedLength(t DataType, _ cassandraprotocol.ProtocolVersion) (length int, err error) {
	customType, ok := t.(*customType)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected customType struct, got %T", t))
	}
	length += primitives.LengthOfString(customType.className)
	return length, nil
}

func (c *customTypeCodec) Decode(source []byte, _ cassandraprotocol.ProtocolVersion) (t DataType, remaining []byte, err error) {
	customType := &customType{}
	if customType.className, source, err = primitives.ReadString(source); err != nil {
		return nil, source, fmt.Errorf("cannot read custom type class name: %w", err)
	}
	return customType, source, nil
}
