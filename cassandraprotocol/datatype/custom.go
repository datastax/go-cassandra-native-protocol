package datatype

import (
	"errors"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
	"io"
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

func (c *customTypeCodec) Encode(t DataType, dest io.Writer, _ cassandraprotocol.ProtocolVersion) (err error) {
	if customType, ok := t.(*customType); !ok {
		return errors.New(fmt.Sprintf("expected customType struct, got %T", t))
	} else if err = primitives.WriteString(customType.className, dest); err != nil {
		return fmt.Errorf("cannot write custom type class name: %w", err)
	}
	return nil
}

func (c *customTypeCodec) EncodedLength(t DataType, _ cassandraprotocol.ProtocolVersion) (length int, err error) {
	if customType, ok := t.(*customType); !ok {
		return -1, errors.New(fmt.Sprintf("expected customType struct, got %T", t))
	} else {
		length += primitives.LengthOfString(customType.className)
	}
	return length, nil
}

func (c *customTypeCodec) Decode(source io.Reader, _ cassandraprotocol.ProtocolVersion) (t DataType, err error) {
	customType := &customType{}
	if customType.className, err = primitives.ReadString(source); err != nil {
		return nil, fmt.Errorf("cannot read custom type class name: %w", err)
	}
	return customType, nil
}
