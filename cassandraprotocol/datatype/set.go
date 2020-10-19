package datatype

import (
	"errors"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"io"
)

type setType struct {
	elementType DataType
}

func (t *setType) GetDataTypeCode() cassandraprotocol.DataTypeCode {
	return cassandraprotocol.DataTypeCodeSet
}

func (t *setType) String() string {
	return fmt.Sprintf("set<%v>", t.elementType)
}

type setTypeCodec struct{}

func (c *setTypeCodec) Encode(t DataType, dest io.Writer, version cassandraprotocol.ProtocolVersion) (err error) {
	if setType, ok := t.(*setType); !ok {
		return errors.New(fmt.Sprintf("expected setType struct, got %T", t))
	} else if err = WriteDataType(setType.elementType, dest, version); err != nil {
		return fmt.Errorf("cannot write set element type: %w", err)
	}
	return nil
}

func (c *setTypeCodec) EncodedLength(t DataType, version cassandraprotocol.ProtocolVersion) (length int, err error) {
	if setType, ok := t.(*setType); !ok {
		return -1, errors.New(fmt.Sprintf("expected setType struct, got %T", t))
	} else if elementLength, err := LengthOfDataType(setType.elementType, version); err != nil {
		return -1, fmt.Errorf("cannot compute length of set element type: %w", err)
	} else {
		length += elementLength
	}
	return length, nil
}

func (c *setTypeCodec) Decode(source io.Reader, version cassandraprotocol.ProtocolVersion) (decoded DataType, err error) {
	setType := &setType{}
	if setType.elementType, err = ReadDataType(source, version); err != nil {
		return nil, fmt.Errorf("cannot read set element type: %w", err)
	}
	return setType, nil
}
