package datatype

import (
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitive"
	"io"
)

type SetType interface {
	DataType
	GetElementType() DataType
}

type setType struct {
	elementType DataType
}

func (t *setType) GetElementType() DataType {
	return t.elementType
}

func (t *setType) GetDataTypeCode() primitive.DataTypeCode {
	return primitive.DataTypeCodeSet
}

func (t *setType) String() string {
	return fmt.Sprintf("set<%v>", t.elementType)
}

func (t *setType) MarshalJSON() ([]byte, error) {
	return []byte("\"" + t.String() + "\""), nil
}

func NewSetType(elementType DataType) SetType {
	return &setType{elementType: elementType}
}

type setTypeCodec struct{}

func (c *setTypeCodec) encode(t DataType, dest io.Writer, version primitive.ProtocolVersion) (err error) {
	if setType, ok := t.(SetType); !ok {
		return errors.New(fmt.Sprintf("expected SetType, got %T", t))
	} else if err = WriteDataType(setType.GetElementType(), dest, version); err != nil {
		return fmt.Errorf("cannot write set element type: %w", err)
	}
	return nil
}

func (c *setTypeCodec) encodedLength(t DataType, version primitive.ProtocolVersion) (length int, err error) {
	if setType, ok := t.(SetType); !ok {
		return -1, errors.New(fmt.Sprintf("expected SetType, got %T", t))
	} else if elementLength, err := LengthOfDataType(setType.GetElementType(), version); err != nil {
		return -1, fmt.Errorf("cannot compute length of set element type: %w", err)
	} else {
		length += elementLength
	}
	return length, nil
}

func (c *setTypeCodec) decode(source io.Reader, version primitive.ProtocolVersion) (decoded DataType, err error) {
	setType := &setType{}
	if setType.elementType, err = ReadDataType(source, version); err != nil {
		return nil, fmt.Errorf("cannot read set element type: %w", err)
	}
	return setType, nil
}
