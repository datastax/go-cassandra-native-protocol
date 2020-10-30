package datatype

import (
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"io"
)

type TupleType interface {
	DataType
	GetFieldTypes() []DataType
}

type tupleType struct {
	fieldTypes []DataType
}

func (t *tupleType) GetFieldTypes() []DataType {
	return t.fieldTypes
}

func NewTupleType(fieldTypes ...DataType) TupleType {
	return &tupleType{fieldTypes: fieldTypes}
}

func (t *tupleType) GetDataTypeCode() primitive.DataTypeCode {
	return primitive.DataTypeCodeTuple
}

func (t *tupleType) String() string {
	return fmt.Sprintf("tuple<%v>", t.fieldTypes)
}

func (t *tupleType) MarshalJSON() ([]byte, error) {
	return []byte("\"" + t.String() + "\""), nil
}

type tupleTypeCodec struct{}

func (c *tupleTypeCodec) encode(t DataType, dest io.Writer, version primitive.ProtocolVersion) (err error) {
	tupleType, ok := t.(TupleType)
	if !ok {
		return errors.New(fmt.Sprintf("expected TupleType, got %T", t))
	} else if err = primitive.WriteShort(uint16(len(tupleType.GetFieldTypes())), dest); err != nil {
		return fmt.Errorf("cannot write tuple type field count: %w", err)
	}
	for i, fieldType := range tupleType.GetFieldTypes() {
		if err = WriteDataType(fieldType, dest, version); err != nil {
			return fmt.Errorf("cannot write tuple field %d: %w", i, err)
		}
	}
	return nil
}

func (c *tupleTypeCodec) encodedLength(t DataType, version primitive.ProtocolVersion) (int, error) {
	if tupleType, ok := t.(TupleType); !ok {
		return -1, errors.New(fmt.Sprintf("expected TupleType, got %T", t))
	} else {
		length := primitive.LengthOfShort // field count
		for i, fieldType := range tupleType.GetFieldTypes() {
			if fieldLength, err := LengthOfDataType(fieldType, version); err != nil {
				return -1, fmt.Errorf("cannot compute length of tuple field %d: %w", i, err)
			} else {
				length += fieldLength
			}
		}
		return length, nil
	}
}

func (c *tupleTypeCodec) decode(source io.Reader, version primitive.ProtocolVersion) (DataType, error) {
	if fieldCount, err := primitive.ReadShort(source); err != nil {
		return nil, fmt.Errorf("cannot read tuple field count: %w", err)
	} else {
		tupleType := &tupleType{}
		tupleType.fieldTypes = make([]DataType, fieldCount)
		for i := 0; i < int(fieldCount); i++ {
			if tupleType.fieldTypes[i], err = ReadDataType(source, version); err != nil {
				return nil, fmt.Errorf("cannot read tuple field %d: %w", i, err)
			}
		}
		return tupleType, nil
	}
}
