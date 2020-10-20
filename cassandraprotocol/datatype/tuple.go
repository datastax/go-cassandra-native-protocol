package datatype

import (
	"errors"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
	"io"
)

type tupleType struct {
	FieldTypes []DataType
}

func NewTupleType(fieldTypes ...DataType) DataType {
	return &tupleType{FieldTypes: fieldTypes}
}

func (t *tupleType) GetDataTypeCode() cassandraprotocol.DataTypeCode {
	return cassandraprotocol.DataTypeCodeTuple
}

func (t *tupleType) String() string {
	return fmt.Sprintf("tuple<%v>", t.FieldTypes)
}

func (t *tupleType) MarshalJSON() ([]byte, error) {
	return []byte("\"" + t.String() + "\""), nil
}

type tupleTypeCodec struct{}

func (c *tupleTypeCodec) Encode(t DataType, dest io.Writer, version cassandraprotocol.ProtocolVersion) (err error) {
	tupleType, ok := t.(*tupleType)
	if !ok {
		return errors.New(fmt.Sprintf("expected tupleType struct, got %T", t))
	} else if err = primitives.WriteShort(uint16(len(tupleType.FieldTypes)), dest); err != nil {
		return fmt.Errorf("cannot write tuple type field count: %w", err)
	}
	for i, fieldType := range tupleType.FieldTypes {
		if err = WriteDataType(fieldType, dest, version); err != nil {
			return fmt.Errorf("cannot write tuple field %d: %w", i, err)
		}
	}
	return nil
}

func (c *tupleTypeCodec) EncodedLength(t DataType, version cassandraprotocol.ProtocolVersion) (int, error) {
	if tupleType, ok := t.(*tupleType); !ok {
		return -1, errors.New(fmt.Sprintf("expected tupleType struct, got %T", t))
	} else {
		length := primitives.LengthOfShort // field count
		for i, fieldType := range tupleType.FieldTypes {
			if fieldLength, err := LengthOfDataType(fieldType, version); err != nil {
				return -1, fmt.Errorf("cannot compute length of tuple field %d: %w", i, err)
			} else {
				length += fieldLength
			}
		}
		return length, nil
	}
}

func (c *tupleTypeCodec) Decode(source io.Reader, version cassandraprotocol.ProtocolVersion) (DataType, error) {
	if fieldCount, err := primitives.ReadShort(source); err != nil {
		return nil, fmt.Errorf("cannot read tuple field count: %w", err)
	} else {
		tupleType := &tupleType{}
		tupleType.FieldTypes = make([]DataType, fieldCount)
		for i := 0; i < int(fieldCount); i++ {
			if tupleType.FieldTypes[i], err = ReadDataType(source, version); err != nil {
				return nil, fmt.Errorf("cannot read tuple field %d: %w", i, err)
			}
		}
		return tupleType, nil
	}
}
