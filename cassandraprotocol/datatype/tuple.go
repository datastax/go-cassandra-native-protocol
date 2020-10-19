package datatype

import (
	"errors"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
)

type tupleType struct {
	FieldTypes []DataType
}

func (t *tupleType) GetDataTypeCode() cassandraprotocol.DataTypeCode {
	return cassandraprotocol.DataTypeCodeTuple
}

func (t *tupleType) String() string {
	return fmt.Sprintf("tuple<%v>", t.FieldTypes)
}

type tupleTypeCodec struct{}

func (c *tupleTypeCodec) Encode(t DataType, dest []byte, version cassandraprotocol.ProtocolVersion) (remaining []byte, err error) {
	tupleType, ok := t.(*tupleType)
	if !ok {
		return dest, errors.New(fmt.Sprintf("expected tupleType struct, got %T", t))
	}
	if dest, err = primitives.WriteShort(uint16(len(tupleType.FieldTypes)), dest); err != nil {
		return dest, fmt.Errorf("cannot write tuple type field count: %w", err)
	}
	for i, fieldType := range tupleType.FieldTypes {
		if dest, err = WriteDataType(fieldType, dest, version); err != nil {
			return dest, fmt.Errorf("cannot write tuple field %d: %w", i, err)
		}
	}
	return dest, nil
}

func (c *tupleTypeCodec) EncodedLength(t DataType, version cassandraprotocol.ProtocolVersion) (length int, err error) {
	tupleType, ok := t.(*tupleType)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected tupleType struct, got %T", t))
	}
	length += primitives.LengthOfShort // field count
	for i, fieldType := range tupleType.FieldTypes {
		if fieldLength, err := LengthOfDataType(fieldType, version); err != nil {
			return -1, fmt.Errorf("cannot compute length of tuple field %d: %w", i, err)
		} else {
			length += fieldLength
		}
	}
	return length, nil
}

func (c *tupleTypeCodec) Decode(source []byte, version cassandraprotocol.ProtocolVersion) (decoded DataType, remaining []byte, err error) {
	if fieldCount, source, err := primitives.ReadShort(source); err != nil {
		return nil, source, fmt.Errorf("cannot read tuple field count: %w", err)
	} else {
		tupleType := &tupleType{}
		tupleType.FieldTypes = make([]DataType, fieldCount)
		for i := 0; i < int(fieldCount); i++ {
			if tupleType.FieldTypes[i], source, err = ReadDataType(source, version); err != nil {
				return nil, source, fmt.Errorf("cannot read tuple field %d: %w", i, err)
			}
		}
		return tupleType, source, nil
	}
}
