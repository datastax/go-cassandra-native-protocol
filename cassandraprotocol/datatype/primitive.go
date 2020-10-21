package datatype

import (
	"errors"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"io"
)

var (
	Ascii     PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeAscii}
	Bigint    PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeBigint}
	Blob      PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeBlob}
	Boolean   PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeBoolean}
	Counter   PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeCounter}
	Decimal   PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeDecimal}
	Double    PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeDouble}
	Float     PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeFloat}
	Int       PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeInt}
	Timestamp PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeTimestamp}
	Uuid      PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeUuid}
	Varchar   PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeVarchar}
	Varint    PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeVarint}
	Timeuuid  PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeTimeuuid}
	Inet      PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeInet}
	Date      PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeDate}
	Time      PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeTime}
	Smallint  PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeSmallint}
	Tinyint   PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeTinyint}
	Duration  PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeDuration}
)

type PrimitiveType interface {
	DataType
}

type primitiveType struct {
	code cassandraprotocol.DataTypeCode
}

func (t *primitiveType) GetDataTypeCode() cassandraprotocol.DataTypeCode {
	return t.code
}

func (t *primitiveType) String() string {
	switch t.GetDataTypeCode() {
	case cassandraprotocol.DataTypeCodeAscii:
		return "ascii"
	case cassandraprotocol.DataTypeCodeBigint:
		return "bigint"
	case cassandraprotocol.DataTypeCodeBlob:
		return "blob"
	case cassandraprotocol.DataTypeCodeBoolean:
		return "boolean"
	case cassandraprotocol.DataTypeCodeCounter:
		return "counter"
	case cassandraprotocol.DataTypeCodeDecimal:
		return "decimal"
	case cassandraprotocol.DataTypeCodeDouble:
		return "double"
	case cassandraprotocol.DataTypeCodeFloat:
		return "float"
	case cassandraprotocol.DataTypeCodeInt:
		return "int"
	case cassandraprotocol.DataTypeCodeTimestamp:
		return "timestamp"
	case cassandraprotocol.DataTypeCodeUuid:
		return "uuid"
	case cassandraprotocol.DataTypeCodeVarchar:
		return "varchar"
	case cassandraprotocol.DataTypeCodeVarint:
		return "varint"
	case cassandraprotocol.DataTypeCodeTimeuuid:
		return "timeuuid"
	case cassandraprotocol.DataTypeCodeInet:
		return "inet"
	case cassandraprotocol.DataTypeCodeDate:
		return "date"
	case cassandraprotocol.DataTypeCodeTime:
		return "tune"
	case cassandraprotocol.DataTypeCodeSmallint:
		return "smallint"
	case cassandraprotocol.DataTypeCodeTinyint:
		return "tinyint"
	case cassandraprotocol.DataTypeCodeDuration:
		return "duration"
	}
	return "?"
}

func (t *primitiveType) MarshalJSON() ([]byte, error) {
	return []byte("\"" + t.String() + "\""), nil
}

type primitiveTypeCodec struct {
	primitiveType PrimitiveType
}

func (c *primitiveTypeCodec) encode(t DataType, _ io.Writer, version cassandraprotocol.ProtocolVersion) (err error) {
	_, ok := t.(PrimitiveType)
	if !ok {
		return errors.New(fmt.Sprintf("expected PrimitiveType, got %T", t))
	}
	if err := cassandraprotocol.CheckPrimitiveDataTypeCode(t.GetDataTypeCode()); err != nil {
		return err
	}
	if version < cassandraprotocol.ProtocolVersion5 && c.primitiveType.GetDataTypeCode() == cassandraprotocol.DataTypeCodeDuration {
		return fmt.Errorf("cannot use duration type with protocol version %v", version)
	}
	return nil
}

func (c *primitiveTypeCodec) encodedLength(t DataType, _ cassandraprotocol.ProtocolVersion) (int, error) {
	_, ok := t.(PrimitiveType)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected PrimitiveType, got %T", t))
	}
	if err := cassandraprotocol.CheckPrimitiveDataTypeCode(t.GetDataTypeCode()); err != nil {
		return -1, err
	}
	return 0, nil
}

func (c *primitiveTypeCodec) decode(_ io.Reader, version cassandraprotocol.ProtocolVersion) (t DataType, err error) {
	if version < cassandraprotocol.ProtocolVersion5 && c.primitiveType.GetDataTypeCode() == cassandraprotocol.DataTypeCodeDuration {
		return nil, fmt.Errorf("cannot use duration type with protocol version %v", version)
	}
	return c.primitiveType, nil
}
