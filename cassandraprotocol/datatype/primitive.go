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

func (c *primitiveTypeCodec) Encode(t DataType, _ io.Writer, version cassandraprotocol.ProtocolVersion) (err error) {
	_, ok := t.(PrimitiveType)
	if !ok {
		return errors.New(fmt.Sprintf("expected PrimitiveType, got %T", t))
	}
	if err := CheckPrimitiveDataTypeCode(t.GetDataTypeCode()); err != nil {
		return err
	}
	if version < cassandraprotocol.ProtocolVersion5 && c.primitiveType.GetDataTypeCode() == cassandraprotocol.DataTypeCodeDuration {
		return fmt.Errorf("cannot use duration type with protocol version %v", version)
	}
	return nil
}

func (c *primitiveTypeCodec) EncodedLength(t DataType, _ cassandraprotocol.ProtocolVersion) (int, error) {
	_, ok := t.(PrimitiveType)
	if !ok {
		return -1, errors.New(fmt.Sprintf("expected PrimitiveType, got %T", t))
	}
	if err := CheckPrimitiveDataTypeCode(t.GetDataTypeCode()); err != nil {
		return -1, err
	}
	return 0, nil
}

func (c *primitiveTypeCodec) Decode(_ io.Reader, version cassandraprotocol.ProtocolVersion) (t DataType, err error) {
	if version < cassandraprotocol.ProtocolVersion5 && c.primitiveType.GetDataTypeCode() == cassandraprotocol.DataTypeCodeDuration {
		return nil, fmt.Errorf("cannot use duration type with protocol version %v", version)
	}
	return c.primitiveType, nil
}

func CheckPrimitiveDataTypeCode(code cassandraprotocol.DataTypeCode) error {
	switch code {
	case cassandraprotocol.DataTypeCodeAscii:
		return nil
	case cassandraprotocol.DataTypeCodeBigint:
		return nil
	case cassandraprotocol.DataTypeCodeBlob:
		return nil
	case cassandraprotocol.DataTypeCodeBoolean:
		return nil
	case cassandraprotocol.DataTypeCodeCounter:
		return nil
	case cassandraprotocol.DataTypeCodeDecimal:
		return nil
	case cassandraprotocol.DataTypeCodeDouble:
		return nil
	case cassandraprotocol.DataTypeCodeFloat:
		return nil
	case cassandraprotocol.DataTypeCodeInt:
		return nil
	case cassandraprotocol.DataTypeCodeTimestamp:
		return nil
	case cassandraprotocol.DataTypeCodeUuid:
		return nil
	case cassandraprotocol.DataTypeCodeVarchar:
		return nil
	case cassandraprotocol.DataTypeCodeVarint:
		return nil
	case cassandraprotocol.DataTypeCodeTimeuuid:
		return nil
	case cassandraprotocol.DataTypeCodeInet:
		return nil
	case cassandraprotocol.DataTypeCodeDate:
		return nil
	case cassandraprotocol.DataTypeCodeTime:
		return nil
	case cassandraprotocol.DataTypeCodeSmallint:
		return nil
	case cassandraprotocol.DataTypeCodeTinyint:
		return nil
	case cassandraprotocol.DataTypeCodeDuration:
		return nil
	}
	return fmt.Errorf("invalid primitive data type code: %v", code)
}
