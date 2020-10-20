package datatype

import (
	"errors"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"io"
)

var Ascii PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeAscii}
var Bigint PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeBigint}
var Blob PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeBlob}
var Boolean PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeBoolean}
var Counter PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeCounter}
var Decimal PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeDecimal}
var Double PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeDouble}
var Float PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeFloat}
var Int PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeInt}
var Timestamp PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeTimestamp}
var Uuid PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeUuid}
var Varchar PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeVarchar}
var Varint PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeVarint}
var Timeuuid PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeTimeuuid}
var Inet PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeInet}
var Date PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeDate}
var Time PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeTime}
var Smallint PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeSmallint}
var Tinyint PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeTinyint}
var Duration PrimitiveType = &primitiveType{code: cassandraprotocol.DataTypeCodeDuration}

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
	primitiveType *primitiveType
}

func (c *primitiveTypeCodec) Encode(t DataType, dest io.Writer, version cassandraprotocol.ProtocolVersion) (err error) {
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

func (c *primitiveTypeCodec) Decode(source io.Reader, version cassandraprotocol.ProtocolVersion) (t DataType, err error) {
	if version < cassandraprotocol.ProtocolVersion5 && c.primitiveType.GetDataTypeCode() == cassandraprotocol.DataTypeCodeDuration {
		return nil, fmt.Errorf("cannot use duration type with protocol version %v", version)
	}
	return c.primitiveType, nil
}

func CheckPrimitiveDataTypeCode(code cassandraprotocol.DataTypeCode) error {
	switch code {
	case cassandraprotocol.DataTypeCodeAscii:
	case cassandraprotocol.DataTypeCodeBigint:
	case cassandraprotocol.DataTypeCodeBlob:
	case cassandraprotocol.DataTypeCodeBoolean:
	case cassandraprotocol.DataTypeCodeCounter:
	case cassandraprotocol.DataTypeCodeDecimal:
	case cassandraprotocol.DataTypeCodeDouble:
	case cassandraprotocol.DataTypeCodeFloat:
	case cassandraprotocol.DataTypeCodeInt:
	case cassandraprotocol.DataTypeCodeTimestamp:
	case cassandraprotocol.DataTypeCodeUuid:
	case cassandraprotocol.DataTypeCodeVarchar:
	case cassandraprotocol.DataTypeCodeVarint:
	case cassandraprotocol.DataTypeCodeTimeuuid:
	case cassandraprotocol.DataTypeCodeInet:
	case cassandraprotocol.DataTypeCodeDate:
	case cassandraprotocol.DataTypeCodeTime:
	case cassandraprotocol.DataTypeCodeSmallint:
	case cassandraprotocol.DataTypeCodeTinyint:
	case cassandraprotocol.DataTypeCodeDuration:
		return nil
	}
	return fmt.Errorf("invalid primitive data type code: %v", code)
}
