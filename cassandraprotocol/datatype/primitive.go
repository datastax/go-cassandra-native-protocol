package datatype

import (
	"go-cassandra-native-protocol/cassandraprotocol"
)

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

type primitiveTypeCodec struct {
	primitiveType *primitiveType
}

func (c *primitiveTypeCodec) Encode(_ DataType, dest []byte, _ cassandraprotocol.ProtocolVersion) (remaining []byte, err error) {
	return dest, nil
}

func (c *primitiveTypeCodec) EncodedLength(_ DataType, _ cassandraprotocol.ProtocolVersion) (int, error) {
	return 0, nil
}

func (c *primitiveTypeCodec) Decode(source []byte, _ cassandraprotocol.ProtocolVersion) (t DataType, remaining []byte, err error) {
	return c.primitiveType, source, nil
}
