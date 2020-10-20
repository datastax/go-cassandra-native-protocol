package datatype

import (
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"io"
)

var Ascii DataType = &primitiveType{code: cassandraprotocol.DataTypeCodeAscii}
var Bigint DataType = &primitiveType{code: cassandraprotocol.DataTypeCodeBigint}
var Blob DataType = &primitiveType{code: cassandraprotocol.DataTypeCodeBlob}
var Boolean DataType = &primitiveType{code: cassandraprotocol.DataTypeCodeBoolean}
var Counter DataType = &primitiveType{code: cassandraprotocol.DataTypeCodeCounter}
var Decimal DataType = &primitiveType{code: cassandraprotocol.DataTypeCodeDecimal}
var Double DataType = &primitiveType{code: cassandraprotocol.DataTypeCodeDouble}
var Float DataType = &primitiveType{code: cassandraprotocol.DataTypeCodeFloat}
var Int DataType = &primitiveType{code: cassandraprotocol.DataTypeCodeInt}
var Timestamp DataType = &primitiveType{code: cassandraprotocol.DataTypeCodeTimestamp}
var Uuid DataType = &primitiveType{code: cassandraprotocol.DataTypeCodeUuid}
var Varchar DataType = &primitiveType{code: cassandraprotocol.DataTypeCodeVarchar}
var Varint DataType = &primitiveType{code: cassandraprotocol.DataTypeCodeVarint}
var Timeuuid DataType = &primitiveType{code: cassandraprotocol.DataTypeCodeTimeuuid}
var Inet DataType = &primitiveType{code: cassandraprotocol.DataTypeCodeInet}
var Date DataType = &primitiveType{code: cassandraprotocol.DataTypeCodeDate}
var Time DataType = &primitiveType{code: cassandraprotocol.DataTypeCodeTime}
var Smallint DataType = &primitiveType{code: cassandraprotocol.DataTypeCodeSmallint}
var Tinyint DataType = &primitiveType{code: cassandraprotocol.DataTypeCodeTinyint}
var Duration DataType = &primitiveType{code: cassandraprotocol.DataTypeCodeDuration}

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

func (c *primitiveTypeCodec) Encode(_ DataType, dest io.Writer, version cassandraprotocol.ProtocolVersion) (err error) {
	if version < cassandraprotocol.ProtocolVersion5 && c.primitiveType.GetDataTypeCode() == cassandraprotocol.DataTypeCodeDuration {
		return fmt.Errorf("cannot use duration type with protocol version %v", version)
	}
	return nil
}

func (c *primitiveTypeCodec) EncodedLength(_ DataType, _ cassandraprotocol.ProtocolVersion) (int, error) {
	return 0, nil
}

func (c *primitiveTypeCodec) Decode(source io.Reader, version cassandraprotocol.ProtocolVersion) (t DataType, err error) {
	if version < cassandraprotocol.ProtocolVersion5 && c.primitiveType.GetDataTypeCode() == cassandraprotocol.DataTypeCodeDuration {
		return nil, fmt.Errorf("cannot use duration type with protocol version %v", version)
	}
	return c.primitiveType, nil
}
