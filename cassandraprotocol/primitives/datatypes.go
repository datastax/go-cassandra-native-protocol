package primitives

import "go-cassandra-native-protocol/cassandraprotocol"

func WriteDataType(dataType cassandraprotocol.DataType, dest []byte) (remaining []byte, err error) {
	// TODO
	return
}

func LengthOfDataType(dataType cassandraprotocol.DataType) (length int) {
	// TODO
	return
}

func ReadDataType(source []byte) (decoded cassandraprotocol.DataType, remaining []byte, err error) {
	// TODO
	var id uint16
	if id, source, err = ReadShort(source); err != nil {

	}
	switch id {
	case cassandraprotocol.DataTypeCustom:
	case cassandraprotocol.DataTypeAscii:
	case cassandraprotocol.DataTypeBigint:
	case cassandraprotocol.DataTypeBlob:
	case cassandraprotocol.DataTypeBoolean:
	case cassandraprotocol.DataTypeCounter:
	case cassandraprotocol.DataTypeDecimal:
	case cassandraprotocol.DataTypeDouble:
	case cassandraprotocol.DataTypeFloat:
	case cassandraprotocol.DataTypeInt:
	case cassandraprotocol.DataTypeTimestamp:
	case cassandraprotocol.DataTypeUuid:
	case cassandraprotocol.DataTypeVarchar:
	case cassandraprotocol.DataTypeVarint:
	case cassandraprotocol.DataTypeTimeuuid:
	case cassandraprotocol.DataTypeInet:
	case cassandraprotocol.DataTypeDate:
	case cassandraprotocol.DataTypeTime:
	case cassandraprotocol.DataTypeSmallint:
	case cassandraprotocol.DataTypeTinyint:
	case cassandraprotocol.DataTypeDuration:
	case cassandraprotocol.DataTypeList:
	case cassandraprotocol.DataTypeMap:
	case cassandraprotocol.DataTypeSet:
	case cassandraprotocol.DataTypeUdt:
	case cassandraprotocol.DataTypeTuple:
	}
	return
}
