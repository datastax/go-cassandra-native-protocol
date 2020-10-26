package datatype

import (
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPrimitiveType(t *testing.T) {
	tests := []struct {
		name     string
		input    PrimitiveType
		expected cassandraprotocol.DataTypeCode
	}{
		{"Ascii", Ascii, cassandraprotocol.DataTypeCodeAscii},
		{"Bigint", Bigint, cassandraprotocol.DataTypeCodeBigint},
		{"Blob", Blob, cassandraprotocol.DataTypeCodeBlob},
		{"Boolean", Boolean, cassandraprotocol.DataTypeCodeBoolean},
		{"Counter", Counter, cassandraprotocol.DataTypeCodeCounter},
		{"Decimal", Decimal, cassandraprotocol.DataTypeCodeDecimal},
		{"Double", Double, cassandraprotocol.DataTypeCodeDouble},
		{"Float", Float, cassandraprotocol.DataTypeCodeFloat},
		{"Int", Int, cassandraprotocol.DataTypeCodeInt},
		{"Timestamp", Timestamp, cassandraprotocol.DataTypeCodeTimestamp},
		{"Uuid", Uuid, cassandraprotocol.DataTypeCodeUuid},
		{"Varchar", Varchar, cassandraprotocol.DataTypeCodeVarchar},
		{"Varint", Varint, cassandraprotocol.DataTypeCodeVarint},
		{"Timeuuid", Timeuuid, cassandraprotocol.DataTypeCodeTimeuuid},
		{"Inet", Inet, cassandraprotocol.DataTypeCodeInet},
		{"Date", Date, cassandraprotocol.DataTypeCodeDate},
		{"Time", Time, cassandraprotocol.DataTypeCodeTime},
		{"Smallint", Smallint, cassandraprotocol.DataTypeCodeSmallint},
		{"Tinyint", Tinyint, cassandraprotocol.DataTypeCodeTinyint},
		{"Duration", Duration, cassandraprotocol.DataTypeCodeDuration},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := test.input.GetDataTypeCode()
			assert.Equal(t, test.expected, actual)
		})
	}
}
