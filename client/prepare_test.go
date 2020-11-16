// Copyright 2020 DataStax
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client_test

import (
	"github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewPreparedStatementHandler(t *testing.T) {

	query := "SELECT v FROM ks.t1 WHERE pk = ?"

	// bound variables in the prepared statement (pk)
	var variables = &message.VariablesMetadata{
		PkIndices: []uint16{0},
		Columns: []*message.ColumnMetadata{{
			Keyspace: "ks",
			Table:    "t1",
			Name:     "pk",
			Index:    0,
			Type:     datatype.Varchar,
		}},
	}

	// columns in each row returned by the statement execution (v)
	var columns = &message.RowsMetadata{
		ColumnCount: 1,
		Columns: []*message.ColumnMetadata{{
			Keyspace: "ks",
			Table:    "t1",
			Name:     "v",
			Index:    0,
			Type:     datatype.Varchar,
		}},
	}

	var pk1 = primitive.NewValue([]byte("pk1"))
	var pk2 = primitive.NewValue([]byte("pk2"))

	var v1 = message.Row{message.Column("v1")}
	var v2 = message.Row{message.Column("v2")}

	// if bound variable pk = pk1 then EXECUTE should return v1, otherwise EXECUTE should return v2
	rows := func(options *message.QueryOptions) message.RowSet {
		value := options.PositionalValues[0]
		if string(value.Contents) == "pk1" {
			return message.RowSet{v1}
		} else {
			return message.RowSet{v2}
		}
	}

	handler := client.NewPreparedStatementHandler(query, variables, columns, rows)

	server, clientConn, cancelFn := createServerAndClient(t, handler)

	testUnprepared(t, clientConn, query)
	testPrepare(t, clientConn, query, variables, columns)
	testExecute(t, clientConn, query, columns, pk1, v1)
	testExecute(t, clientConn, query, columns, pk2, v2)

	cancelFn()
	checkClosed(t, clientConn, server)

}

func testUnprepared(
	t *testing.T,
	clientConn *client.CqlClientConnection,
	query string,
) {
	execute, _ := frame.NewRequestFrame(
		primitive.ProtocolVersion4,
		client.ManagedStreamId,
		false,
		nil,
		&message.Execute{QueryId: []byte(query)},
		false,
	)
	response, err := clientConn.SendAndReceive(execute)
	require.NotNil(t, response)
	require.Nil(t, err)
	require.Equal(t, primitive.OpCodeError, response.Header.OpCode)
	require.IsType(t, &message.Unprepared{}, response.Body.Message)
	result := response.Body.Message.(*message.Unprepared)
	require.Equal(t, []byte(query), result.Id)
}

func testPrepare(
	t *testing.T,
	clientConn *client.CqlClientConnection,
	query string,
	variables *message.VariablesMetadata,
	columns *message.RowsMetadata,
) {
	prepare, _ := frame.NewRequestFrame(
		primitive.ProtocolVersion4,
		client.ManagedStreamId,
		false,
		nil,
		&message.Prepare{Query: query},
		false,
	)
	response, err := clientConn.SendAndReceive(prepare)
	require.NotNil(t, response)
	require.Nil(t, err)
	require.Equal(t, primitive.OpCodeResult, response.Header.OpCode)
	require.IsType(t, &message.PreparedResult{}, response.Body.Message)
	result := response.Body.Message.(*message.PreparedResult)
	require.Equal(t, []byte(query), result.PreparedQueryId)
	require.Equal(t, variables, result.VariablesMetadata)
	require.Equal(t, columns, result.ResultMetadata)

}

func testExecute(
	t *testing.T,
	clientConn *client.CqlClientConnection,
	query string,
	columns *message.RowsMetadata,
	pk *primitive.Value,
	row message.Row,
) {
	execute, _ := frame.NewRequestFrame(
		primitive.ProtocolVersion4,
		client.ManagedStreamId,
		false,
		nil,
		&message.Execute{
			QueryId: []byte(query),
			Options: &message.QueryOptions{PositionalValues: []*primitive.Value{pk}},
		},
		false,
	)
	response, err := clientConn.SendAndReceive(execute)
	require.NotNil(t, response)
	require.Nil(t, err)
	require.Equal(t, primitive.OpCodeResult, response.Header.OpCode)
	require.IsType(t, &message.RowsResult{}, response.Body.Message)
	result := response.Body.Message.(*message.RowsResult)
	require.Equal(t, message.RowSet{row}, result.Data)
	require.Equal(t, columns, result.Metadata)
}
