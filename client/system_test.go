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
	"bytes"
	"github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewSystemTablesHandler_FullSystemLocal(t *testing.T) {

	handler := client.NewSystemTablesHandler("cluster_test", "datacenter_test")
	server, clientConn, cancelFunc := createServerAndClient(t, handler)

	testFullSystemLocal(t, clientConn)

	cancelFunc()
	checkClosed(t, clientConn, server)

}

func TestNewSystemTablesHandler_SchemaVersion(t *testing.T) {

	handler := client.NewSystemTablesHandler("cluster_test", "datacenter_test")
	server, clientConn, cancelFunc := createServerAndClient(t, handler)

	testSchemaVersion(t, clientConn)

	cancelFunc()
	checkClosed(t, clientConn, server)
}

func TestNewSystemTablesHandler_ClusterName(t *testing.T) {

	handler := client.NewSystemTablesHandler("cluster_test", "datacenter_test")
	server, clientConn, cancelFunc := createServerAndClient(t, handler)

	testClusterName(t, clientConn)

	cancelFunc()
	checkClosed(t, clientConn, server)
}

func TestNewSystemTablesHandler_EmptySystemPeers(t *testing.T) {

	handler := client.NewSystemTablesHandler("cluster_test", "datacenter_test")
	server, clientConn, cancelFunc := createServerAndClient(t, handler)

	testSystemPeers(t, clientConn)

	cancelFunc()
	checkClosed(t, clientConn, server)
}

func testFullSystemLocal(t *testing.T, clientConn *client.CqlClientConnection) {
	fullSystemLocalQuery, _ := frame.NewRequestFrame(
		primitive.ProtocolVersion4,
		client.ManagedStreamId,
		false,
		nil,
		&message.Query{
			Query: " SELECT  * \n FROM  \t system.local WHERE key = 'local'",
		},
		false,
	)

	response, err := clientConn.SendAndReceive(fullSystemLocalQuery)
	require.NotNil(t, response)
	require.Nil(t, err)
	require.Equal(t, primitive.OpCodeResult, response.Header.OpCode)
	require.IsType(t, &message.RowsResult{}, response.Body.Message)

	rowsResult := response.Body.Message.(*message.RowsResult)
	require.EqualValues(t, rowsResult.Metadata.ColumnCount, 13)
	require.Len(t, rowsResult.Metadata.Columns, 13)
	require.Len(t, rowsResult.Data, 1)
	require.Len(t, rowsResult.Data[0], 13)
}

func testSchemaVersion(t *testing.T, clientConn *client.CqlClientConnection) {
	schemaVersionQuery, _ := frame.NewRequestFrame(
		primitive.ProtocolVersion4,
		client.ManagedStreamId,
		false,
		nil,
		&message.Query{
			Query: " SELECT  schema_version \n FROM  \t system.local WHERE key = 'local'",
		},
		false,
	)

	response, err := clientConn.SendAndReceive(schemaVersionQuery)
	require.NotNil(t, response)
	require.Nil(t, err)
	require.Equal(t, primitive.OpCodeResult, response.Header.OpCode)
	require.IsType(t, &message.RowsResult{}, response.Body.Message)

	rowsResult := response.Body.Message.(*message.RowsResult)
	require.Len(t, rowsResult.Data, 1)
	require.Len(t, rowsResult.Data[0], 1)

	column := rowsResult.Data[0][0]
	_, err = primitive.ReadUuid(bytes.NewBuffer(column))
	require.Nil(t, err)
}

func testClusterName(t *testing.T, clientConn *client.CqlClientConnection) {
	clusterNameQuery, _ := frame.NewRequestFrame(
		primitive.ProtocolVersion4,
		client.ManagedStreamId,
		false,
		nil,
		&message.Query{
			Query: " SELECT   cluster_name \n FROM  \t system.local",
		},
		false,
	)

	response, err := clientConn.SendAndReceive(clusterNameQuery)
	require.NotNil(t, response)
	require.Nil(t, err)
	require.Equal(t, primitive.OpCodeResult, response.Header.OpCode)
	require.IsType(t, &message.RowsResult{}, response.Body.Message)

	rowsResult := response.Body.Message.(*message.RowsResult)
	require.Len(t, rowsResult.Data, 1)
	require.Len(t, rowsResult.Data[0], 1)

	column := rowsResult.Data[0][0]
	require.Equal(t, "cluster_test", string(column))
}

func testSystemPeers(t *testing.T, clientConn *client.CqlClientConnection) {
	systemPeersQuery, _ := frame.NewRequestFrame(
		primitive.ProtocolVersion4,
		client.ManagedStreamId,
		false,
		nil,
		&message.Query{
			Query: " SELECT  * \n FROM  \t system.peers",
		},
		false,
	)

	response, err := clientConn.SendAndReceive(systemPeersQuery)
	require.NotNil(t, response)
	require.Nil(t, err)
	require.Equal(t, primitive.OpCodeResult, response.Header.OpCode)
	require.IsType(t, &message.RowsResult{}, response.Body.Message)

	rowsResult := response.Body.Message.(*message.RowsResult)
	require.EqualValues(t, rowsResult.Metadata.ColumnCount, 0)
	require.Nil(t, rowsResult.Metadata.Columns)
	require.Len(t, rowsResult.Data, 0)
}
