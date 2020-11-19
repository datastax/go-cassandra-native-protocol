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
	"context"
	"github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestHeartbeatHandler(t *testing.T) {

	server, clientConn, cancelFn := createServerAndClient(t, client.HeartbeatHandler)

	testHeartbeat(t, clientConn)

	cancelFn()
	checkClosed(t, clientConn, server)

}

func TestSetKeyspaceHandler(t *testing.T) {

	onKeyspaceSetCalled := false
	var onKeyspaceSet = func(keyspace string) {
		require.Equal(t, "ks1", keyspace)
		onKeyspaceSetCalled = true
	}
	server, clientConn, cancelFn := createServerAndClient(t, client.NewSetKeyspaceHandler(onKeyspaceSet))

	testUseQuery(t, clientConn)
	require.True(t, onKeyspaceSetCalled)

	cancelFn()
	checkClosed(t, clientConn, server)

}

func TestRegisterHandler(t *testing.T) {

	server, clientConn, cancelFn := createServerAndClient(t, client.RegisterHandler)

	testRegister(t, clientConn)

	cancelFn()
	checkClosed(t, clientConn, server)

}

func TestNewCompositeRequestHandler(t *testing.T) {

	handler := client.NewCompositeRequestHandler(client.HeartbeatHandler, client.RegisterHandler)
	server, clientConn, cancelFn := createServerAndClient(t, handler)

	testHeartbeat(t, clientConn)
	testRegister(t, clientConn)

	cancelFn()
	checkClosed(t, clientConn, server)

}

func TestNewDriverConnectionInitializationHandler(t *testing.T) {

	var onKeyspaceSet = func(keyspace string) {
		require.Equal(t, "ks1", keyspace)
	}
	handler := client.NewDriverConnectionInitializationHandler("cluster_test", "datacenter_test", onKeyspaceSet)
	server, clientConn, cancelFn := createServerAndClient(t, handler)

	err := clientConn.InitiateHandshake(primitive.ProtocolVersion4, client.ManagedStreamId)
	require.Nil(t, err)

	testHeartbeat(t, clientConn)
	testRegister(t, clientConn)
	testUseQuery(t, clientConn)
	testClusterName(t, clientConn)
	testSchemaVersion(t, clientConn)
	testFullSystemLocal(t, clientConn)
	testSystemPeers(t, clientConn)

	cancelFn()
	checkClosed(t, clientConn, server)

}

func createServerAndClient(t *testing.T, handlers ...client.RequestHandler) (*client.CqlServer, *client.CqlClientConnection, context.CancelFunc) {
	server := client.NewCqlServer("127.0.0.1:9043", nil)
	server.RequestHandlers = handlers
	clt := client.NewCqlClient("127.0.0.1:9043", nil)
	ctx, cancelFn := context.WithCancel(context.Background())
	err := server.Start(ctx)
	require.Nil(t, err)
	clientConn, err := clt.Connect(ctx)
	require.Nil(t, err)
	require.NotNil(t, clientConn)
	return server, clientConn, cancelFn
}

func checkClosed(t *testing.T, clientConn *client.CqlClientConnection, server *client.CqlServer) {
	assert.Eventually(t, clientConn.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, server.IsClosed, time.Second*10, time.Millisecond*10)
}

func testHeartbeat(t *testing.T, clientConn *client.CqlClientConnection) {
	heartbeat := frame.NewFrame(
		primitive.ProtocolVersion4,
		client.ManagedStreamId,
		&message.Options{},
	)
	for i := 0; i < 100; i++ {
		response, err := clientConn.SendAndReceive(heartbeat)
		require.NotNil(t, response)
		require.Nil(t, err)
		require.Equal(t, primitive.OpCodeSupported, response.Header.OpCode)
		require.IsType(t, &message.Supported{}, response.Body.Message)
	}
}

func testUseQuery(t *testing.T, clientConn *client.CqlClientConnection) {
	useQuery := frame.NewFrame(
		primitive.ProtocolVersion4,
		client.ManagedStreamId,
		&message.Query{Query: " USE \n ks1 "},
	)
	response, err := clientConn.SendAndReceive(useQuery)
	require.NotNil(t, response)
	require.Nil(t, err)
	require.Equal(t, primitive.OpCodeResult, response.Header.OpCode)
	require.IsType(t, &message.SetKeyspaceResult{}, response.Body.Message)
	result := response.Body.Message.(*message.SetKeyspaceResult)
	require.Equal(t, "ks1", result.Keyspace)
}

func testRegister(t *testing.T, clientConn *client.CqlClientConnection) {
	register := frame.NewFrame(
		primitive.ProtocolVersion4,
		client.ManagedStreamId,
		&message.Register{EventTypes: []primitive.EventType{primitive.EventTypeSchemaChange, primitive.EventTypeTopologyChange}},
	)
	response, err := clientConn.SendAndReceive(register)
	require.NotNil(t, response)
	require.Nil(t, err)
	require.Equal(t, primitive.OpCodeReady, response.Header.OpCode)
	require.IsType(t, &message.Ready{}, response.Body.Message)
}
