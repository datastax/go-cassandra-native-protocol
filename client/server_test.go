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
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestCqlServer_Accept(t *testing.T) {

	server := client.NewCqlServer("127.0.0.1:9043", nil)

	clt1 := client.NewCqlClient("127.0.0.1:9043", nil)
	clt2 := client.NewCqlClient("127.0.0.1:9043", nil)

	ctx, cancelFn := context.WithCancel(context.Background())

	err := server.Start(ctx)
	require.Nil(t, err)

	clientConn1, err := clt1.Connect(ctx)
	require.Nil(t, err)
	require.NotNil(t, clientConn1)

	clientConn2, err := clt2.Connect(ctx)
	require.Nil(t, err)
	require.NotNil(t, clientConn2)

	serverConn1, err := server.Accept(clientConn1)
	require.Nil(t, err)
	require.NotNil(t, serverConn1)

	serverConn2, err := server.Accept(clientConn2)
	require.Nil(t, err)
	require.NotNil(t, serverConn2)

	require.Equal(t, serverConn1.RemoteAddr(), clientConn1.LocalAddr())
	require.Equal(t, serverConn2.RemoteAddr(), clientConn2.LocalAddr())
	require.Equal(t, serverConn1.LocalAddr(), clientConn1.RemoteAddr())
	require.Equal(t, serverConn2.LocalAddr(), clientConn2.RemoteAddr())

	cancelFn()

	assert.Eventually(t, clientConn1.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, clientConn2.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, serverConn1.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, serverConn2.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, server.IsClosed, time.Second*10, time.Millisecond*10)
}

func TestCqlServer_AcceptAny(t *testing.T) {

	server := client.NewCqlServer("127.0.0.1:9043", nil)

	clt1 := client.NewCqlClient("127.0.0.1:9043", nil)
	clt2 := client.NewCqlClient("127.0.0.1:9043", nil)

	ctx, cancelFn := context.WithCancel(context.Background())

	err := server.Start(ctx)
	require.Nil(t, err)

	clientConn1, err := clt1.Connect(ctx)
	require.Nil(t, err)
	require.NotNil(t, clientConn1)

	serverConn1, err := server.AcceptAny()
	require.Nil(t, err)
	require.NotNil(t, serverConn1)

	clientConn2, err := clt2.Connect(ctx)
	require.Nil(t, err)
	require.NotNil(t, clientConn2)

	serverConn2, err := server.AcceptAny()
	require.Nil(t, err)
	require.NotNil(t, serverConn2)

	require.Equal(t, serverConn1.RemoteAddr(), clientConn1.LocalAddr())
	require.Equal(t, serverConn2.RemoteAddr(), clientConn2.LocalAddr())
	require.Equal(t, serverConn1.LocalAddr(), clientConn1.RemoteAddr())
	require.Equal(t, serverConn2.LocalAddr(), clientConn2.RemoteAddr())

	cancelFn()

	assert.Eventually(t, clientConn1.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, clientConn2.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, serverConn1.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, serverConn2.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, server.IsClosed, time.Second*10, time.Millisecond*10)
}

func TestCqlServer_AllAcceptedClients(t *testing.T) {

	server := client.NewCqlServer("127.0.0.1:9043", nil)

	clt1 := client.NewCqlClient("127.0.0.1:9043", nil)
	clt2 := client.NewCqlClient("127.0.0.1:9043", nil)

	ctx, cancelFn := context.WithCancel(context.Background())

	err := server.Start(ctx)
	require.Nil(t, err)

	clientConn1, err := clt1.Connect(ctx)
	require.Nil(t, err)
	require.NotNil(t, clientConn1)

	clientConn2, err := clt2.Connect(ctx)
	require.Nil(t, err)
	require.NotNil(t, clientConn2)

	serverConn1, err := server.Accept(clientConn1)
	require.Nil(t, err)
	require.NotNil(t, serverConn1)

	serverConn2, err := server.Accept(clientConn2)
	require.Nil(t, err)
	require.NotNil(t, serverConn2)

	clients, err := server.AllAcceptedClients()
	require.Nil(t, err)
	require.NotNil(t, clients)
	require.Len(t, clients, 2)

	cancelFn()

	assert.Eventually(t, clientConn1.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, clientConn2.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, clients[0].IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, clients[1].IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, server.IsClosed, time.Second*10, time.Millisecond*10)
}

func TestCqlServer_Bind(t *testing.T) {

	server := client.NewCqlServer("127.0.0.1:9043", nil)

	clt1 := client.NewCqlClient("127.0.0.1:9043", nil)
	clt2 := client.NewCqlClient("127.0.0.1:9043", nil)

	ctx, cancelFn := context.WithCancel(context.Background())

	err := server.Start(ctx)
	require.Nil(t, err)

	clientConn1, serverConn1, err := server.Bind(clt1, ctx)
	require.Nil(t, err)
	require.NotNil(t, clientConn1)
	require.NotNil(t, serverConn1)

	clientConn2, serverConn2, err := server.Bind(clt2, ctx)
	require.Nil(t, err)
	require.NotNil(t, clientConn2)
	require.NotNil(t, serverConn2)

	require.Equal(t, serverConn1.RemoteAddr(), clientConn1.LocalAddr())
	require.Equal(t, serverConn2.RemoteAddr(), clientConn2.LocalAddr())
	require.Equal(t, serverConn1.LocalAddr(), clientConn1.RemoteAddr())
	require.Equal(t, serverConn2.LocalAddr(), clientConn2.RemoteAddr())

	cancelFn()

	assert.Eventually(t, clientConn1.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, clientConn2.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, serverConn1.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, serverConn2.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, server.IsClosed, time.Second*10, time.Millisecond*10)
}

func TestCqlServer_BindAndInit(t *testing.T) {

	server := client.NewCqlServer("127.0.0.1:9043", nil)

	clt1 := client.NewCqlClient("127.0.0.1:9043", nil)
	clt2 := client.NewCqlClient("127.0.0.1:9043", nil)

	ctx, cancelFn := context.WithCancel(context.Background())

	err := server.Start(ctx)
	require.Nil(t, err)

	clientConn1, serverConn1, err := server.BindAndInit(clt1, ctx, primitive.ProtocolVersion4, client.ManagedStreamId)
	require.Nil(t, err)
	require.NotNil(t, clientConn1)
	require.NotNil(t, serverConn1)

	clientConn2, serverConn2, err := server.Bind(clt2, ctx)
	require.Nil(t, err)
	require.NotNil(t, clientConn2)
	require.NotNil(t, serverConn2)

	require.Equal(t, serverConn1.RemoteAddr(), clientConn1.LocalAddr())
	require.Equal(t, serverConn2.RemoteAddr(), clientConn2.LocalAddr())
	require.Equal(t, serverConn1.LocalAddr(), clientConn1.RemoteAddr())
	require.Equal(t, serverConn2.LocalAddr(), clientConn2.RemoteAddr())

	cancelFn()

	assert.Eventually(t, clientConn1.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, clientConn2.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, serverConn1.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, serverConn2.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, server.IsClosed, time.Second*10, time.Millisecond*10)
}
