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

func TestHandshakeHandler_NoAuth(t *testing.T) {

	server := client.NewCqlServer("127.0.0.1:9043", nil)
	server.RequestHandlers = []client.RequestHandler{client.HandshakeHandler}

	clt := client.NewCqlClient("127.0.0.1:9043", nil)

	ctx, cancelFn := context.WithCancel(context.Background())

	err := server.Start(ctx)
	require.Nil(t, err)

	clientConn, err := clt.Connect(ctx)
	require.Nil(t, err)
	require.NotNil(t, clientConn)

	err = clientConn.InitiateHandshake(primitive.ProtocolVersion4, client.ManagedStreamId)
	require.Nil(t, err)

	cancelFn()

	assert.Eventually(t, clientConn.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, server.IsClosed, time.Second*10, time.Millisecond*10)

}

func TestHandshakeHandler_Auth(t *testing.T) {

	server := client.NewCqlServer("127.0.0.1:9043", &client.AuthCredentials{
		Username: "user1",
		Password: "pass1",
	})
	server.RequestHandlers = []client.RequestHandler{client.HandshakeHandler}

	clt := client.NewCqlClient("127.0.0.1:9043", &client.AuthCredentials{
		Username: "user1",
		Password: "pass1",
	})

	ctx, cancelFn := context.WithCancel(context.Background())

	err := server.Start(ctx)
	require.Nil(t, err)

	clientConn, err := clt.Connect(ctx)
	require.Nil(t, err)
	require.NotNil(t, clientConn)

	err = clientConn.InitiateHandshake(primitive.ProtocolVersion4, client.ManagedStreamId)
	require.Nil(t, err)

	cancelFn()

	assert.Eventually(t, clientConn.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, server.IsClosed, time.Second*10, time.Millisecond*10)

}
