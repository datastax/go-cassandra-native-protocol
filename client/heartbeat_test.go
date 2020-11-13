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

	server := client.NewCqlServer("127.0.0.1:9043", nil)
	server.RequestHandlers = []client.RequestHandler{client.HeartbeatHandler}

	clt := client.NewCqlClient("127.0.0.1:9043", nil)

	ctx, cancelFn := context.WithCancel(context.Background())

	err := server.Start(ctx)
	require.Nil(t, err)

	clientConn, err := clt.Connect(ctx)
	require.Nil(t, err)
	require.NotNil(t, clientConn)

	heartbeat, _ := frame.NewRequestFrame(
		primitive.ProtocolVersion4,
		client.ManagedStreamId,
		false,
		nil,
		&message.Options{},
		false,
	)

	for i := 0; i < 100; i++ {
		response, err := clientConn.SendAndReceive(heartbeat)
		require.NotNil(t, response)
		require.Nil(t, err)
		require.Equal(t, primitive.OpCodeSupported, response.Header.OpCode)
		require.IsType(t, &message.Supported{}, response.Body.Message)
	}

	cancelFn()

	assert.Eventually(t, clientConn.IsClosed, time.Second*10, time.Millisecond*10)
	assert.Eventually(t, server.IsClosed, time.Second*10, time.Millisecond*10)

}
