package client_test

import (
	"context"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestLocalServer(t *testing.T) {

	for _, version := range primitive.AllProtocolVersions() {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {

			for genName, generator := range streamIdGenerators {
				t.Run(fmt.Sprintf("generator %v", genName), func(t *testing.T) {

					for compressor, frameCodec := range codecs {
						t.Run(fmt.Sprintf("compression %v", compressor), func(t *testing.T) {

							server := client.NewCqlServer(
								"127.0.0.1:9043",
								&client.AuthCredentials{
									Username: "cassandra",
									Password: "cassandra",
								},
							)
							server.Codec = frameCodec

							clt := client.NewCqlClient(
								"127.0.0.1:9043",
								&client.AuthCredentials{
									Username: "cassandra",
									Password: "cassandra",
								},
							)
							clt.Codec = frameCodec

							ctx, cancelFn := context.WithCancel(context.Background())

							err := server.Start(ctx)
							require.Nil(t, err)

							clientConn, serverConn, err := server.BindAndInit(clt, ctx, version, client.ManagedStreamId)
							require.Nil(t, err)

							playServer(serverConn, version, compressor, ctx)
							playClient(t, clientConn, version, compressor, generator)

							cancelFn()

							assert.Eventually(t, clientConn.IsClosed, time.Second*10, time.Millisecond*10)
							assert.Eventually(t, serverConn.IsClosed, time.Second*10, time.Millisecond*10)
							assert.Eventually(t, server.IsClosed, time.Second*10, time.Millisecond*10)

						})
					}
				})
			}
		})
	}
}

func playServer(
	serverConn *client.CqlServerConnection,
	version primitive.ProtocolVersion,
	compressor string,
	ctx context.Context,
) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				incoming, err := serverConn.Receive()
				if err != nil {
					return
				}
				outgoing, _ := frame.NewResponseFrame(
					version,
					incoming.Header.StreamId,
					nil,
					nil,
					nil,
					&message.RowsResult{
						Metadata: &message.RowsMetadata{ColumnCount: 1},
						Data: message.RowSet{
							message.Row{
								message.Column{0, 0, 0, 4, 1, 2, 3, 4},
							},
							message.Row{
								message.Column{0, 0, 0, 4, 5, 6, 7, 8},
							},
						},
					},
					compressor != "NONE",
				)
				err = serverConn.Send(outgoing)
				if err != nil {
					return
				}
			}
		}
	}()
}

func playClient(
	t *testing.T,
	clientConn *client.CqlClientConnection,
	version primitive.ProtocolVersion,
	compressor string,
	generateStreamId func(int) int16,
) {
	wg := &sync.WaitGroup{}
	for i := 1; i <= 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 1; j <= 10; j++ {
				outgoing, _ := frame.NewRequestFrame(
					version,
					generateStreamId(i),
					false,
					nil,
					&message.Query{
						Query:   "SELECT * FROM system.local",
						Options: &message.QueryOptions{},
					},
					compressor != "NONE",
				)
				incoming, err := clientConn.SendAndReceive(outgoing)
				require.Nil(t, err)
				require.NotNil(t, incoming)
			}

		}(i)
	}
	wg.Wait()
}
