package client_test

import (
	"context"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
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

							err := server.Start()
							require.Nil(t, err)
							defer func() {
								_ = server.Close()
							}()

							clientConn, serverConn, err := server.BindAndInit(clt, version, client.ManagedStreamId)
							require.Nil(t, err)

							wg := &sync.WaitGroup{}
							ctx, cancelFn := context.WithCancel(context.Background())

							defer func() {
								cancelFn()
								_ = clientConn.Close()
								_ = serverConn.Close()
								wg.Wait()
							}()

							playServer(serverConn, version, compressor, ctx, wg)
							playClient(t, clientConn, version, compressor, generator)

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
	wg *sync.WaitGroup,
) {
	wg.Add(1)
	go func() {
		defer wg.Done()
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
	for i := 0; i < 100; i++ {
		outgoing, _ := frame.NewRequestFrame(
			version,
			generateStreamId(1),
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
}
