package client_test

import (
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestLocalServer(t *testing.T) {

	for _, version := range primitive.AllProtocolVersions() {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {

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

					clientConn, serverConn, err := server.BindAndInit(clt, version)
					require.Nil(t, err)

					defer func() {
						_ = clientConn.Close()
						_ = serverConn.Close()
					}()

					// client -> server
					outgoing, _ := frame.NewRequestFrame(
						version,
						0,
						false,
						nil,
						&message.Query{
							Query:   "SELECT * FROM system.local",
							Options: &message.QueryOptions{},
						},
						compressor != "NONE",
					)
					ch, err := clientConn.Send(outgoing)
					require.Nil(t, err)

					incoming, err := serverConn.Receive()
					require.Nil(t, err)
					require.NotNil(t, incoming)
					require.Equal(t, outgoing, incoming)

					// server -> client
					outgoing, _ = frame.NewResponseFrame(
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
					require.Nil(t, err)

					incoming, err = clientConn.Receive(ch)
					require.Nil(t, err)
					require.NotNil(t, incoming)
					require.Equal(t, outgoing, incoming)
				})
			}
		})
	}
}
