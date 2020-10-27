package client

import (
	"flag"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/compression"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/message"
	"github.com/stretchr/testify/assert"
	"testing"
)

var compressors = map[string]*frame.Codec{
	"LZ4":    frame.NewCodec(frame.WithCompressor(&compression.Lz4Compressor{})),
	"SNAPPY": frame.NewCodec(frame.WithCompressor(&compression.SnappyCompressor{})),
	"NONE":   frame.NewCodec(),
}

var ccmAvailable bool

func init() {
	flag.BoolVar(&ccmAvailable, "ccm", false, "whether a CCM cluster is available on localhost:9042")
}

// This test requires a remote server listening on localhost:9042 without authentication.
func TestRemoteServerNoAuth(t *testing.T) {
	if !ccmAvailable {
		t.Skip("No CCM cluster available")
	}
	for _, version := range cassandraprotocol.AllProtocolVersions() {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {

			for compressor, frameCodec := range compressors {
				t.Run(fmt.Sprintf("compression %v", compressor), func(t *testing.T) {

					client := NewCqlClient("127.0.0.1:9042", frameCodec)
					clientConn, err := client.Connect()
					if err != nil {
						return // skip test, no remote server available
					}
					defer func() { _ = clientConn.Close() }()

					err = Handshake(clientConn, version, 1)
					assert.Nil(t, err)

					request, _ := frame.NewRequestFrame(version, 1, false, nil, &message.Query{
						Query:   "SELECT * FROM system.local",
						Options: message.NewQueryOptions(),
					})
					err = clientConn.Send(request)
					assert.Nil(t, err)
					fmt.Printf("CLIENT sent:     %v\n", request)

					response, err := clientConn.Receive()
					assert.Nil(t, err)
					assert.NotNil(t, response)
					assert.IsType(t, &message.RowsResult{}, response.Body.Message)
					fmt.Printf("CLIENT received: %v\n", response)
				})
			}
		})
	}
}

// This test requires a remote server listening on localhost:9042 with authentication.
func TestRemoteServerAuth(t *testing.T) {
	if !ccmAvailable {
		t.Skip("No CCM cluster available")
	}
	for _, version := range cassandraprotocol.AllProtocolVersions() {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {

			for compressor, frameCodec := range compressors {
				t.Run(fmt.Sprintf("compression %v", compressor), func(t *testing.T) {

					client := NewCqlClient("127.0.0.1:9042", frameCodec)
					clientConn, err := client.Connect()
					if err != nil {
						return // skip test, no remote server available
					}
					defer func() { _ = clientConn.Close() }()

					err = HandshakeAuth(clientConn, version, 1, "cassandra", "cassandra")
					assert.Nil(t, err)

					query, _ := frame.NewRequestFrame(version, 1, false, nil, &message.Query{
						Query:   "SELECT * FROM system.local",
						Options: message.NewQueryOptions(),
					})
					err = clientConn.Send(query)
					assert.Nil(t, err)
					fmt.Printf("CLIENT sent:     %v\n", query)

					rows, err := clientConn.Receive()
					assert.Nil(t, err)
					assert.NotNil(t, rows)
					assert.IsType(t, &message.RowsResult{}, rows.Body.Message)
					fmt.Printf("CLIENT received: %v\n", rows)
				})
			}
		})
	}
}

// This test requires a remote DSE 5.1+ server listening on localhost:9042 with authentication.
func TestRemoteDseServerAuthContinuousPaging(t *testing.T) {
	if !ccmAvailable {
		t.Skip("No CCM cluster available")
	}
	for _, version := range cassandraprotocol.AllDseProtocolVersions() {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {

			for compressor, frameCodec := range compressors {
				t.Run(fmt.Sprintf("compression %v", compressor), func(t *testing.T) {

					client := NewCqlClient("127.0.0.1:9042", frameCodec)
					clientConn, err := client.Connect()
					if err != nil {
						return // skip test, no remote server available
					}
					defer func() { _ = clientConn.Close() }()

					err = HandshakeAuth(clientConn, version, 1, "cassandra", "cassandra")
					assert.Nil(t, err)

					query, _ := frame.NewRequestFrame(version, 1, false, nil, &message.Query{
						Query: "SELECT * FROM system_schema.columns",
						Options: message.NewQueryOptions(
							message.WithPageSize(1),
							message.WithContinuousPagingOptions(&message.ContinuousPagingOptions{MaxPages: 3})),
					})
					err = clientConn.Send(query)
					assert.Nil(t, err)
					fmt.Printf("CLIENT sent:     %v\n", query)

					response, err := clientConn.Receive()
					assert.Nil(t, err)
					assert.NotNil(t, response)
					assert.Equal(t, response.Header.StreamId, int16(1))
					assert.IsType(t, &message.RowsResult{}, response.Body.Message)
					fmt.Printf("CLIENT received: %v\n", response)
					rows := response.Body.Message.(*message.RowsResult)
					assert.Equal(t, rows.Metadata.ContinuousPageNumber, int32(1))
					assert.Equal(t, rows.Metadata.LastContinuousPage, false)

					response, err = clientConn.Receive()
					assert.Nil(t, err)
					assert.NotNil(t, response)
					assert.Equal(t, response.Header.StreamId, int16(1))
					assert.IsType(t, &message.RowsResult{}, response.Body.Message)
					fmt.Printf("CLIENT received: %v\n", response)
					rows = response.Body.Message.(*message.RowsResult)
					assert.Equal(t, rows.Metadata.ContinuousPageNumber, int32(2))
					assert.Equal(t, rows.Metadata.LastContinuousPage, false)

					response, err = clientConn.Receive()
					assert.Nil(t, err)
					assert.NotNil(t, response)
					assert.Equal(t, response.Header.StreamId, int16(1))
					assert.IsType(t, &message.RowsResult{}, response.Body.Message)
					fmt.Printf("CLIENT received: %v\n", response)
					rows = response.Body.Message.(*message.RowsResult)
					assert.Equal(t, rows.Metadata.ContinuousPageNumber, int32(3))
					assert.Equal(t, rows.Metadata.LastContinuousPage, true)

					cancel, _ := frame.NewRequestFrame(version, 2, false, nil, &message.Revise{
						RevisionType:   cassandraprotocol.DseRevisionTypeCancelContinuousPaging,
						TargetStreamId: 1,
					})
					err = clientConn.Send(cancel)
					assert.Nil(t, err)
					fmt.Printf("CLIENT sent:     %v\n", cancel)

					response, err = clientConn.Receive()
					assert.Nil(t, err)
					assert.NotNil(t, response)
					assert.Equal(t, response.Header.StreamId, int16(2))
					assert.IsType(t, &message.RowsResult{}, response.Body.Message)
					fmt.Printf("CLIENT received: %v\n", response)

				})
			}
		})
	}
}

func TestLocalServer(t *testing.T) {

	for _, version := range cassandraprotocol.AllProtocolVersions() {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {

			for compressor, frameCodec := range compressors {
				t.Run(fmt.Sprintf("compression %v", compressor), func(t *testing.T) {

					server := NewCqlServer("127.0.0.1:9043", frameCodec)
					defer func() { _ = server.Close() }()

					client := NewCqlClient("127.0.0.1:9043", frameCodec)

					clientConn, serverConn, err := server.Bind(client)
					assert.Nil(t, err)

					defer func() { _ = clientConn.Close(); _ = serverConn.Close() }()

					msg := NewStartupRequest(clientConn, version, 1)
					_ = clientConn.Send(msg)
					fmt.Printf("CLIENT sent:     %v\n", msg)

					msg, _ = serverConn.Receive()
					assert.NotNil(t, msg)
					assert.IsType(t, &message.Startup{}, msg.Body.Message)
					fmt.Printf("SERVER received: %v\n", msg)

					msg, _ = frame.NewResponseFrame(version, 1, nil, nil, nil, &message.Ready{})
					_ = serverConn.Send(msg)
					fmt.Printf("SERVER sent:     %v\n", msg)

					msg, _ = clientConn.Receive()
					assert.NotNil(t, msg)
					assert.IsType(t, &message.Ready{}, msg.Body.Message)
					fmt.Printf("CLIENT received: %v\n", msg)

					msg, _ = frame.NewRequestFrame(version, 1, false, nil, &message.Query{
						Query:   "SELECT * FROM system.local",
						Options: message.NewQueryOptions(),
					})
					err = clientConn.Send(msg)
					fmt.Printf("CLIENT sent:     %v\n", msg)

					msg, _ = serverConn.Receive()
					assert.NotNil(t, msg)
					assert.IsType(t, &message.Query{}, msg.Body.Message)
					fmt.Printf("SERVER received: %v\n", msg)

					msg, _ = frame.NewResponseFrame(
						version,
						1,
						nil,
						nil,
						nil,
						message.NewRowsResult(message.WithRowsMetadata(
							message.NewRowsMetadata(message.NoColumnMetadata(1))),
							message.WithRowsData(
								[][]byte{
									{0, 0, 0, 4, 1, 2, 3, 4},
								},
								[][]byte{
									{0, 0, 0, 4, 5, 6, 7, 8},
								},
							),
						),
					)
					_ = serverConn.Send(msg)
					fmt.Printf("SERVER sent:     %v\n", msg)

					msg, _ = clientConn.Receive()
					assert.NotNil(t, msg)
					assert.IsType(t, &message.RowsResult{}, msg.Body.Message)
					fmt.Printf("CLIENT received: %v\n", msg)
				})
			}
		})
	}
}

func TestLocalServerDiscardBody(t *testing.T) {

	for _, version := range cassandraprotocol.AllProtocolVersions() {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {

			for compressor, frameCodec := range compressors {
				t.Run(fmt.Sprintf("compression %v", compressor), func(t *testing.T) {

					server := NewCqlServer("127.0.0.1:9043", frameCodec)
					defer func() { _ = server.Close() }()

					client := NewCqlClient("127.0.0.1:9043", frameCodec)

					clientConn, serverConn, err := server.Bind(client)
					assert.Nil(t, err)

					defer func() { _ = clientConn.Close(); _ = serverConn.Close() }()

					msg := NewStartupRequest(clientConn, version, 1)
					_ = clientConn.Send(msg)
					fmt.Printf("CLIENT sent:     %v\n", msg)

					header, _ := serverConn.ReceiveHeader()
					assert.NotNil(t, header)
					fmt.Printf("SERVER received: %v\n", header)

					msg, _ = frame.NewResponseFrame(version, 1, nil, nil, nil, &message.Ready{})
					_ = serverConn.Send(msg)
					fmt.Printf("SERVER sent:     %v\n", msg)

					header, _ = clientConn.ReceiveHeader()
					assert.NotNil(t, header)
					fmt.Printf("CLIENT received: %v\n", header)

					msg, _ = frame.NewRequestFrame(version, 1, false, nil, &message.Query{
						Query:   "SELECT * FROM system.local",
						Options: message.NewQueryOptions(),
					})
					err = clientConn.Send(msg)
					fmt.Printf("CLIENT sent:     %v\n", msg)

					header, _ = serverConn.ReceiveHeader()
					assert.NotNil(t, header)
					fmt.Printf("SERVER received: %v\n", header)

					msg, _ = frame.NewResponseFrame(
						version,
						1,
						nil,
						nil,
						nil,
						message.NewRowsResult(message.WithRowsMetadata(
							message.NewRowsMetadata(message.NoColumnMetadata(1))),
							message.WithRowsData(
								[][]byte{
									{0, 0, 0, 4, 1, 2, 3, 4},
								},
								[][]byte{
									{0, 0, 0, 4, 5, 6, 7, 8},
								},
							),
						),
					)
					_ = serverConn.Send(msg)
					fmt.Printf("SERVER sent:     %v\n", msg)

					header, _ = clientConn.ReceiveHeader()
					assert.NotNil(t, header)
					fmt.Printf("CLIENT received: %v\n", header)
				})
			}
		})
	}
}
