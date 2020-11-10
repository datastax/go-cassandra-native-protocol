package client_test

import (
	"context"
	"encoding/binary"
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

// This test requires a remote server listening on localhost:9042 without authentication.
func TestRemoteServerNoAuth(t *testing.T) {
	if !remoteAvailable {
		t.Skip("No remote cluster available")
	}
	for _, version := range primitive.AllProtocolVersions() {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {

			for genName, generator := range streamIdGenerators {
				t.Run(fmt.Sprintf("generator %v", genName), func(t *testing.T) {

					for compressor, frameCodec := range codecs {
						t.Run(fmt.Sprintf("compression %v", compressor), func(t *testing.T) {

							clt := client.NewCqlClient("127.0.0.1:9042", nil)
							clt.Codec = frameCodec

							clientTest(t, clt, version, generator, compressor != "NONE", false)

						})
					}
				})
			}
		})
	}
}

// This test requires a remote server listening on localhost:9042 with authentication.
func TestRemoteServerAuth(t *testing.T) {
	if !remoteAvailable {
		t.Skip("No remote cluster available")
	}
	for _, version := range primitive.AllProtocolVersions() {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {

			for genName, generator := range streamIdGenerators {
				t.Run(fmt.Sprintf("generator %v", genName), func(t *testing.T) {

					for compressor, frameCodec := range codecs {
						t.Run(fmt.Sprintf("compression %v", compressor), func(t *testing.T) {

							clt := client.NewCqlClient(
								"127.0.0.1:9042",
								&client.AuthCredentials{Username: "cassandra", Password: "cassandra"},
							)
							clt.Codec = frameCodec

							clientTest(t, clt, version, generator, compressor != "NONE", false)
						})
					}
				})
			}
		})
	}
}

// This test requires a remote DSE 5.1+ server listening on localhost:9042 with authentication.
func TestRemoteDseServerAuthContinuousPaging(t *testing.T) {
	if !remoteAvailable {
		t.Skip("No remote cluster available")
	}
	for _, version := range primitive.AllDseProtocolVersions() {
		t.Run(fmt.Sprintf("version %v", version), func(t *testing.T) {

			for genName, generator := range streamIdGenerators {
				t.Run(fmt.Sprintf("generator %v", genName), func(t *testing.T) {

					for compressor, frameCodec := range codecs {
						t.Run(fmt.Sprintf("compression %v", compressor), func(t *testing.T) {

							clt := client.NewCqlClient(
								"127.0.0.1:9042",
								&client.AuthCredentials{Username: "cassandra", Password: "cassandra"},
							)
							clt.Codec = frameCodec

							clientTest(t, clt, version, generator, compressor != "NONE", true)

						})
					}
				})
			}
		})
	}
}

func clientTest(
	t *testing.T,
	clt *client.CqlClient,
	version primitive.ProtocolVersion,
	generator func(int) int16,
	compress bool,
	continuousPaging bool,
) {
	ks := fmt.Sprintf("ks_%d", time.Now().UnixNano())
	table := fmt.Sprintf("t_%d", time.Now().UnixNano())

	ctx, cancel := context.WithCancel(context.Background())

	clientConn, err := clt.ConnectAndInit(ctx, version, generator(1))
	require.Nil(t, err)

	createSchema(t, clientConn, ks, table, version, generator, compress)
	insertData(t, clientConn, ks, table, version, generator, compress)
	if continuousPaging {
		retrieveDataContinuousPaging(t, clientConn, ks, table, version, generator, compress)
	} else {
		retrieveData(t, clientConn, ks, table, version, generator, compress)
	}
	dropSchema(t, clientConn, ks, version, generator, compress)

	cancel()
	assert.Eventually(t, clientConn.IsClosed, time.Second*10, time.Millisecond*10)

}

func createSchema(
	t *testing.T,
	clientConn *client.CqlClientConnection,
	ks string,
	table string,
	version primitive.ProtocolVersion,
	generator func(int) int16,
	compress bool,
) {
	request, _ := frame.NewRequestFrame(
		version,
		generator(1),
		false,
		nil,
		&message.Query{
			Query: fmt.Sprintf("CREATE KEYSPACE %s "+
				"WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} "+
				"AND durable_writes = true", ks),
		},
		compress,
	)
	response, err := clientConn.SendAndReceive(request)
	require.Nil(t, err)
	require.IsType(t, &message.SchemaChangeResult{}, response.Body.Message)
	result := response.Body.Message.(*message.SchemaChangeResult)
	require.Equal(t, result.ChangeType, primitive.SchemaChangeTypeCreated)
	require.Equal(t, result.Target, primitive.SchemaChangeTargetKeyspace)
	require.Equal(t, result.Keyspace, ks)
	require.Equal(t, result.Object, "")
	request, _ = frame.NewRequestFrame(
		version,
		generator(1),
		false,
		nil,
		&message.Query{
			Query: fmt.Sprintf("CREATE TABLE %s.%s "+
				"(pk int, cc int, v int, PRIMARY KEY (pk, cc))", ks, table),
		},
		compress,
	)
	response, err = clientConn.SendAndReceive(request)
	require.Nil(t, err)
	require.IsType(t, &message.SchemaChangeResult{}, response.Body.Message)
	result = response.Body.Message.(*message.SchemaChangeResult)
	require.Equal(t, result.ChangeType, primitive.SchemaChangeTypeCreated)
	require.Equal(t, result.Target, primitive.SchemaChangeTargetTable)
	require.Equal(t, result.Keyspace, ks)
	require.Equal(t, result.Object, table)
}

func dropSchema(
	t *testing.T,
	clientConn *client.CqlClientConnection,
	ks string,
	version primitive.ProtocolVersion,
	generator func(int) int16,
	compress bool,
) {
	request, _ := frame.NewRequestFrame(
		version,
		generator(1),
		false,
		nil,
		&message.Query{
			Query: fmt.Sprintf("DROP KEYSPACE %s", ks),
		},
		compress,
	)
	response, err := clientConn.SendAndReceive(request)
	require.Nil(t, err)
	require.IsType(t, &message.SchemaChangeResult{}, response.Body.Message)
	result := response.Body.Message.(*message.SchemaChangeResult)
	require.Equal(t, result.ChangeType, primitive.SchemaChangeTypeDropped)
	require.Equal(t, result.Target, primitive.SchemaChangeTargetKeyspace)
	require.Equal(t, result.Keyspace, ks)
	require.Equal(t, result.Object, "")
}

func insertData(
	t *testing.T,
	clientConn *client.CqlClientConnection,
	ks string,
	table string,
	version primitive.ProtocolVersion,
	generator func(int) int16,
	compress bool,
) {
	wg := &sync.WaitGroup{}
	for i := 1; i <= 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 1; j <= 10; j++ {
				pk := make([]byte, 4)
				cc := make([]byte, 4)
				v := make([]byte, 4)
				binary.BigEndian.PutUint32(pk, uint32(i))
				binary.BigEndian.PutUint32(cc, uint32(j))
				binary.BigEndian.PutUint32(v, uint32(i)*uint32(j))
				request, _ := frame.NewRequestFrame(
					version,
					generator(i),
					false,
					nil,
					&message.Query{
						Query: fmt.Sprintf("INSERT INTO %s.%s (pk, cc, v) VALUES (?,?,?)", ks, table),
						Options: &message.QueryOptions{
							Consistency: primitive.ConsistencyLevelOne,
							PositionalValues: []*primitive.Value{
								primitive.NewValue(pk),
								primitive.NewValue(cc),
								primitive.NewValue(v),
							},
						},
					},
					compress,
				)
				response, err := clientConn.SendAndReceive(request)
				require.Nil(t, err)
				require.IsType(t, &message.VoidResult{}, response.Body.Message)
			}
		}(i)
	}
	wg.Wait()
}

func retrieveData(
	t *testing.T,
	clientConn *client.CqlClientConnection,
	ks string,
	table string,
	version primitive.ProtocolVersion,
	generator func(int) int16,
	compress bool,
) {
	wg := &sync.WaitGroup{}
	for i := 1; i <= 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 1; j <= 10; j++ {
				pk := make([]byte, 4)
				cc := make([]byte, 4)
				binary.BigEndian.PutUint32(pk, uint32(i))
				binary.BigEndian.PutUint32(cc, uint32(j))
				request, _ := frame.NewRequestFrame(
					version,
					generator(i),
					false,
					nil,
					&message.Query{
						Query: fmt.Sprintf("SELECT v FROM %s.%s WHERE pk = ? AND cc = ?", ks, table),
						Options: &message.QueryOptions{
							Consistency: primitive.ConsistencyLevelOne,
							PositionalValues: []*primitive.Value{
								primitive.NewValue(pk),
								primitive.NewValue(cc),
							},
						},
					},
					compress,
				)
				response, err := clientConn.SendAndReceive(request)
				require.Nil(t, err)
				require.IsType(t, &message.RowsResult{}, response.Body.Message)
				result := response.Body.Message.(*message.RowsResult)
				require.Len(t, result.Data, 1)
				row := result.Data[0]
				require.Len(t, row, 1)
				column := row[0]
				require.Len(t, column, 4)
				v := binary.BigEndian.Uint32(column)
				require.Equal(t, uint32(i*j), v)
			}
		}(i)
	}
	wg.Wait()
}

func retrieveDataContinuousPaging(t *testing.T, clientConn *client.CqlClientConnection, ks string, table string, version primitive.ProtocolVersion, generator func(int) int16, compress bool) {
	request, _ := frame.NewRequestFrame(
		version,
		generator(1),
		false,
		nil,
		&message.Query{
			Query: fmt.Sprintf("SELECT v FROM %s.%s", ks, table),
			Options: &message.QueryOptions{
				Consistency:             primitive.ConsistencyLevelLocalOne,
				PageSize:                100,
				ContinuousPagingOptions: &message.ContinuousPagingOptions{MaxPages: 5},
			},
		},
		compress,
	)

	ch, err := clientConn.Send(request)
	require.Nil(t, err)

	for i := 1; i <= 5; i++ {
		f, err := clientConn.Receive(ch)
		require.Nil(t, err)
		require.NotNil(t, f)
		require.IsType(t, &message.RowsResult{}, f.Body.Message)
		result := f.Body.Message.(*message.RowsResult)
		require.Equal(t, result.Metadata.ContinuousPageNumber, int32(i))
		require.Equal(t, result.Metadata.LastContinuousPage, i == 5)
		require.Len(t, result.Data, 100)
	}

	request, _ = frame.NewRequestFrame(
		version,
		generator(1),
		false,
		nil,
		&message.Revise{
			RevisionType:   primitive.DseRevisionTypeCancelContinuousPaging,
			TargetStreamId: int32(request.Header.StreamId),
		},
		compress,
	)

	response, err := clientConn.SendAndReceive(request)
	require.Nil(t, err)
	require.IsType(t, &message.RowsResult{}, response.Body.Message)
	result := response.Body.Message.(*message.RowsResult)
	require.Len(t, result.Data, 1)
	row := result.Data[0]
	require.Len(t, row, 1)
	column := row[0]
	require.Len(t, column, 1) // boolean
}
