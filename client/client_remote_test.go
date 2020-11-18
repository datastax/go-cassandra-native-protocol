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
	"encoding/binary"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math"
	"sync"
	"testing"
	"time"
)

// This test requires a remote server listening on localhost:9042 without authentication.
func TestRemoteServerNoAuth(t *testing.T) {
	if !remoteAvailable {
		t.Skip("No remote cluster available")
	}
	for _, version := range primitive.AllNonBetaProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {

			for genName, generator := range streamIdGenerators {
				t.Run(fmt.Sprintf("generator %v", genName), func(t *testing.T) {

					for compressor, frameCodec := range codecs {
						t.Run(fmt.Sprintf("compression %v", compressor), func(t *testing.T) {

							clt := client.NewCqlClient("127.0.0.1:9042", nil)
							clt.Codec = frameCodec
							if version <= primitive.ProtocolVersion2 {
								clt.MaxInFlight = math.MaxInt8
							}
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
	for _, version := range primitive.AllNonBetaProtocolVersions() {
		t.Run(version.String(), func(t *testing.T) {

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
		t.Run(version.String(), func(t *testing.T) {

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
	generator func(int, primitive.ProtocolVersion) int16,
	compress bool,
	continuousPaging bool,
) {
	ks := fmt.Sprintf("ks_%d", time.Now().UnixNano())
	table := fmt.Sprintf("t_%d", time.Now().UnixNano())

	eventsCount := 0
	clt.EventHandlers = []client.EventHandler{checkEventsReceived(t, &eventsCount, ks, table)}

	ctx, cancel := context.WithCancel(context.Background())

	clientConn, err := clt.ConnectAndInit(ctx, version, generator(1, version))
	require.Nil(t, err)
	require.False(t, clientConn.IsClosed())

	registerForSchemaChanges(t, clientConn, version, generator, compress)
	createSchema(t, clientConn, ks, table, version, generator, compress)

	require.GreaterOrEqual(t, eventsCount, 2)

	insertData(t, clientConn, ks, table, version, generator, compress)
	insertDataBatch(t, clientConn, ks, table, version, generator, compress)
	insertDataPrepared(t, clientConn, ks, table, version, generator, compress)
	insertDataBatchPrepared(t, clientConn, ks, table, version, generator, compress)

	retrieveData(t, clientConn, ks, table, version, generator, compress)
	retrieveDataPrepared(t, clientConn, ks, table, version, generator, compress)
	if continuousPaging {
		retrieveDataContinuousPaging(t, clientConn, ks, table, version, generator, compress)
	}

	dropSchema(t, clientConn, ks, version, generator, compress)
	checkEventsByChannel(t, clientConn)
	require.GreaterOrEqual(t, eventsCount, 4)

	cancel()
	assert.Eventually(t, clientConn.IsClosed, time.Second*10, time.Millisecond*10)

}

func checkEventsReceived(t *testing.T, eventsCount *int, ks string, table string) client.EventHandler {
	return func(event *frame.Frame, conn *client.CqlClientConnection) {
		assert.Equal(t, primitive.OpCodeEvent, event.Header.OpCode)
		assert.EqualValues(t, -1, event.Header.StreamId)
		assert.IsType(t, &message.SchemaChangeEvent{}, event.Body.Message)
		sce := event.Body.Message.(*message.SchemaChangeEvent)
		if *eventsCount == 0 {
			assert.Equal(t, primitive.SchemaChangeTargetKeyspace, sce.Target)
			assert.Equal(t, primitive.SchemaChangeTypeCreated, sce.ChangeType)
			assert.Equal(t, ks, sce.Keyspace)
			assert.Equal(t, "", sce.Object)
		} else if *eventsCount == 1 {
			assert.Equal(t, primitive.SchemaChangeTargetTable, sce.Target)
			assert.Equal(t, primitive.SchemaChangeTypeCreated, sce.ChangeType)
			assert.Equal(t, ks, sce.Keyspace)
			assert.Equal(t, table, sce.Object)
		} else {
			assert.Equal(t, primitive.SchemaChangeTypeDropped, sce.ChangeType)
		}
		*eventsCount++
	}
}

func checkEventsByChannel(t *testing.T, clientConn *client.CqlClientConnection) {
	// expect 4 events, 2 CREATED, 2 DROPPED // 2 KEYSPACE, 2 TABLE
	created := 0
	dropped := 0
	ks := 0
	table := 0
	for i := 0; i < 4; i++ {
		event, err := clientConn.ReceiveEvent()
		require.Nil(t, err)
		assert.EqualValues(t, -1, event.Header.StreamId)
		assert.IsType(t, &message.SchemaChangeEvent{}, event.Body.Message)
		sce := event.Body.Message.(*message.SchemaChangeEvent)
		switch sce.ChangeType {
		case primitive.SchemaChangeTypeCreated:
			created++
		case primitive.SchemaChangeTypeDropped:
			dropped++
		}
		switch sce.Target {
		case primitive.SchemaChangeTargetKeyspace:
			ks++
		case primitive.SchemaChangeTargetTable:
			table++
		}
	}
	assert.Equal(t, 2, created)
	assert.Equal(t, 2, dropped)
	assert.Equal(t, 2, ks)
	assert.Equal(t, 2, table)
}

func registerForSchemaChanges(
	t *testing.T,
	clientConn *client.CqlClientConnection,
	version primitive.ProtocolVersion,
	generator func(int, primitive.ProtocolVersion) int16,
	compress bool,
) {
	request := frame.NewFrame(
		version,
		generator(1, version),
		&message.Register{EventTypes: []primitive.EventType{primitive.EventTypeSchemaChange}},
	)
	request.SetCompress(compress)
	response, err := clientConn.SendAndReceive(request)
	require.Nil(t, err)
	require.IsType(t, &message.Ready{}, response.Body.Message)
}

func createSchema(
	t *testing.T,
	clientConn *client.CqlClientConnection,
	ks string,
	table string,
	version primitive.ProtocolVersion,
	generator func(int, primitive.ProtocolVersion) int16,
	compress bool,
) {
	request := frame.NewFrame(
		version,
		generator(1, version),
		&message.Query{
			Query: fmt.Sprintf("CREATE KEYSPACE %s "+
				"WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} "+
				"AND durable_writes = true", ks),
		},
	)
	request.SetCompress(compress)
	response, err := clientConn.SendAndReceive(request)
	require.Nil(t, err)
	require.IsType(t, &message.SchemaChangeResult{}, response.Body.Message)
	result := response.Body.Message.(*message.SchemaChangeResult)
	require.Equal(t, result.ChangeType, primitive.SchemaChangeTypeCreated)
	require.Equal(t, result.Target, primitive.SchemaChangeTargetKeyspace)
	require.Equal(t, result.Keyspace, ks)
	require.Equal(t, result.Object, "")
	request = frame.NewFrame(
		version,
		generator(1, version),
		&message.Query{
			Query: fmt.Sprintf("CREATE TABLE %s.%s "+
				"(pk int, cc int, v int, PRIMARY KEY (pk, cc))", ks, table),
		},
	)
	request.SetCompress(compress)
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
	generator func(int, primitive.ProtocolVersion) int16,
	compress bool,
) {
	request := frame.NewFrame(
		version,
		generator(1, version),
		&message.Query{
			Query: fmt.Sprintf("DROP KEYSPACE %s", ks),
		},
	)
	request.SetCompress(compress)
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
	generator func(int, primitive.ProtocolVersion) int16,
	compress bool,
) {
	wg := &sync.WaitGroup{}
	for i := 1; i <= 10; i++ {
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
				request := frame.NewFrame(
					version,
					generator(i, version),
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
				)
				request.SetCompress(compress)
				response, err := clientConn.SendAndReceive(request)
				require.Nil(t, err)
				require.IsType(t, &message.VoidResult{}, response.Body.Message)
			}
		}(i)
	}
	wg.Wait()
}

func insertDataPrepared(
	t *testing.T,
	clientConn *client.CqlClientConnection,
	ks string,
	table string,
	version primitive.ProtocolVersion,
	generator func(int, primitive.ProtocolVersion) int16,
	compress bool,
) {
	prepared := prepareInsert(t, clientConn, ks, table, version, generator, compress)
	wg := &sync.WaitGroup{}
	for i := 1; i <= 10; i++ {
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
				request := frame.NewFrame(
					version,
					generator(i, version),
					&message.Execute{
						QueryId: prepared.PreparedQueryId,
						Options: &message.QueryOptions{
							Consistency: primitive.ConsistencyLevelOne,
							PositionalValues: []*primitive.Value{
								primitive.NewValue(pk),
								primitive.NewValue(cc),
								primitive.NewValue(v),
							},
						},
					},
				)
				request.SetCompress(compress)
				response, err := clientConn.SendAndReceive(request)
				require.Nil(t, err)
				require.IsType(t, &message.VoidResult{}, response.Body.Message)
			}
		}(i)
	}
	wg.Wait()
}

func insertDataBatch(
	t *testing.T,
	clientConn *client.CqlClientConnection,
	ks string,
	table string,
	version primitive.ProtocolVersion,
	generator func(int, primitive.ProtocolVersion) int16,
	compress bool,
) {
	wg := &sync.WaitGroup{}
	for i := 1; i <= 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			children := make([]*message.BatchChild, 10)
			for j := 1; j <= 10; j++ {
				pk := make([]byte, 4)
				cc := make([]byte, 4)
				v := make([]byte, 4)
				binary.BigEndian.PutUint32(pk, uint32(i))
				binary.BigEndian.PutUint32(cc, uint32(j))
				binary.BigEndian.PutUint32(v, uint32(i)*uint32(j))
				children[j-1] = &message.BatchChild{
					QueryOrId: fmt.Sprintf("INSERT INTO %s.%s (pk, cc, v) VALUES (?,?,?)", ks, table),
					Values: []*primitive.Value{
						primitive.NewValue(pk),
						primitive.NewValue(cc),
						primitive.NewValue(v),
					},
				}
			}
			request := frame.NewFrame(
				version,
				generator(i, version),
				&message.Batch{
					Consistency: primitive.ConsistencyLevelLocalOne,
					Children:    children,
				},
			)
			request.SetCompress(compress)
			response, err := clientConn.SendAndReceive(request)
			require.Nil(t, err)
			require.IsType(t, &message.VoidResult{}, response.Body.Message)
		}(i)
	}
	wg.Wait()
}

func insertDataBatchPrepared(
	t *testing.T,
	clientConn *client.CqlClientConnection,
	ks string,
	table string,
	version primitive.ProtocolVersion,
	generator func(int, primitive.ProtocolVersion) int16,
	compress bool,
) {
	prepared := prepareInsert(t, clientConn, ks, table, version, generator, compress)
	wg := &sync.WaitGroup{}
	for i := 1; i <= 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			children := make([]*message.BatchChild, 10)
			for j := 1; j <= 10; j++ {
				pk := make([]byte, 4)
				cc := make([]byte, 4)
				v := make([]byte, 4)
				binary.BigEndian.PutUint32(pk, uint32(i))
				binary.BigEndian.PutUint32(cc, uint32(j))
				binary.BigEndian.PutUint32(v, uint32(i)*uint32(j))
				children[j-1] = &message.BatchChild{
					QueryOrId: prepared.PreparedQueryId,
					Values: []*primitive.Value{
						primitive.NewValue(pk),
						primitive.NewValue(cc),
						primitive.NewValue(v),
					},
				}
			}
			request := frame.NewFrame(
				version,
				generator(i, version),
				&message.Batch{
					Consistency: primitive.ConsistencyLevelLocalOne,
					Children:    children,
				},
			)
			request.SetCompress(compress)
			response, err := clientConn.SendAndReceive(request)
			require.Nil(t, err)
			require.IsType(t, &message.VoidResult{}, response.Body.Message)

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
	generator func(int, primitive.ProtocolVersion) int16,
	compress bool,
) {
	wg := &sync.WaitGroup{}
	for i := 1; i <= 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 1; j <= 10; j++ {
				pk := make([]byte, 4)
				cc := make([]byte, 4)
				binary.BigEndian.PutUint32(pk, uint32(i))
				binary.BigEndian.PutUint32(cc, uint32(j))
				request := frame.NewFrame(
					version,
					generator(i, version),
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
				)
				request.SetCompress(compress)
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

func retrieveDataPrepared(
	t *testing.T,
	clientConn *client.CqlClientConnection,
	ks string,
	table string,
	version primitive.ProtocolVersion,
	generator func(int, primitive.ProtocolVersion) int16,
	compress bool,
) {
	prepared := prepareSelect(t, clientConn, ks, table, version, generator, compress)
	wg := &sync.WaitGroup{}
	for i := 1; i <= 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 1; j <= 10; j++ {
				pk := make([]byte, 4)
				cc := make([]byte, 4)
				binary.BigEndian.PutUint32(pk, uint32(i))
				binary.BigEndian.PutUint32(cc, uint32(j))
				request := frame.NewFrame(
					version,
					generator(i, version),
					&message.Execute{
						QueryId: prepared.PreparedQueryId,
						Options: &message.QueryOptions{
							Consistency: primitive.ConsistencyLevelOne,
							PositionalValues: []*primitive.Value{
								primitive.NewValue(pk),
								primitive.NewValue(cc),
							},
						},
					},
				)
				request.SetCompress(compress)
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

func retrieveDataContinuousPaging(
	t *testing.T,
	clientConn *client.CqlClientConnection,
	ks string,
	table string,
	version primitive.ProtocolVersion,
	generator func(int, primitive.ProtocolVersion) int16,
	compress bool,
) {
	request := frame.NewFrame(
		version,
		generator(1, version),
		&message.Query{
			Query: fmt.Sprintf("SELECT v FROM %s.%s", ks, table),
			Options: &message.QueryOptions{
				Consistency:             primitive.ConsistencyLevelLocalOne,
				PageSize:                100,
				ContinuousPagingOptions: &message.ContinuousPagingOptions{MaxPages: 5},
			},
		},
	)
	request.SetCompress(compress)

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

	request = frame.NewFrame(
		version,
		generator(1, version),
		&message.Revise{
			RevisionType:   primitive.DseRevisionTypeCancelContinuousPaging,
			TargetStreamId: int32(request.Header.StreamId),
		},
	)
	request.SetCompress(compress)

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

func prepareInsert(
	t *testing.T,
	clientConn *client.CqlClientConnection,
	ks string,
	table string,
	version primitive.ProtocolVersion,
	generator func(int, primitive.ProtocolVersion) int16,
	compress bool,
) *message.PreparedResult {
	request := frame.NewFrame(
		version,
		generator(1, version),
		&message.Prepare{
			Query: fmt.Sprintf("INSERT INTO %s.%s (pk, cc, v) VALUES (?,?,?)", ks, table),
		},
	)
	request.SetCompress(compress)
	response, err := clientConn.SendAndReceive(request)
	require.Nil(t, err)
	require.IsType(t, &message.PreparedResult{}, response.Body.Message)
	prepared := response.Body.Message.(*message.PreparedResult)
	assert.Len(t, prepared.VariablesMetadata.Columns, 3)
	assert.Equal(t, ks, prepared.VariablesMetadata.Columns[0].Keyspace)
	assert.Equal(t, table, prepared.VariablesMetadata.Columns[0].Table)
	assert.Equal(t, "pk", prepared.VariablesMetadata.Columns[0].Name)
	assert.Equal(t, datatype.Int, prepared.VariablesMetadata.Columns[0].Type)
	assert.Equal(t, ks, prepared.VariablesMetadata.Columns[1].Keyspace)
	assert.Equal(t, table, prepared.VariablesMetadata.Columns[1].Table)
	assert.Equal(t, "cc", prepared.VariablesMetadata.Columns[1].Name)
	assert.Equal(t, datatype.Int, prepared.VariablesMetadata.Columns[1].Type)
	assert.Equal(t, ks, prepared.VariablesMetadata.Columns[2].Keyspace)
	assert.Equal(t, table, prepared.VariablesMetadata.Columns[2].Table)
	assert.Equal(t, "v", prepared.VariablesMetadata.Columns[2].Name)
	assert.Equal(t, datatype.Int, prepared.VariablesMetadata.Columns[2].Type)
	if version >= primitive.ProtocolVersion4 {
		assert.Equal(t, prepared.VariablesMetadata.PkIndices, []uint16{0})
	}
	assert.Zero(t, prepared.ResultMetadata.ColumnCount)
	assert.Nil(t, prepared.ResultMetadata.Columns)
	return prepared
}

func prepareSelect(
	t *testing.T,
	clientConn *client.CqlClientConnection,
	ks string,
	table string,
	version primitive.ProtocolVersion,
	generator func(int, primitive.ProtocolVersion) int16,
	compress bool,
) *message.PreparedResult {
	request := frame.NewFrame(
		version,
		generator(1, version),
		&message.Prepare{
			Query: fmt.Sprintf("SELECT v FROM %s.%s WHERE pk = ? AND cc = ?", ks, table),
		},
	)
	request.SetCompress(compress)
	response, err := clientConn.SendAndReceive(request)
	require.Nil(t, err)
	require.IsType(t, &message.PreparedResult{}, response.Body.Message)
	prepared := response.Body.Message.(*message.PreparedResult)
	assert.Len(t, prepared.VariablesMetadata.Columns, 2)
	assert.Equal(t, ks, prepared.VariablesMetadata.Columns[0].Keyspace)
	assert.Equal(t, table, prepared.VariablesMetadata.Columns[0].Table)
	assert.Equal(t, "pk", prepared.VariablesMetadata.Columns[0].Name)
	assert.Equal(t, datatype.Int, prepared.VariablesMetadata.Columns[0].Type)
	assert.Equal(t, ks, prepared.VariablesMetadata.Columns[1].Keyspace)
	assert.Equal(t, table, prepared.VariablesMetadata.Columns[1].Table)
	assert.Equal(t, "cc", prepared.VariablesMetadata.Columns[1].Name)
	assert.Equal(t, datatype.Int, prepared.VariablesMetadata.Columns[1].Type)
	if version >= primitive.ProtocolVersion4 {
		assert.Equal(t, prepared.VariablesMetadata.PkIndices, []uint16{0})
	}
	assert.EqualValues(t, 1, prepared.ResultMetadata.ColumnCount)
	assert.Len(t, prepared.ResultMetadata.Columns, 1)
	assert.Equal(t, ks, prepared.ResultMetadata.Columns[0].Keyspace)
	assert.Equal(t, table, prepared.ResultMetadata.Columns[0].Table)
	assert.Equal(t, "v", prepared.ResultMetadata.Columns[0].Name)
	assert.Equal(t, datatype.Int, prepared.ResultMetadata.Columns[0].Type)
	return prepared
}
