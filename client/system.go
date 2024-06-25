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

package client

import (
	"bytes"
	"net"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

// Creates a new RequestHandler to handle queries to system tables (system.local and system.peers).
func NewSystemTablesHandler(cluster string, datacenter string) RequestHandler {
	return func(request *frame.Frame, conn *CqlServerConnection, _ RequestHandlerContext) (response *frame.Frame) {
		if query, ok := request.Body.Message.(*message.Query); ok {
			q := strings.TrimSpace(strings.ToLower(query.Query))
			q = strings.Join(strings.Fields(q), " ") // remove extra whitespace
			if strings.HasPrefix(q, "select * from system.local") {
				log.Debug().Msgf("%v: [system tables handler]: returning full system.local", conn)
				response = fullSystemLocal(cluster, datacenter, request, conn)
			} else if strings.HasPrefix(q, "select schema_version from system.local") {
				log.Debug().Msgf("%v: [system tables handler]: returning schema_version", conn)
				response = schemaVersion(request)
			} else if strings.HasPrefix(q, "select cluster_name from system.local") {
				log.Debug().Msgf("%v: [system tables handler]: returning cluster_name", conn)
				response = clusterName(cluster, request)
			} else if strings.Contains(q, "from system.peers") {
				log.Debug().Msgf("%v: [system tables handler]: returning empty system.peers", conn)
				response = emptySystemPeers(request)
			}
		}
		return
	}
}

var (
	keyColumn              = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "key", Type: datatype.Varchar}
	broadcastAddressColumn = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "broadcast_address", Type: datatype.Inet}
	clusterNameColumn      = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "cluster_name", Type: datatype.Varchar}
	cqlVersionColumn       = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "cql_version", Type: datatype.Varchar}
	datacenterColumn       = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "data_center", Type: datatype.Varchar}
	hostIdColumn           = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "host_id", Type: datatype.Uuid}
	listenAddressColumn    = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "listen_address", Type: datatype.Inet}
	partitionerColumn      = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "partitioner", Type: datatype.Varchar}
	rackColumn             = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "rack", Type: datatype.Varchar}
	releaseVersionColumn   = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "release_version", Type: datatype.Varchar}
	rpcAddressColumn       = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "rpc_address", Type: datatype.Inet}
	schemaVersionColumn    = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "schema_version", Type: datatype.Uuid}
	tokensColumn           = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "tokens", Type: datatype.NewSet(datatype.Varchar)}
)

// These columns are a subset of the total columns returned by OSS C* 3.11.2, and contain all the information that
// drivers need in order to establish the cluster topology and determine its characteristics.
var systemLocalColumns = []*message.ColumnMetadata{
	keyColumn,
	broadcastAddressColumn,
	clusterNameColumn,
	cqlVersionColumn,
	datacenterColumn,
	hostIdColumn,
	listenAddressColumn,
	partitionerColumn,
	rackColumn,
	releaseVersionColumn,
	rpcAddressColumn,
	schemaVersionColumn,
	tokensColumn,
}

var (
	keyValue            = message.Column("local")
	cqlVersionValue     = message.Column("3.4.4")
	hostIdValue         = message.Column{0xC0, 0xD1, 0xD2, 0x1E, 0xBB, 0x01, 0x41, 0x96, 0x86, 0xDB, 0xBC, 0x31, 0x7B, 0xC1, 0x79, 0x6A}
	partitionerValue    = message.Column("org.apache.cassandra.dht.Murmur3Partitioner")
	rackValue           = message.Column("rack1")
	releaseVersionValue = message.Column("3.11.2")
	schemaVersionValue  = message.Column{0xC0, 0xD1, 0xD2, 0x1E, 0xBB, 0x01, 0x41, 0x96, 0x86, 0xDB, 0xBC, 0x31, 0x7B, 0xC1, 0x79, 0x6A}
)

func systemLocalRow(cluster string, datacenter string, addr net.Addr, version primitive.ProtocolVersion) message.Row {
	addrBuf := &bytes.Buffer{}
	// TODO replace the serialization code below when data type serialization is implemented
	inetAddr := addr.(*net.TCPAddr).IP
	if inetAddr.To4() != nil {
		addrBuf.Write(inetAddr.To4())
	} else {
		addrBuf.Write(inetAddr)
	}
	// emulate {'-9223372036854775808'} (entire ring)
	tokensBuf := &bytes.Buffer{}
	if version >= primitive.ProtocolVersion3 {
		_ = primitive.WriteInt(1, tokensBuf)
		_ = primitive.WriteInt(int32(len("-9223372036854775808")), tokensBuf)
	} else {
		_ = primitive.WriteShort(1, tokensBuf)
		_ = primitive.WriteShort(uint16(len("-9223372036854775808")), tokensBuf)
	}
	tokensBuf.WriteString("-9223372036854775808")
	return message.Row{
		keyValue,
		addrBuf.Bytes(),
		message.Column(cluster),
		cqlVersionValue,
		message.Column(datacenter),
		hostIdValue,
		addrBuf.Bytes(),
		partitionerValue,
		rackValue,
		releaseVersionValue,
		addrBuf.Bytes(),
		schemaVersionValue,
		tokensBuf.Bytes(),
	}
}

func fullSystemLocal(cluster string, datacenter string, request *frame.Frame, conn *CqlServerConnection) *frame.Frame {
	systemLocalRow := systemLocalRow(cluster, datacenter, conn.LocalAddr(), request.Header.Version)
	msg := &message.RowsResult{
		Metadata: &message.RowsMetadata{
			ColumnCount: int32(len(systemLocalColumns)),
			Columns:     systemLocalColumns,
		},
		Data: message.RowSet{systemLocalRow},
	}
	return frame.NewFrame(request.Header.Version, request.Header.StreamId, msg)
}

func schemaVersion(request *frame.Frame) *frame.Frame {
	msg := &message.RowsResult{
		Metadata: &message.RowsMetadata{
			ColumnCount: 1,
			Columns:     []*message.ColumnMetadata{schemaVersionColumn},
		},
		Data: message.RowSet{message.Row{schemaVersionValue}},
	}
	return frame.NewFrame(request.Header.Version, request.Header.StreamId, msg)
}

func clusterName(cluster string, request *frame.Frame) *frame.Frame {
	msg := &message.RowsResult{
		Metadata: &message.RowsMetadata{
			ColumnCount: 1,
			Columns:     []*message.ColumnMetadata{clusterNameColumn},
		},
		Data: message.RowSet{message.Row{message.Column(cluster)}},
	}
	return frame.NewFrame(request.Header.Version, request.Header.StreamId, msg)
}

func emptySystemPeers(request *frame.Frame) *frame.Frame {
	msg := &message.RowsResult{
		Metadata: &message.RowsMetadata{ColumnCount: 0},
		Data:     message.RowSet{},
	}
	return frame.NewFrame(request.Header.Version, request.Header.StreamId, msg)
}
