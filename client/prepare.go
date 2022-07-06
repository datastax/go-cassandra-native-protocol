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
	"github.com/rs/zerolog/log"

	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
)

// A RequestHandler to handle PREPARE and EXECUTE requests for the given query string, effectively emulating the
// behavior of a statement being prepared, then executed.
// When a PREPARE request targets the query string, it is intercepted and the handler replies with a PreparedResult.
// The prepared id is simply the query string bytes, and the metadata is the metadata provided to the function.
// When an EXECUTE request targets the same query string:
// - If the request was previously prepared, returns a Rows RESULT response; the actual data returned is produced by
// invoking the provided rows factory function, which allows the result to be customized according to the bound
// variables provided with the EXECUTE message.
// - If the request was not prepared, returns an Unprepared ERROR response.
func NewPreparedStatementHandler(
	query string,
	variables *message.VariablesMetadata,
	columns *message.RowsMetadata,
	rows func(options *message.QueryOptions) message.RowSet,
) RequestHandler {
	prepared := false
	return func(request *frame.Frame, conn *CqlServerConnection, ctx RequestHandlerContext) (response *frame.Frame) {
		version := request.Header.Version
		id := request.Header.StreamId
		switch msg := request.Body.Message.(type) {
		case *message.Prepare:
			if msg.Query == query {
				log.Debug().Msgf("%v: [prepare handler]: intercepted PREPARE", conn)
				result := &message.PreparedResult{
					PreparedQueryId:   []byte(query),
					VariablesMetadata: variables,
					ResultMetadata:    columns,
				}
				prepared = true
				response = frame.NewFrame(version, id, result)
				log.Debug().Msgf("%v: [prepare handler]: returning %v", conn, response)
			}
		case *message.Execute:
			if string(msg.QueryId) == query {
				log.Debug().Msgf("%v: [prepare handler]: intercepted EXECUTE", conn)
				if prepared {
					result := &message.RowsResult{
						Metadata: columns,
						Data:     rows(msg.Options),
					}
					response = frame.NewFrame(version, id, result)
				} else {
					result := &message.Unprepared{
						ErrorMessage: "Unprepared query: " + query,
						Id:           []byte(query),
					}
					response = frame.NewFrame(version, id, result)
				}
				log.Debug().Msgf("%v: [prepare handler]: returning %v", conn, response)
			}
		}
		return
	}
}
