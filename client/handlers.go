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
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
)

// A RequestHandler to handle server-side heartbeats. This handler assumes that every OPTIONS request is a heartbeat
// probe and replies with a SUPPORTED response.
var HeartbeatHandler RequestHandler = func(request *frame.Frame, conn *CqlServerConnection, _ RequestHandlerContext) (response *frame.Frame) {
	if _, ok := request.Body.Message.(*message.Options); ok {
		log.Debug().Msgf("%v: [heartbeat handler]: received heartbeat probe", conn)
		response = frame.NewFrame(request.Header.Version, request.Header.StreamId, &message.Supported{})
	}
	return
}

// A RequestHandler to handle USE queries. This handler intercepts QUERY requests with a USE statement and replies
// with a message.SetKeyspaceResult. The provided callback function will be invoked with the new keyspace.
func NewSetKeyspaceHandler(onKeyspaceSet func(string)) RequestHandler {
	return func(request *frame.Frame, conn *CqlServerConnection, _ RequestHandlerContext) (response *frame.Frame) {
		if query, ok := request.Body.Message.(*message.Query); ok {
			q := strings.TrimSpace(strings.ToLower(query.Query))
			q = strings.Join(strings.Fields(q), " ")
			if strings.HasPrefix(q, "use ") {
				keyspace := strings.TrimPrefix(q, "use ")
				onKeyspaceSet(keyspace)
				log.Debug().Msgf("%v: [set keyspace handler]: received USE %v", conn, keyspace)
				response = frame.NewFrame(request.Header.Version, request.Header.StreamId, &message.SetKeyspaceResult{Keyspace: keyspace})
			}
		}
		return
	}
}

// A RequestHandler to handle USE requests. This handler intercepts REGISTER requests and replies with READY.
var RegisterHandler RequestHandler = func(request *frame.Frame, conn *CqlServerConnection, _ RequestHandlerContext) (response *frame.Frame) {
	if register, ok := request.Body.Message.(*message.Register); ok {
		log.Debug().Msgf("%v: [register handler]: received REGISTER: %v", conn, register.EventTypes)
		response = frame.NewFrame(request.Header.Version, request.Header.StreamId, &message.Ready{})
	}
	return
}

// Creates a new composite RequestHandler combining many child handlers together. The child handlers are invoked in
// order. Registering a composite handler is functionally equivalent to the individual registration of its child
// handlers, but allows to ensure that handlers that are supposed to work together are all registered and invoked in
// proper order.
func NewCompositeRequestHandler(handlers ...RequestHandler) RequestHandler {
	return func(request *frame.Frame, conn *CqlServerConnection, ctx RequestHandlerContext) (response *frame.Frame) {
		for _, handler := range handlers {
			response = handler(request, conn, ctx)
			if response != nil {
				break
			}
		}
		return
	}
}

// A RequestHandler to fully initialize a connection initiated by a DataStax driver. This handler intercepts all the
// requests that a driver typically issues when opening a new connection and / or probing for its liveness:
// - Heartbeats
// - Handshake, including with plain-text authentication if configured
// - USE queries
// - REGISTER requests
// - Queries targeting system.local and system.peers tables
func NewDriverConnectionInitializationHandler(cluster string, datacenter string, onKeyspaceSet func(string)) RequestHandler {
	return NewCompositeRequestHandler(
		HeartbeatHandler,
		HandshakeHandler,
		NewSetKeyspaceHandler(onKeyspaceSet),
		RegisterHandler,
		NewSystemTablesHandler(cluster, datacenter),
	)
}
