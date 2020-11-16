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
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/rs/zerolog/log"
)

// A RequestHandler to handle server-side heartbeats. This handler assumes that every OPTIONS request is a heartbeat
// probe and replies with a SUPPORTED response.
func HeartbeatHandler(request *frame.Frame, conn *CqlServerConnection, _ RequestHandlerContext) (response *frame.Frame) {
	if _, ok := request.Body.Message.(*message.Options); ok {
		log.Debug().Msgf("%v: [heartbeat handler]: received heartbeat probe", conn)
		response, _ = frame.NewResponseFrame(request.Header.Version, request.Header.StreamId, nil, nil, nil, &message.Supported{}, false)
	}
	return
}
