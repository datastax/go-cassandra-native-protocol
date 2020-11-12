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
