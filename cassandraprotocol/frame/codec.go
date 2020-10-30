package frame

import (
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/compression"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/message"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/primitive"
)

type Codec struct {
	compressor    compression.MessageCompressor
	messageCodecs map[primitive.OpCode]message.Codec
}

var defaultMessageCodecs = []message.Codec{
	&message.StartupCodec{},
	&message.OptionsCodec{},
	&message.QueryCodec{},
	&message.PrepareCodec{},
	&message.ExecuteCodec{},
	&message.RegisterCodec{},
	&message.BatchCodec{},
	&message.AuthResponseCodec{},
	&message.ErrorCodec{},
	&message.ReadyCodec{},
	&message.AuthenticateCodec{},
	&message.SupportedCodec{},
	&message.ResultCodec{},
	&message.EventCodec{},
	&message.AuthChallengeCodec{},
	&message.AuthSuccessCodec{},
	// DSE-specific
	&message.ReviseCodec{},
}

type CodecCustomizer func(*Codec)

func NewCodec(customizers ...CodecCustomizer) *Codec {
	frameCodec := &Codec{messageCodecs: make(map[primitive.OpCode]message.Codec, len(defaultMessageCodecs))}
	for _, messageCodec := range defaultMessageCodecs {
		frameCodec.messageCodecs[messageCodec.GetOpCode()] = messageCodec
	}
	for _, customizer := range customizers {
		customizer(frameCodec)
	}
	return frameCodec
}

func WithCompressor(compressor compression.MessageCompressor) CodecCustomizer {
	return func(frameCodec *Codec) {
		frameCodec.compressor = compressor
	}
}

func WithMessageCodecs(messageCodecs ...message.Codec) CodecCustomizer {
	return func(frameCodec *Codec) {
		for _, codec := range messageCodecs {
			frameCodec.messageCodecs[codec.GetOpCode()] = codec
		}
	}
}

func (c *Codec) CompressionAlgorithm() string {
	if c.compressor == nil {
		return "NONE"
	} else {
		return c.compressor.Algorithm()
	}
}
