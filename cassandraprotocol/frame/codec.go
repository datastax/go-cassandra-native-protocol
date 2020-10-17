package frame

import (
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/compression"
	"go-cassandra-native-protocol/cassandraprotocol/message"
)

type Codec struct {
	compressor compression.MessageCompressor
	codecs     map[cassandraprotocol.OpCode]message.Codec
}

var defaultCodecs = []message.Codec{
	&message.StartupCodec{},
	&message.OptionsCodec{},
	&message.QueryCodec{},
	&message.PrepareCodec{},
	&message.ExecuteCodec{},
	&message.RegisterCodec{},
	// TODO Batch
	&message.AuthResponseCodec{},
	// TODO error
	&message.ReadyCodec{},
	&message.AuthenticateCodec{},
	&message.SupportedCodec{},
	// TODO result
	&message.EventCodec{},
	&message.AuthChallengeCodec{},
	&message.AuthSuccessCodec{},
}

type CodecOption func(*Codec)

func NewCodec(options ...CodecOption) *Codec {
	codec := &Codec{codecs: makeCodecsMap(defaultCodecs)}
	// Apply options if there are any, can overwrite default
	for _, option := range options {
		option(codec)
	}
	return codec
}

func WithCompressor(compressor compression.MessageCompressor) CodecOption {
	return func(codec *Codec) {
		codec.compressor = compressor
	}
}

func WithMessageCodecs(codecs ...message.Codec) CodecOption {
	return func(codec *Codec) {
		codec.codecs = makeCodecsMap(codecs)
	}
}

func makeCodecsMap(codecs []message.Codec) map[cassandraprotocol.OpCode]message.Codec {
	var codecsMap = make(map[cassandraprotocol.OpCode]message.Codec, len(codecs))
	for _, codec := range codecs {
		codecsMap[codec.GetOpCode()] = codec
	}
	return codecsMap
}
