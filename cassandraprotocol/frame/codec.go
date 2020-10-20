package frame

import (
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/compression"
	"go-cassandra-native-protocol/cassandraprotocol/message"
)

type codec struct {
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
}

type CodecCustomizer func(*codec)

func NewCodec(customizers ...CodecCustomizer) *codec {
	codec := &codec{codecs: makeCodecsMap(defaultCodecs)}
	for _, customizer := range customizers {
		customizer(codec)
	}
	return codec
}

func WithCompressor(compressor compression.MessageCompressor) CodecCustomizer {
	return func(codec *codec) {
		codec.compressor = compressor
	}
}

func WithMessageCodecs(codecs ...message.Codec) CodecCustomizer {
	return func(codec *codec) {
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
