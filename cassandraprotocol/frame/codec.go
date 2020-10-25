package frame

import (
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/compression"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/message"
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

type CodecCustomizer func(*Codec)

func NewCodec(customizers ...CodecCustomizer) *Codec {
	codec := &Codec{codecs: makeCodecsMap(defaultCodecs)}
	for _, customizer := range customizers {
		customizer(codec)
	}
	return codec
}

func WithCompressor(compressor compression.MessageCompressor) CodecCustomizer {
	return func(codec *Codec) {
		codec.compressor = compressor
	}
}

func WithMessageCodecs(codecs ...message.Codec) CodecCustomizer {
	return func(codec *Codec) {
		codec.codecs = makeCodecsMap(codecs)
	}
}

func (c *Codec) CompressionAlgorithm() string {
	if c.compressor == nil {
		return "NONE"
	} else {
		return c.compressor.Algorithm()
	}
}

func makeCodecsMap(codecs []message.Codec) map[cassandraprotocol.OpCode]message.Codec {
	var codecsMap = make(map[cassandraprotocol.OpCode]message.Codec, len(codecs))
	for _, codec := range codecs {
		codecsMap[codec.GetOpCode()] = codec
	}
	return codecsMap
}
