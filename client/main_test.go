package client_test

import (
	"flag"
	"github.com/datastax/go-cassandra-native-protocol/compression/lz4"
	"github.com/datastax/go-cassandra-native-protocol/compression/snappy"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	setLogLevel()
	setRemoteAvailable()
	createCodecs()
	os.Exit(m.Run())
}

func setLogLevel() {
	var logLevel int
	flag.IntVar(
		&logLevel,
		"logLevel",
		int(zerolog.InfoLevel),
		"the log level to use (default: info)",
	)
	zerolog.SetGlobalLevel(zerolog.Level(logLevel))
	log.Logger = log.Output(zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: zerolog.TimeFormatUnix,
	})
}

var remoteAvailable bool

func setRemoteAvailable() {
	flag.BoolVar(
		&remoteAvailable,
		"remote",
		false,
		"whether a remote cluster is available on localhost:9042",
	)
}

var codecs map[string]frame.Codec

func createCodecs() {
	lz4Codec := frame.NewCodec()
	lz4Codec.SetBodyCompressor(&lz4.BodyCompressor{})
	snappyCodec := frame.NewCodec()
	snappyCodec.SetBodyCompressor(&snappy.BodyCompressor{})
	codecs = map[string]frame.Codec{
		"LZ4":    lz4Codec,
		"SNAPPY": snappyCodec,
		"NONE":   frame.NewCodec(),
	}
}
