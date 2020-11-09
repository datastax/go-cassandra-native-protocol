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

var remoteAvailable bool
var logLevel int

func TestMain(m *testing.M) {
	parseFlags()
	setLogLevel()
	createCodecs()
	os.Exit(m.Run())
}

func parseFlags() {
	flag.IntVar(
		&logLevel,
		"logLevel",
		int(zerolog.InfoLevel),
		"the log level to use (default: info)",
	)
	flag.BoolVar(
		&remoteAvailable,
		"remote",
		false,
		"whether a remote cluster is available on localhost:9042",
	)
	flag.Parse()
}

func setLogLevel() {
	zerolog.SetGlobalLevel(zerolog.Level(logLevel))
	log.Logger = log.Output(zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: zerolog.TimeFormatUnix,
	})
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
