package client_test

import (
	"flag"
	"github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/compression/lz4"
	"github.com/datastax/go-cassandra-native-protocol/compression/snappy"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
	"sync/atomic"
	"testing"
)

var remoteAvailable bool
var logLevel int

func TestMain(m *testing.M) {
	parseFlags()
	setLogLevel()
	createCodecs()
	createStreamIdGenerators()
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

var streamIdGenerators map[string]func(int) int16

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

func createStreamIdGenerators() {
	var managed = func(int) int16 {
		return client.ManagedStreamId
	}
	var fixed = func(clientId int) int16 {
		if int16(clientId) == client.ManagedStreamId {
			panic("stream id 0")
		}
		return int16(clientId)
	}
	counter := uint32(0)
	var incremental = func(clientId int) int16 {
		for {
			i := int16(atomic.AddUint32(&counter, 1))
			// can overflow during tests
			if i == client.ManagedStreamId {
				continue
			}
			return i
		}
	}
	streamIdGenerators = map[string]func(int) int16{
		"managed":     managed,
		"fixed":       fixed,
		"incremental": incremental,
	}
}
