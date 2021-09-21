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

package client_test

import (
	"flag"
	"github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"math"
	"os"
	"sync/atomic"
	"testing"
)

var remoteAvailable bool
var logLevel int

func TestMain(m *testing.M) {
	parseFlags()
	setLogLevel()
	createStreamIdGenerators()
	os.Exit(m.Run())
}

func parseFlags() {
	flag.IntVar(
		&logLevel,
		"logLevel",
		int(zerolog.ErrorLevel),
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

var compressions = []primitive.Compression{primitive.CompressionNone, primitive.CompressionLz4, primitive.CompressionSnappy}

var streamIdGenerators map[string]func(int, primitive.ProtocolVersion) int16

func createStreamIdGenerators() {
	var managed = func(clientId int, version primitive.ProtocolVersion) int16 {
		return client.ManagedStreamId
	}
	var fixed = func(clientId int, version primitive.ProtocolVersion) int16 {
		if int16(clientId) == client.ManagedStreamId {
			panic("stream id 0")
		}
		return int16(clientId)
	}
	counter := uint32(1)
	var incremental = func(clientId int, version primitive.ProtocolVersion) int16 {
		var max uint32
		if version <= primitive.ProtocolVersion2 {
			max = math.MaxInt8
		} else {
			max = math.MaxInt16
		}
		for {
			current := counter
			next := current + 1
			if next > max {
				next = 1
			}
			if atomic.CompareAndSwapUint32(&counter, current, next) {
				return int16(next)
			}
		}
	}
	streamIdGenerators = map[string]func(int, primitive.ProtocolVersion) int16{
		"managed":     managed,
		"fixed":       fixed,
		"incremental": incremental,
	}
}
