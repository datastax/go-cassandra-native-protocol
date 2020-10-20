package main

import (
	"encoding/json"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/frame"
	"go-cassandra-native-protocol/cassandraprotocol/message"
)

func main() {
	testMessage(message.NewStartup())
	testMessage(&message.Query{
		Query: "SELECT * FROM system.local",
		Options: message.NewQueryOptions(
			cassandraprotocol.ConsistencyLevelOne,
			cassandraprotocol.ConsistencyLevelSerial,
			[]*cassandraprotocol.Value{},
			nil,
			false,
			5000,
			nil,
			message.DefaultTimestampNone,
			"",
			message.NowInSecondsNone,
		),
	})
}

func testMessage(msg message.Message) {
	if originalFrame, err := frame.NewRequestFrame(
		cassandraprotocol.ProtocolVersion4,
		1, true, nil, msg); err != nil {
		panic(err)
	} else {
		codec := frame.NewCodec()
		if encoded, err := codec.Encode(originalFrame); err != nil {
			panic(err)
		} else if decodedFrame, err := codec.Decode(encoded); err != nil {
			panic(err)
		} else {
			fmt.Printf("original frame: %v\n", originalFrame)
			fmt.Printf(" decoded frame: %v\n", decodedFrame)
			b1, _ := json.MarshalIndent(originalFrame, "", "\t")
			b2, _ := json.MarshalIndent(decodedFrame, "", "\t")
			fmt.Printf("original frame:\n%v\n", string(b1))
			fmt.Printf("decdoded frame:\n%v\n", string(b2))
		}
	}
}
