package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/frame"
	"go-cassandra-native-protocol/cassandraprotocol/message"
)

func main() {

	if startupFrame, err := frame.NewRequestFrame(
		cassandraprotocol.ProtocolVersion4,
		1,
		true,
		nil,
		message.NewStartup(),
	); err != nil {
		panic(err)
	} else {
		testMessage(startupFrame, false)
	}

	if queryFrame, err := frame.NewRequestFrame(
		cassandraprotocol.ProtocolVersion4,
		1,
		true,
		nil,
		&message.Query{
			Query:   "SELECT * FROM system.local",
			Options: message.QueryOptionsDefault,
		},
	); err != nil {
		panic(err)
	} else {
		testMessage(queryFrame, false)
	}

	if rowsFrame, err := frame.NewResponseFrame(
		cassandraprotocol.ProtocolVersion4,
		1,
		nil,
		map[string][]byte{"hello": {0xca, 0xfe, 0xba, 0xbe}},
		[]string{"you naughty boy"},
		&message.Rows{
			Metadata: message.NewRowsMetadata(message.WithoutColumnSpecs(1, nil, nil, nil)),
			Data:     [][][]byte{},
		},
	); err != nil {
		panic(err)
	} else {
		testMessage(rowsFrame, true)
	}
}

func testMessage(originalFrame *frame.Frame, useJson bool) {
	println("--------------------------------")
	if useJson {
		b1, _ := json.MarshalIndent(originalFrame, "", "\t")
		fmt.Printf("original frame:\n%v\n", string(b1))
	} else {
		fmt.Printf("original frame:\n%v\n", originalFrame)
	}
	codec := frame.NewCodec()
	if encodedFrame, err := codec.Encode(originalFrame); err != nil {
		panic(err)
	} else {
		fmt.Print("encoded frame:\n", hex.Dump(encodedFrame.Bytes()))
		if decodedFrame, err := codec.Decode(encodedFrame); err != nil {
			panic(err)
		} else {
			if useJson {
				b2, _ := json.MarshalIndent(decodedFrame, "", "\t")
				fmt.Printf("decoded frame:\n%v\n", string(b2))
			} else {
				fmt.Printf("decoded frame:\n%v\n", decodedFrame)
			}
		}
	}
	println()
}
