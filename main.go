package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

func main() {

	if startupFrame, err := frame.NewRequestFrame(
		primitive.ProtocolVersion4,
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
		primitive.ProtocolVersion4,
		1,
		true,
		nil,
		&message.Query{Query: "SELECT * FROM system.local", Options: message.NewQueryOptions()},
	); err != nil {
		panic(err)
	} else {
		testMessage(queryFrame, false)
	}

	if rowsFrame, err := frame.NewResponseFrame(
		primitive.ProtocolVersion4,
		1,
		nil,
		map[string][]byte{"hello": {0xca, 0xfe, 0xba, 0xbe}},
		[]string{"you naughty boy"},
		message.NewRowsResult(
			message.WithRowsMetadata(
				message.NewRowsMetadata(message.NoColumnMetadata(1)),
			),
			message.WithRowsData(),
		),
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
	encodedFrame := bytes.Buffer{}
	if err := codec.EncodeFrame(originalFrame, &encodedFrame); err != nil {
		panic(err)
	} else {
		fmt.Print("encoded frame:\n", hex.Dump(encodedFrame.Bytes()))
		if decodedFrame, err := codec.DecodeFrame(&encodedFrame); err != nil {
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
