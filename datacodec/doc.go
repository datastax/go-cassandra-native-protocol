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

// Package datacodec contains functionality to encode and decode CQL data.
//
// This package is not used anywhere in this library, but can be used by client code to encode and decode actual CQL
// values sent to or received from any Cassandra-compatible backend.
//
// `datacodec.Codec` is its main interface, and it has two main methods:
//
//  // Encode encodes the given source into dest. The parameter source must be a value of a supported Go type for the
//  // CQL type being encoded, or a pointer thereto; a nil value is encoded as a CQL NULL.
//  Encode(source interface{}, version primitive.ProtocolVersion) (dest []byte, err error)
//
//  // Decode decodes the given source into dest. The parameter dest must be a pointer to a supported Go type for
//  // the CQL type being decoded; it cannot be nil. If return parameter wasNull is true, then the decoded value was a
//  // NULL, in which case the actual value stored in dest will be set to its zero value.
//  Decode(source []byte, dest interface{}, version primitive.ProtocolVersion) (wasNull bool, err error)
//
// Obtaining a codec
//
// It is not required to implement the datacodec.Codec interface unless a custom encoding/decoding scheme must be used.
// The NewCodec function can be used to obtain a codec for a all valid CQL types, including complex ones (lists, sets,
// maps, tuples and user-defined types).
//
// Simple CQL types also have a global codec available; for example the datacodec.Varchar codec can be used to decode
// and encode to CQL varchar.
//
// Codecs for complex types can be obtained through constructor functions:
//
//  - NewList
//  - NewSet
//  - NewMap
//  - NewTuple
//  - NewUserDefined
//
// Using a codec
//
// Codecs accept a wide variety of inputs, but they have a "preferred" Go type that is the ideal representation of the
// CQL type in Go. In the table below, preferred types are listed first.
//
//  CQL type              | Accepted Go types (1)                           | Notes
//  bigint, counter       | int64, *int64                                   |
//                        | int[64-8], *int[64-8], uint[64-8], *uint[64-8]  |
//                        | *big.Int                                        |
//                        | string, *string                                 | formatted and parsed as base 10 number
//  blob                  | []byte, *[]byte                                 |
//                        | string, *string                                 |
//  boolean               | bool, *bool                                     |
//                        | int[64-8], *int[64-8], uint[64-8], *uint[64-8]  | zero=false, other=true
//  date                  | time.Time, *time.Time                           | time at start of day (in UTC); clock part set to zero
//                        | int[64-8], *int[64-8], uint[64-8], *uint[64-8]  | days since Unix epoch
//                        | string, *string                                 | parsed according to layout, default is "2006-01-02"
//  decimal               | CqlDecimal, *CqlDecimal                         |
//  double                | float64, *float64                               |
//                        | float32, *float32                               |
//                        | *big.Float                                      |
//  duration              | CqlDuration, *CqlDuration                       |
//  float                 | float32, *float32                               |
//                        | float64, *float64                               |
//  inet                  | net.IP, *net.IP                                 |
//                        | []byte, *[]byte                                 |
//                        | string, *string                                 | parsed with net.ParseIP
//  int                   | int32, *int32                                   |
//                        | int[64-8], *int[64-8], uint[64-8], *uint[64-8]  |
//                        | string, *string                                 | formatted and parsed as base 10 number
//  list, set             | any compatible slice                            | element types must match
//                        | any compatible array                            | element types must match
//  map                   | any compatible map                              | key and value types must match
//  smallint              | int16, *int16                                   |
//                        | int[64-8], *int[64-8], uint[64-8], *uint[64-8]  |
//                        | string, *string                                 | formatted and parsed as base 10 number
//  time                  | time.Duration, *time.Duration                   |
//                        | int[64-8], *int[64-8], uint[64-8], *uint[64-8]  | nanoseconds since start of day
//                        | time.Time, *time.Time                           | time since start of day (in UTC); date part set to zero
//                        | string, *string                                 | parsed according to layout, default is "15:04:05.999999999"
//  timestamp             | time.Time, *time.Time                           |
//                        | int[64-8], *int[64-8], uint[64-8], *uint[64-8]  | milliseconds since Unix epoch
//                        | string, *string                                 | parsed according to layout and location, defaults are "2006-01-02T15:04:05.999999999-07:00" and UTC
//  tinyint               | int8, *int8                                     |
//                        | int[64-8], *int[64-8], uint[64-8], *uint[64-8]  |
//                        | string, *string                                 | formatted and parsed as base 10 number
//  tuple                 | any compatible slice                            | slice size and elements must match
//                        | any compatible array                            | array size and elements must match
//                        | any compatible struct                           | fields must be exported and are marshaled in order of declaration
//  user-defined type     | any compatible map                              | map key must be string
//                        | any compatible struct                           | fields must be exported; field names match case-insensitively (2)
//                        | any compatible slice                            | slice size and elements must match
//                        | any compatible array                            | array size and elements must match
//  uuid, timeuuid        | primitive.UUID, *primitive.UUID                 |
//                        | [16]byte, []byte                                | raw UUID bytes, length must be 16 bytes
//                        | string, *string                                 | hex representation, parsed with primitive.ParseUuid
//  varchar, text, ascii  | string, *string                                 |
//                        | []byte, *[]byte, []rune, *[]rune                |
//  varint                | big.Int, *big.Int                               |
//                        | int[64-8], *int[64-8], uint[64-8], *uint[64-8]  |
//                        | string, *string                                 | formatted and parsed as base 10 number
//
// (1) types listed on the first line are the preferred types for each CQL type. Note that non-pointer types are only
// accepted when encoding, never when decoding.
// (2) when mapping structs to user-defined types, the "cassandra" field tag can be used to override the corresponding
// field name.
//
// In addition to the accepted types above, all codecs also accept interface{} when encoding and *interface{} when
// decoding. When encoding an interface{} value, the actual runtime value stored in the variable must be of an
// accepted type. When decoding to *interface{}, the codec will use the preferred type to decode, then store its value
// in the target variable.
//
// Encoding data
//
// Sources can be passed by value or by reference, unless specified otherwise in the table above. Nils are encoded as
// CQL NULLs; encoding such a value is generally a no-op.
//
// Decoding data
//
// Destination values must be passed by reference, see examples below. This is also valid for slices and maps, and is
// required for the value to be addressable (i.e. settable), and for the changes applied to the value to be visible once
// the method returns.
//
// When a CQL NULL is decoded, the passed `dest` variable is set to its zero value. Depending on the variable type, the
// zero value may or may not be distinguishable from the CQL zero value for that type. For example, if a CQL bigint was
// NULL and `dest` is `int64`, then `dest` will be set to `0`, effectively making this indistinguishable from a non-NULL
// bigint value of `0`. This is why the return parameter `wasNull` is used for: if `wasNull` is true then the decoded
// value was a CQL NULL.
//
// A typical invocation of `Codec.Decode` is as follows:
//
//  source := []byte{...}
//  var value int64
//  wasNull, err := datacodec.Bigint.Decode(source, &value, primitive.ProtocolVersion5)
//  if err != nil {
// 	  fmt.Println("Decoding failed: ", err)
//  } else if wasNull {
// 	  fmt.Println("CQL value was NULL")
//  } else {
// 	  fmt.Println("CQL value was:", value)
//  }
//
// Decoding a complex CQL type, such as a list, is not very different:
//
//  source := []byte{...}
//  var value []int
//  listOfInt := datatype.NewListType(datatype.Int)
//  listOfIntCodec, _ := datacodec.NewList(listOfInt)
//  wasNull, err := listOfIntCodec.Decode(source, &value, primitive.ProtocolVersion5)
//  if err != nil {
// 	  fmt.Println("Decoding failed: ", err)
//  } else if wasNull {
// 	  fmt.Println("CQL value was NULL")
//  } else {
// 	  fmt.Println("CQL value was:", value)
//  }
package datacodec
