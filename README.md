# Cassandra Native Protocol Bindings for Go

[![Go Build Status](https://github.com/datastax/go-cassandra-native-protocol/workflows/Go/badge.svg)](https://github.com/datastax/go-cassandra-native-protocol/actions)

This project contains all the logic required to encode and decode Apache Cassandra(R)'s CQL native protocol frames in
Go.

It currently supports CQL protocol versions 3 and 4. Protocol version 5 is still in beta status at the time of writing,
and support for it is provided as a preview (but it is very likely subject to future changes).

It currently does not support DataStax Enterprise's specific protocol features.

This project originated as an attempt to port the DataStax Cassandra Java driver's 
[native-protocol](https://github.com/datastax/native-protocol) project to the Go language. 