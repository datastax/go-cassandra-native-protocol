# Cassandra Native Protocol Bindings for Go

This project contains all the logic required to encode and decode Apache Cassandra(R)'s CQL native protocol frames in
Go.

It currently supports:

- Cassandra CQL protocol versions 3 and 4. Protocol version 5 is still in beta status at the time of writing,
and support for it is provided as a preview (but it is very likely subject to future changes).
- DSE (DataStax Enterprise) protocol versions 1 and 2.

This project originated as an attempt to port the DataStax Cassandra Java driver's 
[native-protocol](https://github.com/datastax/native-protocol) project to the Go language. 