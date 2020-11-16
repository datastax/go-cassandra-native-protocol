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

/*

Package client contains basic utilities to exchange native protocol frames with compatible endpoints.

The main type in this package is CqlClient, a simple CQL client that can be used to test any CQL-compatible backend.

Please note that code in this package is intended mostly to help driver implementors test their libraries; it should
not be used in production.

*/
package client
