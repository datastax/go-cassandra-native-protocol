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

package primitive

type NillableInt32 struct {
	Value int32
}

type NillableInt64 struct {
	Value int64
}

type NillableConsistencyLevel struct {
	Value ConsistencyLevel
}

func CloneNillableInt32(n *NillableInt32) *NillableInt32 {
	if n == nil {
		return nil
	}

	return &NillableInt32{Value: n.Value}
}

func CloneNillableInt64(n *NillableInt64) *NillableInt64 {
	if n == nil {
		return nil
	}

	return &NillableInt64{Value: n.Value}
}

func CloneNillableConsistencyLevel(n *NillableConsistencyLevel) *NillableConsistencyLevel {
	if n == nil {
		return nil
	}

	return &NillableConsistencyLevel{Value: n.Value}
}