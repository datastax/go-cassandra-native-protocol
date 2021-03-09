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

package client

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAuthCredentials_Copy(t *testing.T) {

	credentials := &AuthCredentials{
		Username: "user1",
		Password: "pass1",
	}

	cpy := credentials.Copy()
	assert.Equal(t, credentials, cpy)

	cpy.Username = "user2"
	cpy.Password = "pass2"

	assert.NotEqual(t, credentials, cpy)

}

func TestAuthCredentials_Marshal(t *testing.T) {

	credentials := &AuthCredentials{
		Username: "user1",
		Password: "pass1",
	}

	bytes := credentials.Marshal()

	assert.Equal(t, []byte{
		0,
		byte('u'), byte('s'), byte('e'), byte('r'), byte('1'),
		0,
		byte('p'), byte('a'), byte('s'), byte('s'), byte('1'),
	}, bytes)
}

func TestAuthCredentials_Unmarshal(t *testing.T) {

	credentials := &AuthCredentials{}

	err := credentials.Unmarshal([]byte{
		0,
		byte('u'), byte('s'), byte('e'), byte('r'), byte('1'),
		0,
		byte('p'), byte('a'), byte('s'), byte('s'), byte('1'),
	})

	assert.Nil(t, err)
	assert.Equal(t, "user1", credentials.Username)
	assert.Equal(t, "pass1", credentials.Password)
}
