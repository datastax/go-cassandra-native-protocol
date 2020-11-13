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
