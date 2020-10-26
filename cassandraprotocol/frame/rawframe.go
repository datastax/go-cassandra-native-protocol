package frame

import (
	"fmt"
)

type RawFrame struct {
	Header *RawHeader
	Body   []byte
}

func (f *RawFrame) String() string {
	return fmt.Sprintf("{header: %v, body: %v}", f.Header, f.Body)
}
