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

package frame

import (
	"bytes"
	"fmt"
)

func (c *codec) ConvertToRawFrame(frame *Frame) (*RawFrame, error) {
	var body bytes.Buffer
	if err := c.EncodeBody(frame.Header, frame.Body, &body); err != nil {
		return nil, fmt.Errorf("cannot encode body: %w", err)
	}
	frame.Header.BodyLength = int32(body.Len())
	return &RawFrame{
		Header: frame.Header,
		Body:   body.Bytes(),
	}, nil
}

func (c *codec) ConvertFromRawFrame(frame *RawFrame) (*Frame, error) {
	if body, err := c.DecodeBody(frame.Header, bytes.NewBuffer(frame.Body)); err != nil {
		return nil, fmt.Errorf("cannot decode body: %w", err)
	} else {
		return &Frame{
			Header: frame.Header,
			Body:   body,
		}, nil
	}
}
